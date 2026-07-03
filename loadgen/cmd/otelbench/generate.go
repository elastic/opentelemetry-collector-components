// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"testing"
	"time"
)

// exitAfterEndMarker matches the fatal-error event the metricsgen receiver emits
// once a backfill completes with exit_after_end. It must not be reported as a failure, so we ignore it.
const exitAfterEndMarker = "exit_after_end"

// telemetryPollInterval is how often otelbench scrapes the collector's own
// Prometheus telemetry endpoint while a metricsgen runs.
const telemetryPollInterval = 250 * time.Millisecond

var findAvailableMetricsTelemetryPort = randomAvailablePort

// runMetricsGenerator runs otelbench as a plain collector soak test.
// metricsgen receiver normally terminates the collector via exit_after_end; the
// optional -duration-metrics flag acts as a safety cap. When
// -metrics-telemetry-endpoint is set, it scrapes the collector's own telemetry
// during the run and prints go-benchmark-style throughput once it completes. It
// returns the process exit code.
func runMetricsGenerator(parent context.Context) int {
	if Config.CollectorConfigPath == "" {
		fmt.Fprintln(os.Stderr, "soak mode requires -config with a collector pipeline")
		return 2
	}

	ctx, cancel := signal.NotifyContext(parent, os.Interrupt)
	defer cancel()

	// RunCollector cancels the collector context when stop is closed. With
	// -duration-metrics unset, stop is never closed and the run continues until
	// the collector returns on its own (e.g. metricsgen exit_after_end).
	stop := make(chan struct{})
	if Config.DurationMetrics > 0 {
		timer := time.AfterFunc(Config.DurationMetrics, func() {
			close(stop)
		})
		defer timer.Stop()
	}

	telemetryEndpoint, configFiles, err := metricsGeneratorConfigFiles(
		Config.CollectorConfigPath,
		Config.MetricsTelemetryEndpoint,
		Config.MetricsTelemetryPortRange,
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	// Poll the collector's self-telemetry during the run so we still have the
	// last counter values after exit_after_end tears the endpoint down.
	var poller *telemetryPoller
	if telemetryEndpoint != "" {
		printMetricsTelemetryEndpoint(os.Stderr, telemetryEndpoint)
		pollCtx, pollCancel := context.WithCancel(ctx)
		defer pollCancel()
		poller = startTelemetryPoller(pollCtx, telemetryEndpoint, telemetryPollInterval)
	}

	// The loadgen Stats channels are unused because the metricsgen config does
	// not make use of the loadgen receiver. So we pass nil values.
	start := time.Now()
	err = RunCollector(ctx, stop, configFiles, nil, nil, nil, nil)
	elapsed := time.Since(start)
	if err != nil && !strings.Contains(err.Error(), exitAfterEndMarker) {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	if poller != nil {
		reportMetricsGenBenchmark(poller, elapsed)
	}
	return 0
}

func metricsGeneratorConfigFiles(configPath, telemetryEndpoint string, ports portRange) (string, []string, error) {
	configFiles := []string{configPath}
	if telemetryEndpoint == "" {
		return "", configFiles, nil
	}

	host, err := metricsTelemetryHost(telemetryEndpoint)
	if err != nil {
		return "", nil, err
	}
	port, err := findAvailableMetricsTelemetryPort(host, ports)
	if err != nil {
		return "", nil, err
	}
	configFiles = append(configFiles, metricsTelemetryConfigFiles(host, port)...)
	return net.JoinHostPort(host, strconv.Itoa(port)), configFiles, nil
}

func printMetricsTelemetryEndpoint(w io.Writer, endpoint string) {
	fmt.Fprintf(w, "soak: scraping collector telemetry from %s\n", endpoint)
}

func metricsTelemetryHost(endpoint string) (string, error) {
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		u, err := url.Parse(endpoint)
		if err != nil {
			return "", err
		}
		if host := u.Hostname(); host != "" {
			return host, nil
		}
		return "", fmt.Errorf("metrics telemetry endpoint %q is missing a host", endpoint)
	}
	if host, _, err := net.SplitHostPort(endpoint); err == nil {
		return host, nil
	}
	return endpoint, nil
}

func metricsTelemetryConfigFiles(host string, port int) []string {
	return setsToConfigs([]string{
		fmt.Sprintf(`service.telemetry.metrics.readers=[{pull: {exporter: {prometheus: {host: %q, port: %d}}}}]`, host, port),
	})
}

// reportMetricsGenBenchmark prints a go-benchmark-style line derived from the
// collector's scraped self-telemetry. Throughput is computed over the active
// send window (first observed sample to last scrape) when available, otherwise
// over the full run duration.
func reportMetricsGenBenchmark(poller *telemetryPoller, elapsed time.Duration) {
	snap, firstSeen := poller.snapshot()
	if !snap.valid {
		fmt.Fprintln(os.Stderr, "soak: no telemetry samples scraped; skipping benchmark output")
		return
	}

	activeDuration := metricsGenActiveDuration(elapsed, snap, firstSeen)
	if activeDuration <= 0 {
		fmt.Fprintln(os.Stderr, "soak: run too short to compute throughput; skipping benchmark output")
		return
	}
	elapsedSeconds := activeDuration.Seconds()
	attempted := snap.sent + snap.failed

	res := testing.BenchmarkResult{
		N: int(attempted),
		T: activeDuration,
		Extra: map[string]float64{
			"duration_s":             elapsedSeconds,
			"metric_points/s":        snap.sent / elapsedSeconds,
			"failed_metric_points/s": snap.failed / elapsedSeconds,
		},
	}
	// Match the harness output format used in main.go.
	fmt.Printf("%s\t%s\n", "BenchmarkOTelbench/metricsgen", res.String())
}

func metricsGenActiveDuration(elapsed time.Duration, snap telemetrySnapshot, firstSeen time.Time) time.Duration {
	if !firstSeen.IsZero() && snap.at.After(firstSeen) {
		return snap.at.Sub(firstSeen)
	}
	return elapsed
}
