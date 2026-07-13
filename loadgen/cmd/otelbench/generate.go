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

const defaultMetricsGenSeed = 123

var findAvailableMetricsTelemetryPort = ephemeralPort
var benchmarkMetricsGen = testing.Benchmark

type metricsGenRunStats struct {
	sent           float64
	failed         float64
	activeDuration time.Duration
}

type metricsGenRunFunc func(context.Context, []string) (metricsGenRunStats, error)

// runMetricsGenerator runs otelbench as a plain metricsgen collector benchmark.
// metricsgen receiver normally terminates the collector via exit_after_end; the
// optional -duration-metrics flag acts as a safety cap. When
// -metrics-telemetry-endpoint is set, it scrapes the collector's own telemetry
// during the run and prints go-benchmark-style throughput once it completes. It
// returns the process exit code.
func runMetricsGenerator(parent context.Context) int {
	if Config.CollectorConfigPath == "" {
		fmt.Fprintln(os.Stderr, "metricsgen mode requires -config with a collector pipeline")
		return 2
	}

	ctx, cancel := signal.NotifyContext(parent, os.Interrupt)
	defer cancel()

	telemetryEndpoint, configFiles, err := metricsGeneratorConfigFiles(
		Config.CollectorConfigPath,
		Config.MetricsTelemetryEndpoint,
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	if telemetryEndpoint != "" {
		printMetricsTelemetryEndpoint(os.Stderr, telemetryEndpoint)
	}

	run := func(ctx context.Context, configFiles []string) (metricsGenRunStats, error) {
		return runMetricsGenCollector(ctx, configFiles, telemetryEndpoint, Config.DurationMetrics)
	}

	if telemetryEndpoint == "" {
		if _, err := run(ctx, configFiles); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	}

	var result testing.BenchmarkResult
	var runErr error
	if Config.MetricsGenBenchmark {
		result, runErr = runMetricsGenBench(ctx, configFiles, defaultMetricsGenSeed, run)
	} else {
		result, runErr = runMetricsGenOnce(ctx, configFiles, run)
	}
	if runErr != nil {
		fmt.Fprintln(os.Stderr, runErr)
		return 1
	}
	reportMetricsGenBenchmark(result)
	return 0
}

func metricsGeneratorConfigFiles(configPath, telemetryEndpoint string) (string, []string, error) {
	configFiles := []string{configPath}
	if telemetryEndpoint == "" {
		return "", configFiles, nil
	}

	host, err := metricsTelemetryHost(telemetryEndpoint)
	if err != nil {
		return "", nil, err
	}
	port, err := findAvailableMetricsTelemetryPort(host)
	if err != nil {
		return "", nil, err
	}
	configFiles = append(configFiles, metricsTelemetryConfigFiles(host, port)...)
	return net.JoinHostPort(host, strconv.Itoa(port)), configFiles, nil
}

func printMetricsTelemetryEndpoint(w io.Writer, endpoint string) {
	_, _ = fmt.Fprintf(w, "metricsgen: scraping collector telemetry from %s\n", endpoint)
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

func metricsGenSeedConfigFiles(seed int) []string {
	return setsToConfigs([]string{
		fmt.Sprintf("receivers.metricsgen.seed=%d", seed),
	})
}

func metricsGenStartNowMinusConfigFiles(minutes int) []string {
	return setsToConfigs([]string{
		fmt.Sprintf("receivers.metricsgen.start_now_minus=%dm", minutes),
	})
}

func runMetricsGenBench(ctx context.Context, configFiles []string, baseSeed int, run metricsGenRunFunc) (testing.BenchmarkResult, error) {
	var finalStats metricsGenRunStats
	var benchErr error
	var runIndex int

	result := benchmarkMetricsGen(func(b *testing.B) {
		seed := baseSeed + runIndex
		runIndex++

		benchConfigFiles := append([]string{}, configFiles...)
		benchConfigFiles = append(benchConfigFiles, metricsGenSeedConfigFiles(seed)...)
		benchConfigFiles = append(benchConfigFiles, metricsGenStartNowMinusConfigFiles(b.N)...)

		runStats, err := run(ctx, benchConfigFiles)
		if err != nil {
			benchErr = err
			b.Fatal(err)
		}
		finalStats = runStats
	})

	if benchErr != nil {
		return result, benchErr
	}
	if finalStats.activeDuration <= 0 {
		return result, fmt.Errorf("metricsgen: run too short to compute throughput")
	}
	result.T = finalStats.activeDuration
	result.Extra = metricsGenBenchmarkExtras(finalStats)
	return result, nil
}

func runMetricsGenOnce(ctx context.Context, configFiles []string, run metricsGenRunFunc) (testing.BenchmarkResult, error) {
	runStats, err := run(ctx, configFiles)
	if err != nil {
		return testing.BenchmarkResult{}, err
	}
	if runStats.activeDuration <= 0 {
		return testing.BenchmarkResult{}, fmt.Errorf("metricsgen: run too short to compute throughput")
	}
	return testing.BenchmarkResult{
		N:     1,
		T:     runStats.activeDuration,
		Extra: metricsGenBenchmarkExtras(runStats),
	}, nil
}

func metricsGenBenchmarkExtras(stats metricsGenRunStats) map[string]float64 {
	elapsedSeconds := stats.activeDuration.Seconds()
	return map[string]float64{
		"duration_s":             elapsedSeconds,
		"metric_points/s":        stats.sent / elapsedSeconds,
		"failed_metric_points/s": stats.failed / elapsedSeconds,
	}
}

func runMetricsGenCollector(ctx context.Context, configFiles []string, telemetryEndpoint string, duration time.Duration) (metricsGenRunStats, error) {
	// RunCollector cancels the collector context when stop is closed. With
	// -duration-metrics unset, stop is never closed and the run continues until
	// the collector returns on its own (e.g. metricsgen exit_after_end).
	stop := make(chan struct{})
	if duration > 0 {
		timer := time.AfterFunc(duration, func() {
			close(stop)
		})
		defer timer.Stop()
	}

	var poller *telemetryPoller
	if telemetryEndpoint != "" {
		pollCtx, pollCancel := context.WithCancel(ctx)
		defer pollCancel()
		// Poll the collector's self-telemetry during the run so we still have
		// the last counter values after exit_after_end tears the endpoint down.
		poller = startTelemetryPoller(pollCtx, telemetryEndpoint, telemetryPollInterval)
	}

	// The loadgen Stats channels are unused because the metricsgen config does
	// not make use of the loadgen receiver. So we pass nil values.
	start := time.Now()
	err := RunCollector(ctx, stop, configFiles, nil, nil, nil, nil)
	elapsed := time.Since(start)
	if err != nil && !strings.Contains(err.Error(), exitAfterEndMarker) {
		return metricsGenRunStats{}, err
	}

	if poller == nil {
		return metricsGenRunStats{}, nil
	}
	snap, firstSeen := poller.snapshot()
	if !snap.valid {
		return metricsGenRunStats{}, fmt.Errorf("metricsgen: no telemetry samples scraped")
	}
	activeDuration := metricsGenActiveDuration(elapsed, snap, firstSeen)
	if activeDuration <= 0 {
		return metricsGenRunStats{}, fmt.Errorf("metricsgen: run too short to compute throughput")
	}
	return metricsGenRunStats{
		sent:           snap.sent,
		failed:         snap.failed,
		activeDuration: activeDuration,
	}, nil
}

func reportMetricsGenBenchmark(res testing.BenchmarkResult) {
	// Match the harness output format used in main.go.
	fmt.Printf("%s\t%s\n", "BenchmarkOTelbench/metricsgen", res.String())
}

func metricsGenActiveDuration(elapsed time.Duration, snap telemetrySnapshot, firstSeen time.Time) time.Duration {
	if !firstSeen.IsZero() && snap.at.After(firstSeen) {
		return snap.at.Sub(firstSeen)
	}
	return elapsed
}
