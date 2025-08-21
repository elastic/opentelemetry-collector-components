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
	_ "embed"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"testing"
	"time"

	"github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver"
)

// getSignal returns a slice of signal names to be benchmarked according to Config
func getSignals() (signals []string) {
	if Config.Logs {
		signals = append(signals, "logs")
	}
	if Config.Metrics {
		signals = append(signals, "metrics")
	}
	if Config.Traces {
		signals = append(signals, "traces")
	}
	if Config.Mixed {
		signals = append(signals, "mixed")
	}
	return
}

// getExporters returns a slice of exporter names to be benchmarked according to Config
func getExporters() (exporters []string) {
	if Config.ExporterOTLP {
		exporters = append(exporters, "otlp")
	}
	if Config.ExporterOTLPHTTP {
		exporters = append(exporters, "otlphttp")
	}
	return
}

func fullBenchmarkName(signal, exporter string, concurrency int) string {
	return fmt.Sprintf("BenchmarkOTelbench/%s-%s-%d", signal, exporter, concurrency)
}

func runBench(ctx context.Context, signal, exporter string, concurrency int, reporter func(b *testing.B)) testing.BenchmarkResult {
	return testing.Benchmark(func(b *testing.B) {
		// loadgenreceiver will send stats about generated telemetry when it finishes sending b.N iterations
		logsDone := make(chan loadgenreceiver.Stats)
		metricsDone := make(chan loadgenreceiver.Stats)
		tracesDone := make(chan loadgenreceiver.Stats)
		// if we do not expect that signal, don't wait for it
		if signal != "mixed" {
			if signal != "logs" {
				close(logsDone)
			}
			if signal != "metrics" {
				close(metricsDone)
			}
			if signal != "traces" {
				close(tracesDone)
			}
		}
		stop := make(chan struct{}) // close channel to stop the loadgen collector
		done := make(chan struct{}) // close channel to exit benchmark after stats were reported

		go func() {
			logsStats := <-logsDone
			metricsStats := <-metricsDone
			tracesStats := <-tracesDone
			b.StopTimer()

			stats := logsStats.Add(metricsStats).Add(tracesStats)
			elapsedSeconds := b.Elapsed().Seconds()
			close(stop)

			b.ReportMetric(float64(stats.LogRecords)/elapsedSeconds, "logs/s")
			b.ReportMetric(float64(stats.MetricDataPoints)/elapsedSeconds, "metric_points/s")
			b.ReportMetric(float64(stats.Spans)/elapsedSeconds, "spans/s")
			b.ReportMetric(float64(stats.Requests)/elapsedSeconds, "requests/s")
			b.ReportMetric(float64(stats.FailedLogRecords)/elapsedSeconds, "failed_logs/s")
			b.ReportMetric(float64(stats.FailedMetricDataPoints)/elapsedSeconds, "failed_metric_points/s")
			b.ReportMetric(float64(stats.FailedSpans)/elapsedSeconds, "failed_spans/s")
			b.ReportMetric(float64(stats.FailedRequests)/elapsedSeconds, "failed_requests/s")
			reporter(b)
			close(done)
		}()

		err := RunCollector(ctx, stop, configs(exporter, signal, b.N, concurrency), logsDone, metricsDone, tracesDone)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			b.Fatal(err)
		}
		<-done
	})
}

//go:embed config.yaml
var collectorConfig []byte

// serveEmbeddedConf serves an embedded collector config in an http server.
// The url will be resolved by collector's confmap http provider.
// The benefit is that the otelbench binary can function without dependency
// on a collector config in the file system.
func serveEmbeddedConf() (string, *http.Server, error) {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", nil, err
	}

	s := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, err := w.Write(collectorConfig); err != nil {
				fmt.Fprintln(os.Stderr, fmt.Errorf("embedded config: error writing http response: %w", err))
			}
		}),
	}
	go func() {
		if err := s.Serve(listener); err != http.ErrServerClosed {
			fmt.Fprintln(os.Stderr, fmt.Errorf("embedded config: http server error: %w", err))
		}
	}()
	return fmt.Sprintf("http://%s/", listener.Addr().String()), s, nil
}

// getBenchCount returns the value of -test.count flag or 1 if not set
func getBenchCount() int {
	if countFlag := flag.Lookup("test.count"); countFlag != nil {
		if n, err := strconv.Atoi(countFlag.Value.String()); err == nil && n > 0 {
			return n
		}
	}
	return 1
}

func main() {
	testing.Init()
	if err := Init(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		flag.Usage()
		os.Exit(2)
	}
	flag.Parse()

	// default to embedded collector config
	if Config.CollectorConfigPath == "" {
		url, srv, err := serveEmbeddedConf()
		if err != nil {
			fmt.Fprintln(os.Stderr, fmt.Errorf("error serving embedded collector config: %w", err))
			flag.Usage()
			os.Exit(3)
		}
		defer srv.Close()
		Config.CollectorConfigPath = url
	}

	// default to running with concurrency=1
	if Config.ConcurrencyList == nil {
		fmt.Fprintln(os.Stderr, "concurrency is unset, defaulting to 1. This may result in lower throughput")
		Config.ConcurrencyList = []int{1}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// TODO(carsonip): configurable warm up

	var maxLen int
	for _, concurrency := range Config.ConcurrencyList {
		for _, signal := range getSignals() {
			for _, exporter := range getExporters() {
				maxLen = max(maxLen, len(fullBenchmarkName(signal, exporter, concurrency)))
			}
		}
	}

	fetcher, ignore, err := newElasticsearchStatsFetcher(elasticsearchTelemetryConfig(Config.Telemetry))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if !ignore {
			os.Exit(2)
		}
	}

	// Get the benchmark count from -test.count flag
	count := getBenchCount()

	signals := getSignals()
	exporters := getExporters()
	if Config.Shuffle {
		rand.Shuffle(len(signals), func(i, j int) {
			signals[i], signals[j] = signals[j], signals[i]
		})
	}
	for _, concurrency := range Config.ConcurrencyList {
		for _, signal := range signals {
			for _, exporter := range exporters {
				benchName := fullBenchmarkName(signal, exporter, concurrency)
				for i := 0; i < count; i++ {
					t := time.Now().UTC()
					result := runBench(ctx, signal, exporter, concurrency, func(b *testing.B) {
						if fetcher == nil {
							return
						}
						// after each run wait a bit to capture late metric arrivals
						time.Sleep(10 * time.Second)
						stats, err := fetcher.FetchStats(ctx, t, time.Now().UTC())
						if err != nil {
							fmt.Fprintf(os.Stderr, "error while fetching remote stats %s", err)
							return
						}
						for unit, n := range stats {
							b.ReportMetric(n, unit)
						}
					})
					// write benchmark result to stdout, as stderr may be cluttered with collector logs
					fmt.Printf("%-*s\t%s\n", maxLen, benchName, result.String())
					// break early if context was canceled
					select {
					case <-ctx.Done():
						return
					default:
					}
				}
			}
		}
	}
}

func configs(exporter, signal string, iterations, concurrency int) (configFiles []string) {
	configFiles = append(configFiles, Config.CollectorConfigPath)
	configFiles = append(configFiles, ExporterConfigs(exporter)...)
	configFiles = append(configFiles, SetIterations(iterations)...)
	configFiles = append(configFiles, SetConcurrency(concurrency)...)
	if signal != "mixed" {
		for _, s := range []string{"logs", "metrics", "traces"} {
			// Disable pipelines not relevant to the benchmark by overriding receiver and exporter to nop
			if signal != s {
				configFiles = append(configFiles, DisableSignal(s)...)
			}
		}
	}
	return
}
