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
	"flag"
	"fmt"
	"os"
	"testing"

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
	return fmt.Sprintf("%s-%s-%d", signal, exporter, concurrency)
}

func main() {
	testing.Init()
	if err := Init(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		flag.Usage()
		os.Exit(2)
	}
	flag.Parse()

	// TODO(carsonip): configurable warm up

	var maxLen int
	for _, concurrency := range Config.ConcurrencyList {
		for _, signal := range getSignals() {
			for _, exporter := range getExporters() {
				maxLen = max(maxLen, len(fullBenchmarkName(signal, exporter, concurrency)))
			}
		}
	}

	for _, concurrency := range Config.ConcurrencyList {
		for _, signal := range getSignals() {
			for _, exporter := range getExporters() {
				benchName := fullBenchmarkName(signal, exporter, concurrency)
				result := testing.Benchmark(func(b *testing.B) {
					// loadgenreceiver will send stats about generated telemetry when it finishes sending b.N iterations
					logsDone := make(chan loadgenreceiver.Stats)
					metricsDone := make(chan loadgenreceiver.Stats)
					tracesDone := make(chan loadgenreceiver.Stats)
					if signal != "logs" {
						close(logsDone)
					}
					if signal != "metrics" {
						close(metricsDone)
					}
					if signal != "traces" {
						close(tracesDone)
					}

					stop := make(chan struct{}) // close channel to stop the loadgen collector

					go func() {
						logsStats := <-logsDone
						metricsStats := <-metricsDone
						tracesStats := <-tracesDone
						b.StopTimer()

						stats := logsStats.Add(metricsStats).Add(tracesStats)

						elapsedSeconds := b.Elapsed().Seconds()
						b.ReportMetric(float64(stats.LogRecords)/elapsedSeconds, "logs/s")
						b.ReportMetric(float64(stats.MetricDataPoints)/elapsedSeconds, "metric_points/s")
						b.ReportMetric(float64(stats.Spans)/elapsedSeconds, "spans/s")
						b.ReportMetric(float64(stats.Requests)/elapsedSeconds, "requests/s")
						b.ReportMetric(float64(stats.FailedLogRecords)/elapsedSeconds, "failed_logs/s")
						b.ReportMetric(float64(stats.FailedMetricDataPoints)/elapsedSeconds, "failed_metric_points/s")
						b.ReportMetric(float64(stats.FailedSpans)/elapsedSeconds, "failed_spans/s")
						b.ReportMetric(float64(stats.FailedRequests)/elapsedSeconds, "failed_requests/s")
						// TODO(carsonip): optionally retrieve metrics (e.g. memory, cpu) of target server from Elasticsearch

						close(stop)
					}()

					err := RunCollector(context.Background(), stop, configs(exporter, signal, b.N, concurrency), logsDone, metricsDone, tracesDone)
					if err != nil {
						b.Fatal(err)
					}
				})
				// write benchmark result to stdout, as stderr may be cluttered with collector logs
				fmt.Printf("%-*s\t%s\n", maxLen, benchName, result.String())
			}
		}
	}
}

func configs(exporter, signal string, iterations, concurrency int) (configFiles []string) {
	configFiles = append(configFiles, Config.CollectorConfigPath)
	configFiles = append(configFiles, ExporterConfigs(exporter)...)
	configFiles = append(configFiles, SetIterations(iterations)...)
	configFiles = append(configFiles, SetConcurrency(concurrency)...)
	for _, s := range []string{"logs", "metrics", "traces"} {
		if signal != s {
			configFiles = append(configFiles, DisableSignal(s)...)
		}
	}
	return
}
