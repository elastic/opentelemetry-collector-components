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

func main() {
	Init()
	testing.Init()
	flag.Parse()

	// TODO(carsonip): configurable warm up

	for _, signal := range getSignals() {
		for _, exporter := range getExporters() {
			result := testing.Benchmark(func(b *testing.B) {
				// TODO(carsonip): simulate > 1 agents for higher load
				// https://github.com/elastic/opentelemetry-collector-components/issues/305

				// loadgenreceiver will send stats about generated telemetry when it finishes sending b.N iterations
				logsDone := make(chan loadgenreceiver.TelemetryStats)
				metricsDone := make(chan loadgenreceiver.TelemetryStats)
				tracesDone := make(chan loadgenreceiver.TelemetryStats)
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

					close(stop)
				}()

				err := RunCollector(context.Background(), stop, configs(exporter, signal, b.N), logsDone, metricsDone, tracesDone)
				if err != nil {
					b.Fatal(err)
				}
			})
			fmt.Printf("%s-%s", signal, exporter)
			fmt.Print("\t")
			fmt.Println(result.String())
		}
	}
}

func configs(exporter, signal string, iterations int) (configFiles []string) {
	configFiles = append(configFiles, Config.CollectorConfigPath)
	configFiles = append(configFiles, ExporterConfigs(exporter)...)
	configFiles = append(configFiles, SetIterations(iterations)...)
	for _, s := range []string{"logs", "metrics", "traces"} {
		if signal != s {
			configFiles = append(configFiles, DisableSignal(s)...)
		}
	}
	return
}
