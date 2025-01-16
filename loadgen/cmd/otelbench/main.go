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

var allSignals = []string{"logs", "metrics", "traces"}

func main() {
	Init()
	testing.Init()
	flag.Parse()

	var signals []string
	if Config.Logs {
		signals = append(signals, "logs")
	}
	if Config.Metrics {
		signals = append(signals, "metrics")
	}
	if Config.Traces {
		signals = append(signals, "traces")
	}

	for _, signal := range signals {
		result := testing.Benchmark(func(b *testing.B) {
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
				total := stats.LogRecords + stats.MetricDataPoints + stats.Spans
				b.ReportMetric(float64(stats.LogRecords)/elapsedSeconds, "logs/s")
				b.ReportMetric(float64(stats.MetricDataPoints)/elapsedSeconds, "metric_points/s")
				b.ReportMetric(float64(stats.Spans)/elapsedSeconds, "spans/s")
				b.ReportMetric(float64(total)/elapsedSeconds, "total/s")
				b.ReportMetric(float64(stats.Requests)/elapsedSeconds, "requests/s")

				close(stop)
			}()

			err := RunCollector(context.Background(), stop, configs(Config.Exporter, signal, b.N), logsDone, metricsDone, tracesDone)
			if err != nil {
				fmt.Println(err)
				b.Log(err)
			}
		})
		fmt.Print(signal)
		fmt.Println(result.String())
	}
}

func configs(exporter, signal string, iterations int) (configFiles []string) {
	configFiles = append(configFiles, Config.CollectorConfigPath)
	configFiles = append(configFiles, SetIterations(iterations)...)
	for _, s := range allSignals {
		if signal != s {
			configFiles = append(configFiles, DisableSignal(s)...)
		}
	}
	configFiles = append(configFiles, CollectorConfigFilesFromConfig(exporter)...)
	return
}
