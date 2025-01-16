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
			done := make(chan loadgenreceiver.TelemetryStats) // loadgenreceiver will send stats on generated telemetry
			stop := make(chan bool)

			go func() {
				for {
					select {
					case <-stop:
					case stats := <-done:
						b.StopTimer()
						close(stop)
						total := stats.LogRecords + stats.MetricDataPoints + stats.Spans
						b.ReportMetric(float64(stats.LogRecords)/b.Elapsed().Seconds(), "logs/s")
						b.ReportMetric(float64(stats.MetricDataPoints)/b.Elapsed().Seconds(), "metric_points/s")
						b.ReportMetric(float64(stats.Spans)/b.Elapsed().Seconds(), "spans/s")
						b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "total/s")
						b.ReportMetric(float64(stats.Requests)/b.Elapsed().Seconds(), "requests/s")
						return
					}
				}
			}()

			var configFiles []string
			configFiles = append(configFiles, Config.CollectorConfigPath)
			configFiles = append(configFiles, CollectorConfigFilesFromConfig(Config.Exporter, signal, b.N)...)
			err := RunCollector(context.Background(), stop, configFiles, done)
			if err != nil {
				fmt.Println(err)
				b.Log(err)
			}
		})
		fmt.Print(signal)
		fmt.Println(result.String())
	}
}
