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
			done := make(chan struct{}) // loadgenreceiver will close the channel after generating b.N
			stop := make(chan bool)

			go func() {
				for {
					select {
					case <-stop:
					case <-done:
						// TODO: calculate rate without using internal telemetry
						// as it is broken on otlphttp
						logs, metricPoints, spans, err := GetTelemetrySent()
						if err != nil {
							b.Logf("error getting internal telemetry: %s", err)
							continue
						}
						total := logs + metricPoints + spans
						b.StopTimer()
						close(stop)
						b.ReportMetric(float64(logs)/b.Elapsed().Seconds(), "logs/s")
						b.ReportMetric(float64(metricPoints)/b.Elapsed().Seconds(), "metric_points/s")
						b.ReportMetric(float64(spans)/b.Elapsed().Seconds(), "spans/s")
						b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "total/s")
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
