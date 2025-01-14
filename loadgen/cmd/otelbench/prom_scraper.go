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

package main // import "github.com/elastic/opentelemetry-collector-components/loadgen"

import (
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prom2json"
)

func GetTelemetrySent() (logs, metricPoints, spans int64, err error) {
	done := make(chan struct{})
	mfChan := make(chan *dto.MetricFamily, 1024)
	go func() {
		for {
			select {
			case mf := <-mfChan:
				if mf == nil {
					close(done)
					return
				}
				for _, m := range mf.GetMetric() {
					var isOtlp bool
					for _, l := range m.GetLabel() {
						if l.GetName() == "exporter" && l.GetValue() == "otlp" {
							isOtlp = true
						}
					}
					if isOtlp {
						v := int64(m.GetCounter().GetValue())
						switch mf.GetName() {
						case "otelcol_exporter_sent_log_records":
							logs = v
						case "otelcol_exporter_sent_metric_points":
							metricPoints = v
						case "otelcol_exporter_sent_spans":
							spans = v
						}
					}
				}

			}
		}

	}()
	err = prom2json.FetchMetricFamilies("http://127.0.0.1:8888/metrics", mfChan, nil)
	if err != nil {
		return
	}
	<-done
	return
}
