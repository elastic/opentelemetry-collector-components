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

package loadgenreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver"

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/receiver"

	"github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver/internal/metadata"
)

func NewFactory() receiver.Factory {
	return NewFactoryWithDone(nil, nil, nil)
}

func createDefaultReceiverConfig(logsDone, metricsDone, tracesDone chan Stats) component.Config {
	return &Config{
		Logs: LogsConfig{
			SignalConfig: SignalConfig{
				doneCh:  logsDone,
				MaxSize: maxScannerBufSize,
			},
		},
		Metrics: MetricsConfig{
			SignalConfig: SignalConfig{
				doneCh:  metricsDone,
				MaxSize: maxScannerBufSize,
			},
			AddCounterAttr: true,
		},
		Traces: TracesConfig{
			SignalConfig: SignalConfig{
				doneCh:  tracesDone,
				MaxSize: maxScannerBufSize,
			},
		},
		Concurrency: 1,
	}
}

func NewFactoryWithDone(logsDone, metricsDone, tracesDone chan Stats) receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		func() component.Config {
			return createDefaultReceiverConfig(logsDone, metricsDone, tracesDone)
		},
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelDevelopment),
		receiver.WithTraces(createTracesReceiver, component.StabilityLevelDevelopment),
		receiver.WithLogs(createLogsReceiver, component.StabilityLevelDevelopment),
	)
}
