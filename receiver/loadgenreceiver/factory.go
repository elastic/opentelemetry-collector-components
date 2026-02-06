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
	"go.opentelemetry.io/collector/receiver/xreceiver"

	"github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver/internal/metadata"
)

func NewFactory() xreceiver.Factory {
	return NewFactoryWithDone(nil, nil, nil, nil)
}

func createDefaultReceiverConfig(logsDone, metricsDone, tracesDone, profilesDone chan Stats) component.Config {
	return &Config{
		Logs: LogsConfig{
			SignalConfig: SignalConfig{
				doneCh:        logsDone,
				MaxBufferSize: maxScannerBufSize,
			},
		},
		Metrics: MetricsConfig{
			SignalConfig: SignalConfig{
				doneCh:        metricsDone,
				MaxBufferSize: maxScannerBufSize,
			},
			AddCounterAttr: true,
		},
		Traces: TracesConfig{
			SignalConfig: SignalConfig{
				doneCh:        tracesDone,
				MaxBufferSize: maxScannerBufSize,
			},
		},
		Profiles: ProfilesConfig{
			SignalConfig: SignalConfig{
				doneCh:        profilesDone,
				MaxBufferSize: maxScannerBufSize,
			},
		},
		Concurrency: 1,
	}
}

func NewFactoryWithDone(logsDone, metricsDone, tracesDone, profilesDone chan Stats) xreceiver.Factory {
	return xreceiver.NewFactory(
		metadata.Type,
		func() component.Config {
			return createDefaultReceiverConfig(logsDone, metricsDone, tracesDone, profilesDone)
		},
		xreceiver.WithMetrics(createMetricsReceiver, component.StabilityLevelDevelopment),
		xreceiver.WithTraces(createTracesReceiver, component.StabilityLevelDevelopment),
		xreceiver.WithLogs(createLogsReceiver, component.StabilityLevelDevelopment),
		xreceiver.WithProfiles(createProfilesReceiver, component.StabilityLevelDevelopment),
	)
}
