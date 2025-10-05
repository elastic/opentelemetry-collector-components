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

package loadgenreceiver

import (
	"path/filepath"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver/internal/metadata"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		id                 component.ID
		expected           component.Config
		expectedErrMessage string
	}{
		{
			id: component.NewID(metadata.Type),
			expected: &Config{
				Metrics: MetricsConfig{
					SignalConfig: SignalConfig{
						MaxBufferSize: maxScannerBufSize,
					},
					AddCounterAttr: true,
				},
				Logs: LogsConfig{
					SignalConfig: SignalConfig{
						MaxBufferSize: maxScannerBufSize,
					},
				},
				Traces: TracesConfig{
					SignalConfig: SignalConfig{
						MaxBufferSize: maxScannerBufSize,
					},
				},
				Concurrency: 1,
			},
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "logs_invalid_max_replay"),
			expectedErrMessage: "logs::max_replay must be >= 0",
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "metrics_invalid_max_replay"),
			expectedErrMessage: "metrics::max_replay must be >= 0",
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "traces_invalid_max_replay"),
			expectedErrMessage: "traces::max_replay must be >= 0",
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "logs_invalid_max_buffer_size"),
			expectedErrMessage: "logs::max_buffer_size must be >= 0",
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "metrics_invalid_max_buffer_size"),
			expectedErrMessage: "metrics::max_buffer_size must be >= 0",
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "traces_invalid_max_buffer_size"),
			expectedErrMessage: "traces::max_buffer_size must be >= 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			t.Parallel()

			cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
			assert.NoError(t, err)

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()
			sub, err := cm.Sub(tt.id.String())
			assert.NoError(t, err)
			assert.NoError(t, sub.Unmarshal(cfg))

			err = xconfmap.Validate(cfg)
			if tt.expectedErrMessage != "" {
				assert.EqualError(t, err, tt.expectedErrMessage)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, cfg)
		})
	}

}
