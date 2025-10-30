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

package elasticpipelineextension

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf("testdata/config.yaml")
	require.NoError(t, err)

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub("elasticpipeline")
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	expected := &Config{
		Source: SourceConfig{
			Elasticsearch: &ElasticsearchConfig{
				Index: ".otel-pipeline-config-test",
			},
		},
		Watcher: WatcherConfig{
			PollInterval:  60 * time.Second,
			CacheDuration: 10 * time.Minute,
			Filters: []FilterConfig{
				{
					Field: "agent.environment",
					Value: "test",
				},
			},
		},
		PipelineManagement: PipelineManagementConfig{
			Namespace:                "test",
			EnableHealthReporting:    false,
			HealthReportInterval:     120 * time.Second,
			StartupTimeout:           30 * time.Second,
			ShutdownTimeout:          15 * time.Second,
			MaxPipelines:             10,
			MaxComponentsPerPipeline: 5,
			ValidateConfigs:          true,
			DryRunMode:               true,
		},
	}

	assert.Equal(t, expected, cfg)
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
	}{
		{
			name: "valid config",
			config: Config{
				Source: SourceConfig{
					Elasticsearch: &ElasticsearchConfig{
						Index: ".test-index",
					},
				},
				Watcher: WatcherConfig{
					PollInterval:  30 * time.Second,
					CacheDuration: 5 * time.Minute,
				},
				PipelineManagement: PipelineManagementConfig{
					MaxPipelines:             10,
					MaxComponentsPerPipeline: 5,
					StartupTimeout:           30 * time.Second,
					ShutdownTimeout:          15 * time.Second,
					HealthReportInterval:     60 * time.Second,
				},
			},
			expectError: false,
		},
		{
			name: "missing elasticsearch config",
			config: Config{
				Source: SourceConfig{},
			},
			expectError: true,
		},
		{
			name: "empty index",
			config: Config{
				Source: SourceConfig{
					Elasticsearch: &ElasticsearchConfig{
						Index: "",
					},
				},
			},
			expectError: true,
		},
		{
			name: "zero poll interval",
			config: Config{
				Source: SourceConfig{
					Elasticsearch: &ElasticsearchConfig{
						Index: ".test-index",
					},
				},
				Watcher: WatcherConfig{
					PollInterval:  0,
					CacheDuration: 5 * time.Minute,
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
