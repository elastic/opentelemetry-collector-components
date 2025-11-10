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
	"go.opentelemetry.io/collector/confmap"
)

func TestIntegrationConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      IntegrationConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid standalone mode",
			config: IntegrationConfig{
				Mode:               "standalone",
				StreamIngress:      "",
				StreamEgress:       "",
				ComponentPrefix:    "stream_",
				ValidateConnectors: true,
			},
			expectError: false,
		},
		{
			name: "valid streaming mode",
			config: IntegrationConfig{
				Mode:               "streaming",
				StreamIngress:      "routing/stream_ingress",
				StreamEgress:       "routing/stream_egress",
				ComponentPrefix:    "stream_",
				ValidateConnectors: true,
			},
			expectError: false,
		},
		{
			name: "invalid mode",
			config: IntegrationConfig{
				Mode: "invalid",
			},
			expectError: true,
			errorMsg:    "integration mode must be 'standalone' or 'streaming'",
		},
		{
			name: "streaming mode missing ingress",
			config: IntegrationConfig{
				Mode:          "streaming",
				StreamIngress: "",
				StreamEgress:  "routing/stream_egress",
			},
			expectError: true,
			errorMsg:    "stream_ingress must be specified in streaming mode",
		},
		{
			name: "streaming mode missing egress",
			config: IntegrationConfig{
				Mode:          "streaming",
				StreamIngress: "routing/stream_ingress",
				StreamEgress:  "",
			},
			expectError: true,
			errorMsg:    "stream_egress must be specified in streaming mode",
		},
		{
			name: "empty mode defaults to standalone",
			config: IntegrationConfig{
				Mode: "",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigWithIntegration(t *testing.T) {
	cfg := &Config{
		Source: SourceConfig{
			Elasticsearch: &ElasticsearchConfig{
				Index: ".otel-pipeline-config",
			},
		},
		Watcher: WatcherConfig{
			PollInterval:  30 * time.Second,
			CacheDuration: 5 * time.Minute,
		},
		PipelineManagement: PipelineManagementConfig{
			Namespace:                "elastic",
			MaxPipelines:             50,
			MaxComponentsPerPipeline: 20,
			StartupTimeout:           60 * time.Second,
			ShutdownTimeout:          30 * time.Second,
		},
		Integration: IntegrationConfig{
			Mode:               "streaming",
			StreamIngress:      "routing/stream_ingress",
			StreamEgress:       "routing/stream_egress",
			ComponentPrefix:    "stream_",
			ValidateConnectors: true,
		},
	}

	err := cfg.Validate()
	require.NoError(t, err)
}

func TestConfigUnmarshal(t *testing.T) {
	configMap := map[string]interface{}{
		"source": map[string]interface{}{
			"elasticsearch": map[string]interface{}{
				"index": ".otel-pipeline-config",
			},
		},
		"watcher": map[string]interface{}{
			"poll_interval":  "30s",
			"cache_duration": "5m",
		},
		"pipeline_management": map[string]interface{}{
			"namespace":                   "elastic",
			"max_pipelines":               50,
			"max_components_per_pipeline": 20,
			"startup_timeout":             "60s",
			"shutdown_timeout":            "30s",
		},
		"integration": map[string]interface{}{
			"mode":                "streaming",
			"stream_ingress":      "routing/stream_ingress",
			"stream_egress":       "routing/stream_egress",
			"component_prefix":    "stream_",
			"validate_connectors": true,
		},
	}

	conf := confmap.NewFromStringMap(configMap)
	cfg := &Config{}

	err := conf.Unmarshal(cfg)
	require.NoError(t, err)

	assert.Equal(t, "streaming", cfg.Integration.Mode)
	assert.Equal(t, "routing/stream_ingress", cfg.Integration.StreamIngress)
	assert.Equal(t, "routing/stream_egress", cfg.Integration.StreamEgress)
	assert.Equal(t, "stream_", cfg.Integration.ComponentPrefix)
	assert.True(t, cfg.Integration.ValidateConnectors)
}
