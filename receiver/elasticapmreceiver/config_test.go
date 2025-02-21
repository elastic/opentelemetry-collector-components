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

package elasticapmreceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver/internal/metadata"
	"github.com/elastic/opentelemetry-lib/config/configelasticsearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	expectedDefaultConfig := func() *Config {
		return &Config{
			ServerConfig: confighttp.ServerConfig{
				Endpoint: defaultEndpoint,
			},
			AgentConfig: AgentConfig{
				Enabled:       false,
				CacheDuration: 30 * time.Second,
				Elasticsearch: func() configelasticsearch.ClientConfig {
					esCfg := configelasticsearch.NewDefaultClientConfig()
					esCfg.Endpoint = defaultESEndpoint
					return esCfg
				}(),
			},
		}
	}

	tests := []struct {
		id                    component.ID
		expected              component.Config
		validateErrorMessage  string
		unmarshalErrorMessage string
	}{
		{
			id:       component.NewID(metadata.Type),
			expected: expectedDefaultConfig(),
		},
		{
			id: component.NewIDWithName(metadata.Type, "elasticsearch_agentcfg"),
			expected: func() *Config {
				cfg := expectedDefaultConfig()
				cfg.AgentConfig.Enabled = true
				cfg.AgentConfig.CacheDuration = 10 * time.Second
				cfg.AgentConfig.Elasticsearch.ClientConfig.Endpoint = "http://localhost:8200"
				return cfg
			}(),
		},
		{
			id:                   component.NewIDWithName(metadata.Type, "invalid_elasticsearch_agentcfg"),
			validateErrorMessage: "exactly one of [endpoint, endpoints, cloudid] must be specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
			require.NoError(t, err)

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)

			if tt.unmarshalErrorMessage != "" {
				assert.ErrorContains(t, sub.Unmarshal(cfg), tt.unmarshalErrorMessage)
				return
			}
			require.NoError(t, sub.Unmarshal(cfg))

			if tt.validateErrorMessage != "" {
				assert.ErrorContains(t, xconfmap.Validate(cfg), tt.validateErrorMessage)
				return
			}

			assert.NoError(t, xconfmap.Validate(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
