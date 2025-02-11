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
			validateErrorMessage: "exactly one of [endpoint, endpoints, cloudid] must be specified; exactly one of [endpoint, endpoints, cloudid] must be specified",
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
				assert.EqualError(t, component.ValidateConfig(cfg), tt.validateErrorMessage)
				return
			}

			assert.NoError(t, component.ValidateConfig(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
