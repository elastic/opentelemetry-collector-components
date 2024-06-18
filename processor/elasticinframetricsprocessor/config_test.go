package elasticinframetricsprocessor

import (
	"path/filepath"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticinframetricsprocessor/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		configPath string
		id         component.ID
		expected   component.Config
	}{
		{
			configPath: "config.yaml",
			id:         component.NewID(metadata.Type),
			expected:   &Config{AddSystemMetrics: true, AddK8sMetrics: true},
		},
		{
			configPath: "config.yaml",
			id:         component.NewIDWithName(metadata.Type, "all"),
			expected:   &Config{AddSystemMetrics: true, AddK8sMetrics: true},
		},
		{
			configPath: "config.yaml",
			id:         component.NewIDWithName(metadata.Type, "with_system_metrics"),
			expected:   &Config{AddSystemMetrics: true, AddK8sMetrics: false},
		},
		{
			configPath: "config.yaml",
			id:         component.NewIDWithName(metadata.Type, "with_kubernetes_metrics"),
			expected:   &Config{AddSystemMetrics: false, AddK8sMetrics: true},
		},
		{
			configPath: "config.yaml",
			id:         component.NewIDWithName(metadata.Type, "without_system_and_k8s_metrics"),
			expected:   &Config{AddSystemMetrics: false, AddK8sMetrics: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			cm, err := confmaptest.LoadConf(filepath.Join("testdata", tt.configPath))
			require.NoError(t, err)

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			if tt.expected == nil {
				err = component.ValidateConfig(cfg)
				assert.Error(t, err)
				return
			}
			assert.NoError(t, component.ValidateConfig(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
