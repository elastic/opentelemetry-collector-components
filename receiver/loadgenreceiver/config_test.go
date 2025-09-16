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
						MaxSize: maxScannerBufSize,
					},
					AddCounterAttr: true,
				},
				Logs: LogsConfig{
					SignalConfig: SignalConfig{
						MaxSize: maxScannerBufSize,
					},
				},
				Traces: TracesConfig{
					SignalConfig: SignalConfig{
						MaxSize: maxScannerBufSize,
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
			id:                 component.NewIDWithName(metadata.Type, "logs_invalid_max_size"),
			expectedErrMessage: "logs::max_size must be >= 0",
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "metrics_invalid_max_size"),
			expectedErrMessage: "metrics::max_size must be >= 0",
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "traces_invalid_max_size"),
			expectedErrMessage: "traces::max_size must be >= 0",
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
