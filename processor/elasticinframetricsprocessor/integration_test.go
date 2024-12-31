// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticinframetricsprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestProcessorHostMetrics(t *testing.T) {
	tests := []struct {
		name                    string
		hostmetricsRoopath      string
		scrapersConfig          string
		expectedHostmetricsPath string
		processorConfig         *Config
		expectedMetricsPath     string
	}{
		{
			name:               "process scrapers",
			hostmetricsRoopath: "./testdata/hostmetrics_integration/e2e/",
			scrapersConfig: `
scrapers:
  process:
    mute_process_exe_error: true
    mute_process_io_error: true
    mute_process_user_error: true
    metrics:
      process.threads:
        enabled: true
      process.open_file_descriptors:
        enabled: true
      process.memory.utilization:
        enabled: true
      process.disk.operations:
        enabled: true
`,
			expectedHostmetricsPath: "./testdata/hostmetrics_integration/input-process-metrics.yaml",
			processorConfig: &Config{
				DropOriginal:     true,
				AddSystemMetrics: true,
			},
			expectedMetricsPath: "./testdata/hostmetrics_integration/output-process-metrics.yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			metricsProvider := hostmetricsreceiver.NewFactory()
			cfg := metricsProvider.CreateDefaultConfig().(*hostmetricsreceiver.Config)
			cfg.RootPath = tt.hostmetricsRoopath
			cfg.CollectionInterval = 100 * time.Millisecond

			hostCfg2, err := confmap.NewRetrievedFromYAML([]byte(tt.scrapersConfig))
			require.NoError(t, err)
			hostCfg, err := hostCfg2.AsConf()
			require.NoError(t, err)

			err = cfg.Unmarshal(hostCfg)
			require.NoError(t, err)

			nextMetrics := new(consumertest.MetricsSink)

			hostComponent, err := metricsProvider.CreateMetrics(ctx, receivertest.NewNopSettings(), cfg, nextMetrics)
			require.NoError(t, err)
			err = hostComponent.Start(ctx, componenttest.NewNopHost())
			require.NoError(t, err)

			var allMetrics []pmetric.Metrics
			expectedHostmetrics, err := golden.ReadMetrics(tt.expectedHostmetricsPath)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				allMetrics = nextMetrics.AllMetrics()
				if len(allMetrics) == 0 {
					return false
				}
				validateErr := pmetrictest.CompareMetrics(expectedHostmetrics, allMetrics[len(allMetrics)-1], pmetrictest.IgnoreResourceMetricsOrder(),
					pmetrictest.IgnoreMetricValues(),
					pmetrictest.IgnoreMetricDataPointsOrder(),
					pmetrictest.IgnoreStartTimestamp(),
					pmetrictest.IgnoreTimestamp())
				return validateErr == nil
			}, 10*time.Second, 100*time.Millisecond)

			err = hostComponent.Shutdown(ctx)
			require.NoError(t, err)

			p := newProcessor(processortest.NewNopSettings(), tt.processorConfig)

			actualMetrics, err := p.processMetrics(context.Background(), allMetrics[0])
			assert.NoError(t, err)

			// golden.WriteMetrics(t, tt.expectedMetricsPath, actualMetrics)

			expectedMetrics, err := golden.ReadMetrics(tt.expectedMetricsPath)
			require.NoError(t, err)
			require.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actualMetrics, pmetrictest.IgnoreResourceMetricsOrder(), pmetrictest.IgnoreMetricDataPointsOrder(), pmetrictest.IgnoreStartTimestamp(), pmetrictest.IgnoreTimestamp()))
		})
	}
}
