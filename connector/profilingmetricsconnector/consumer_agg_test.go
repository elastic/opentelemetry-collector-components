package profilingmetricsconnector

import (
	"context"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestConsumeProfiles_AggregatedFrameMetrics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := new(consumertest.MetricsSink)
		flushInterval := 10 * time.Minute
		ctx, cancelFn := context.WithCancel(t.Context())

		agg := newAggConsumer(m, flushInterval, componenttest.NewNopTelemetrySettings().Logger)
		assert.NoError(t, agg.Start(ctx))

		sampleMetrics := func() pmetric.Metrics {
			inputMetrics, err := golden.ReadMetrics(filepath.Join(testDataDir, "frame_metrics", "output-metrics.yaml"))
			assert.NoError(t, err)
			return inputMetrics
		}

		// aggregate metrics
		agg.ConsumeMetrics(t.Context(), sampleMetrics())
		agg.ConsumeMetrics(t.Context(), sampleMetrics())

		time.Sleep(flushInterval)
		synctest.Wait()

		actualMetrics := m.AllMetrics()
		assert.Len(t, actualMetrics, 1)
		// err := golden.WriteMetrics(t, filepath.Join(testDataDir, "frame_metrics", "output-agg-metrics.yaml"), actualMetrics[0])
		// assert.NoError(t, err)
		expectedMetrics, err := golden.ReadMetrics(filepath.Join(testDataDir, "frame_metrics", "output-agg-metrics.yaml"))
		assert.NoError(t, err)
		require.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actualMetrics[0], pmetrictest.IgnoreStartTimestamp(), pmetrictest.IgnoreDatapointAttributesOrder(), pmetrictest.IgnoreMetricDataPointsOrder()))
		cancelFn()
	})
}
