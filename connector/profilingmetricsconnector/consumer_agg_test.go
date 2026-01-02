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
		assert.NoError(t, agg.ConsumeMetrics(t.Context(), sampleMetrics()))
		assert.NoError(t, agg.ConsumeMetrics(t.Context(), sampleMetrics()))

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
