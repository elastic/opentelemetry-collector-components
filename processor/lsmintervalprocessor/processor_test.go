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

package lsmintervalprocessor

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
)

func TestAggregation(t *testing.T) {
	t.Parallel()

	testCases := []string{
		"sum_cumulative",
		"sum_delta",
		"histogram_cumulative",
		"histogram_delta",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &Config{Intervals: []IntervalConfig{
		{
			Duration: time.Second,
			Statements: []string{
				`set(resource.attributes["custom_res_attr"], "res")`,
				`set(instrumentation_scope.attributes["custom_scope_attr"], "scope")`,
				`set(attributes["custom_dp_attr"], "dp")`,
			},
		},
	}}

	for _, tc := range testCases {
		testName := tc

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			next := &consumertest.MetricsSink{}

			factory := NewFactory()
			settings := processortest.NewNopSettings()
			settings.TelemetrySettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
			mgp, err := factory.CreateMetricsProcessor(
				context.Background(),
				settings,
				config,
				next,
			)
			require.NoError(t, err)
			require.IsType(t, &Processor{}, mgp)
			t.Cleanup(func() { require.NoError(t, mgp.Shutdown(context.Background())) })

			dir := filepath.Join("testdata", testName)
			md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)

			// Start the processor and feed the metrics
			err = mgp.Start(context.Background(), componenttest.NewNopHost())
			require.NoError(t, err)
			err = mgp.ConsumeMetrics(ctx, md)
			require.NoError(t, err)

			var allMetrics []pmetric.Metrics
			require.Eventually(t, func() bool {
				// 1 from calling next on the input and 1 from the export
				allMetrics = next.AllMetrics()
				return len(allMetrics) == 2
			}, 5*time.Second, 100*time.Millisecond)

			expectedNextData, err := golden.ReadMetrics(filepath.Join(dir, "next.yaml"))
			require.NoError(t, err)
			require.NoError(t, pmetrictest.CompareMetrics(expectedNextData, allMetrics[0]))

			expectedExportData, err := golden.ReadMetrics(filepath.Join(dir, "output.yaml"))
			require.NoError(t, err)
			assert.NoError(t, pmetrictest.CompareMetrics(
				expectedExportData,
				allMetrics[1],
			))
		})
	}
}
