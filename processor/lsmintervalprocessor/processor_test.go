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
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
)

func TestAggregation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		passThrough bool
	}{
		{name: "sum_cumulative"},
		{name: "sum_delta"},
		{name: "histogram_cumulative"},
		{name: "histogram_delta"},
		{name: "exphistogram_cumulative"},
		{name: "exphistogram_delta"},
		{name: "summary_enabled"},
		{name: "summary_passthrough", passThrough: true},
	}

	for _, tc := range testCases {
		config := &config.Config{
			Intervals: []config.IntervalConfig{
				{
					Duration: time.Second,
					Statements: []string{
						`set(resource.attributes["custom_res_attr"], "res")`,
						`set(instrumentation_scope.attributes["custom_scope_attr"], "scope")`,
						`set(attributes["custom_dp_attr"], "dp")`,
						`set(resource.attributes["dependent_attr"], Concat([attributes["aaa"], "dependent"], "-"))`,
					},
				},
			},
			PassThrough: config.PassThrough{
				Summary: tc.passThrough,
			},
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRunHelper(t, tc.name, config)
		})
	}
}

func TestAggregationOverflow(t *testing.T) {
	t.Parallel()

	oneCardinalityLimitConfig := config.LimitConfig{
		MaxCardinality: 1,
		Overflow: config.OverflowConfig{
			Attributes: []config.Attribute{{Key: "test_overflow", Value: any(true)}},
		},
	}

	testCases := []struct {
		name string
	}{
		{name: "sum_cumulative_overflow"},
		{name: "sum_delta_overflow"},
		{name: "histogram_cumulative_overflow"},
		{name: "histogram_delta_overflow"},
		{name: "exphistogram_cumulative_overflow"},
		{name: "exphistogram_delta_overflow"},
	}

	for _, tc := range testCases {
		config := &config.Config{
			Intervals: []config.IntervalConfig{
				{
					Duration: time.Second,
					Statements: []string{
						`set(resource.attributes["custom_res_attr"], "res")`,
						`set(instrumentation_scope.attributes["custom_scope_attr"], "scope")`,
						`set(attributes["custom_dp_attr"], "dp")`,
					},
				},
			},
			ResourceLimits:  oneCardinalityLimitConfig,
			ScopeLimits:     oneCardinalityLimitConfig,
			DatapointLimits: oneCardinalityLimitConfig,
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRunHelper(t, tc.name, config)
		})
	}
}

func testRunHelper(t *testing.T, name string, config *config.Config) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	next := &consumertest.MetricsSink{}

	factory := NewFactory()
	settings := processortest.NewNopSettings()
	settings.TelemetrySettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
	mgp, err := factory.CreateMetrics(
		context.Background(),
		settings,
		config,
		next,
	)
	require.NoError(t, err)
	require.IsType(t, &Processor{}, mgp)
	t.Cleanup(func() { require.NoError(t, mgp.Shutdown(context.Background())) })

	dir := filepath.Join("testdata", name)
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
	assert.NoError(t, pmetrictest.CompareMetrics(expectedNextData, allMetrics[0]))

	expectedExportData, err := golden.ReadMetrics(filepath.Join(dir, "output.yaml"))
	require.NoError(t, err)
	assert.NoError(t, pmetrictest.CompareMetrics(expectedExportData, allMetrics[1]))
}

func BenchmarkAggregation(b *testing.B) {
	benchmarkAggregation(b, nil)
}

func BenchmarkAggregationWithOTTL(b *testing.B) {
	benchmarkAggregation(b, []string{
		`set(resource.attributes["custom_res_attr"], "res")`,
		`set(instrumentation_scope.attributes["custom_scope_attr"], "scope")`,
		`set(attributes["custom_dp_attr"], "dp")`,
		`set(resource.attributes["dependent_attr"], Concat([attributes["aaa"], "dependent"], "-"))`,
	})
}

func benchmarkAggregation(b *testing.B, ottlStatements []string) {
	testCases := []struct {
		name        string
		passThrough bool
	}{
		{name: "sum_cumulative"},
		{name: "sum_delta"},
		{name: "histogram_cumulative"},
		{name: "histogram_delta"},
		{name: "exphistogram_cumulative"},
		{name: "exphistogram_delta"},
		{name: "summary_enabled"},
		{name: "summary_passthrough", passThrough: true},
	}

	for _, tc := range testCases {
		config := &config.Config{
			Intervals: []config.IntervalConfig{
				{
					Duration:   time.Hour,
					Statements: ottlStatements,
				},
			},
			PassThrough: config.PassThrough{
				Summary: tc.passThrough,
			},
		}
		b.Run(tc.name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			next := &consumertest.MetricsSink{}

			factory := NewFactory()
			settings := processortest.NewNopSettings()
			settings.TelemetrySettings.Logger = zap.NewNop()
			mgp, err := factory.CreateMetrics(
				context.Background(),
				settings,
				config,
				next,
			)
			require.NoError(b, err)

			dir := filepath.Join("testdata", tc.name)
			md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
			require.NoError(b, err)
			md.MarkReadOnly()
			b.ResetTimer()

			err = mgp.Start(context.Background(), componenttest.NewNopHost())
			require.NoError(b, err)
			for i := 0; i < b.N; i++ {
				mdCopy := pmetric.NewMetrics()
				md.CopyTo(mdCopy)
				// Overwrites the asdf attribute in metrics such that it becomes high cardinality
				mdCopy.ResourceMetrics().At(0).Resource().Attributes().PutStr("asdf", fmt.Sprintf("%d", i))
				err = mgp.ConsumeMetrics(ctx, mdCopy)
				require.NoError(b, err)
			}

			err = mgp.(*Processor).Shutdown(context.Background())
			require.NoError(b, err)
			allMetrics := next.AllMetrics()
			// There should be 1 empty metrics for each of the b.N input metrics,
			// then at last 1 actual merged metrics on shutdown
			assert.Len(b, allMetrics, b.N+1)
		})
	}
}
