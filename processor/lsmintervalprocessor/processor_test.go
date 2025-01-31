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
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.opentelemetry.io/otel/attribute"
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
			ResourceLimit:  oneCardinalityLimitConfig,
			ScopeLimit:     oneCardinalityLimitConfig,
			MetricLimit:    oneCardinalityLimitConfig,
			DatapointLimit: oneCardinalityLimitConfig,
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

	dir := filepath.Join("testdata", name)
	md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
	require.NoError(t, err)

	// Start the processor and feed the metrics
	next := &consumertest.MetricsSink{}
	mgp := newTestProcessor(t, config, next)
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

func newTestProcessor(t testing.TB, cfg *config.Config, next consumer.Metrics) processor.Metrics {
	factory := NewFactory()
	settings := processortest.NewNopSettings()
	settings.TelemetrySettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
	mgp, err := factory.CreateMetrics(context.Background(), settings, cfg, next)
	require.NoError(t, err)
	require.IsType(t, &Processor{}, mgp)
	t.Cleanup(func() { require.NoError(t, mgp.Shutdown(context.Background())) })
	return mgp
}

func TestClientMetadata(t *testing.T) {
	cfg := &config.Config{
		MetadataKeys: []string{"k1", "k2"},
		Intervals: []config.IntervalConfig{{
			Duration: time.Minute,
		}},
	}

	// N is the number of times to send each delta data point,
	// and the expected aggregated value.
	const N = 10

	received := make(map[attribute.Set]int64)
	next, _ := consumer.NewMetrics(consumer.ConsumeMetricsFunc(
		func(ctx context.Context, metrics pmetric.Metrics) error {
			if metrics.MetricCount() == 0 {
				// This is the original input, with
				// pending aggregated metrics removed.
				return nil
			}
			info := client.FromContext(ctx)

			// k3 should never be in the metadata even if it's in
			// the original input, as it's not a configured key
			// in the processor.
			require.Nil(t, info.Metadata.Get("k3"))

			var kvs []attribute.KeyValue
			for _, k := range cfg.MetadataKeys {
				vs := info.Metadata.Get(k)
				if vs == nil {
					continue
				}
				kvs = append(kvs, attribute.StringSlice(k, vs))
			}
			attrs := attribute.NewSet(kvs...)

			require.Equal(t, 1, metrics.MetricCount())
			metric := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
			dp := metric.Sum().DataPoints().At(0)
			received[attrs] = dp.IntValue()
			return nil
		},
	))
	p := newTestProcessor(t, cfg, next)
	err := p.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	metadataCombinations := []map[string][]string{
		{}, // no metadata
		{
			"k3": []string{"ignored"},
		},
		{
			"k1": []string{"v1"},
		},
		{
			"k1": []string{"v2"},
		},
		{
			"k2": []string{"v1"},
		},
		{
			"k2": []string{"v1", "v2"},
		},
		{
			"k1": []string{"v1"},
			"k2": []string{"v1"},
			"k3": []string{"ignored"},
		},
	}

	expected := map[attribute.Set]int64{
		// We should have aggregated the metrics with no metadata
		// and with only ignored metadata together.
		attribute.NewSet( /*empty*/ ): 20,

		attribute.NewSet(attribute.StringSlice("k1", []string{"v1"})):       10,
		attribute.NewSet(attribute.StringSlice("k1", []string{"v2"})):       10,
		attribute.NewSet(attribute.StringSlice("k2", []string{"v1"})):       10,
		attribute.NewSet(attribute.StringSlice("k2", []string{"v1", "v2"})): 10,
		attribute.NewSet(
			attribute.StringSlice("k1", []string{"v1"}),
			attribute.StringSlice("k2", []string{"v1"}),
			// k3 should have been dropped
		): 10,
	}

	// Create a single metric data point to aggregate per combination of client metadata.
	for _, metadata := range metadataCombinations {
		info := client.Info{Metadata: client.NewMetadata(metadata)}
		ctx := client.NewContext(context.Background(), info)

		for i := 0; i < N; i++ {
			// We must create a new pmetric.Metrics per iteration because
			// p.ConsumeMetrics is mutating.
			metrics := pmetric.NewMetrics()
			rm := metrics.ResourceMetrics().AppendEmpty()
			sm := rm.ScopeMetrics().AppendEmpty()
			m := sm.Metrics().AppendEmpty()
			m.SetName("metric_name")
			sum := m.SetEmptySum()
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			sum.DataPoints().AppendEmpty().SetIntValue(1)

			err = p.ConsumeMetrics(ctx, metrics)
			require.NoError(t, err)
		}
	}

	// Shutdown forces all consumed data to be immediately finalised.
	err = p.Shutdown(context.Background())
	require.NoError(t, err)

	assert.Equal(t, expected, received)
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
