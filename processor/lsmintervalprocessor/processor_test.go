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
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/metadatatest"
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
			ExponentialHistogramMaxBuckets: 160,
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRunHelperAllModes(t, tc.name, config)
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
			ResourceLimit:                  oneCardinalityLimitConfig,
			ScopeLimit:                     oneCardinalityLimitConfig,
			MetricLimit:                    oneCardinalityLimitConfig,
			DatapointLimit:                 oneCardinalityLimitConfig,
			ExponentialHistogramMaxBuckets: 160,
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRunHelperAllModes(t, tc.name, config)
		})
	}
}

// preAggregationModes runs tests with the pre-aggregation buffer disabled,
// enabled, and enabled with a size that flushes on every request. All modes
// must produce identical results.
var preAggregationModes = []struct {
	name string
	cfg  config.PreAggregationConfig
}{
	{name: "pre_aggregation=off"},
	{name: "pre_aggregation=on", cfg: config.PreAggregationConfig{Enabled: true}},
	{name: "pre_aggregation=flush_every_request", cfg: config.PreAggregationConfig{Enabled: true, MaxSize: 1}},
}

func testRunHelperAllModes(t *testing.T, name string, cfg *config.Config) {
	t.Helper()
	for _, mode := range preAggregationModes {
		c := *cfg
		c.PreAggregation = mode.cfg
		t.Run(mode.name, func(t *testing.T) {
			testRunHelper(t, name, &c)
		})
	}
}

func testRunHelper(t *testing.T, name string, config *config.Config) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testTel := componenttest.NewTelemetry()
	telSettings := testTel.NewTelemetrySettings()
	telSettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))

	dir := filepath.Join("testdata", name)
	md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
	require.NoError(t, err)

	// Start the processor and feed the metrics
	next := &consumertest.MetricsSink{}
	mgp := newTestProcessor(t, config, telSettings, next)
	err = mgp.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	err = mgp.ConsumeMetrics(ctx, md)
	require.NoError(t, err)

	expectedNextData, err := golden.ReadMetrics(filepath.Join(dir, "next.yaml"))
	require.NoError(t, err)

	var allMetrics []pmetric.Metrics
	require.Eventually(t, func() bool {
		// next will be called only for cases when the input has not aggregated
		// all the datapoints. We check if the expected next has non-zero datapoint
		// and assert based on that.
		allMetrics = next.AllMetrics()
		if expectedNextData.DataPointCount() > 0 {
			return len(allMetrics) == 2
		}
		return len(allMetrics) == 1
	}, 5*time.Second, 100*time.Millisecond)

	var idx int
	if expectedNextData.DataPointCount() > 0 {
		assert.NoError(t, pmetrictest.CompareMetrics(expectedNextData, allMetrics[idx]))
		idx++
	}

	expectedExportData, err := golden.ReadMetrics(filepath.Join(dir, "output.yaml"))
	require.NoError(t, err)
	assert.NoError(t, pmetrictest.CompareMetrics(expectedExportData, allMetrics[idx]))
	// Assert internal telemetry metrics.
	metadatatest.AssertEqualLsmintervalProcessedDataPoints(t, testTel, []metricdata.DataPoint[int64]{
		{
			Value: int64(md.DataPointCount()),
		},
	}, metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualLsmintervalProcessedBytes(t, testTel, []metricdata.DataPoint[int64]{
		{},
	}, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreValue())
	metadatatest.AssertEqualLsmintervalExportedDataPoints(t, testTel, []metricdata.DataPoint[int64]{
		{
			Value:      int64(expectedExportData.DataPointCount()),
			Attributes: attribute.NewSet(attribute.String("interval", "1s")),
		},
	}, metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualLsmintervalExportedBytes(t, testTel, []metricdata.DataPoint[int64]{
		{
			Attributes: attribute.NewSet(attribute.String("interval", "1s")),
		},
	}, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreValue())
	assertPebbleMetric(t, testTel)
}

func newTestProcessor(t testing.TB, cfg *config.Config, telSettings component.TelemetrySettings, next consumer.Metrics) processor.Metrics {
	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	settings.TelemetrySettings = telSettings
	mgp, err := factory.CreateMetrics(context.Background(), settings, cfg, next)
	require.NoError(t, err)
	require.IsType(t, &Processor{}, mgp)
	t.Cleanup(func() { require.NoError(t, mgp.Shutdown(context.Background())) })
	return mgp
}

func assertPebbleMetric(t *testing.T, testTel *componenttest.Telemetry) {
	t.Helper()
	// Just assert that the metrics were reported from pebble.
	checks := []func(*testing.T, *componenttest.Telemetry, []metricdata.DataPoint[int64], ...metricdatatest.Option){
		metadatatest.AssertEqualLsmintervalPebbleCompactedBytesRead,
		metadatatest.AssertEqualLsmintervalPebbleCompactedBytesWritten,
		metadatatest.AssertEqualLsmintervalPebbleCompactions,
		metadatatest.AssertEqualLsmintervalPebbleFlushedBytes,
		metadatatest.AssertEqualLsmintervalPebbleFlushes,
		metadatatest.AssertEqualLsmintervalPebbleIngestedBytes,
		metadatatest.AssertEqualLsmintervalPebbleKeysTombstones,
		metadatatest.AssertEqualLsmintervalPebbleMarkedForCompactionFiles,
		metadatatest.AssertEqualLsmintervalPebblePendingCompaction,
		metadatatest.AssertEqualLsmintervalPebbleReadAmplification,
		metadatatest.AssertEqualLsmintervalPebbleReadersMemory,
		metadatatest.AssertEqualLsmintervalPebbleSstables,
		metadatatest.AssertEqualLsmintervalPebbleTotalDiskUsage,
		metadatatest.AssertEqualLsmintervalPebbleTotalMemtableSize,
	}
	for _, f := range checks {
		f(t, testTel, []metricdata.DataPoint[int64]{{}}, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreValue())
	}
}

func TestClientMetadata(t *testing.T) {
	for _, mode := range preAggregationModes {
		t.Run(mode.name, func(t *testing.T) {
			testClientMetadata(t, mode.cfg)
		})
	}
}

func testClientMetadata(t *testing.T, preAggregation config.PreAggregationConfig) {
	cfg := &config.Config{
		PreAggregation: preAggregation,
		MetadataKeys:   []string{"k1", "k2"},
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
	settings := processortest.NewNopSettings(metadata.Type)
	p := newTestProcessor(t, cfg, settings.TelemetrySettings, next)
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

func TestConcurrentShutdownConsumeMetrics(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		Intervals: []config.IntervalConfig{{
			Duration: time.Hour,
		}},
	}

	testTel := componenttest.NewTelemetry()
	telSettings := testTel.NewTelemetrySettings()
	telSettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))

	next := &consumertest.MetricsSink{}
	p := newTestProcessor(t, cfg, telSettings, next)

	// Start the processor
	err := p.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Create test metrics
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("test_metric")
	sum := m.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	sum.DataPoints().AppendEmpty().SetIntValue(1)

	var wg sync.WaitGroup
	shutdownStarted := make(chan struct{})

	// Start multiple ConsumeMetrics in goroutines
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-shutdownStarted
			// This should not panic even if Shutdown is running concurrently
			_ = p.(*Processor).ConsumeMetrics(context.Background(), md)
		}()
	}

	// Start Shutdown in another goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(shutdownStarted)
		_ = p.(*Processor).Shutdown(context.Background())
	}()

	wg.Wait()
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
			settings := processortest.NewNopSettings(metadata.Type)
			settings.Logger = zap.NewNop()
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

			b.RunParallel(func(pb *testing.PB) {
				// Create a copy of the metrics for each goroutine,
				// so they can set their own resource attributes.
				mdCopy := pmetric.NewMetrics()
				md.CopyTo(mdCopy)
				highCardinalityValuePrefix := fmt.Sprint("rand_", rand.Int())

				for i := 0; pb.Next(); i++ {
					highCardinalityValue := fmt.Sprintf("%s_%d", highCardinalityValuePrefix, i)
					mdCopy.ResourceMetrics().At(0).Resource().Attributes().PutStr("asdf", highCardinalityValue)
					if err := mgp.ConsumeMetrics(ctx, mdCopy); err != nil {
						b.Fatal(err)
					}
				}
			})

			err = mgp.(*Processor).Shutdown(context.Background())
			require.NoError(b, err)
			allMetrics := next.AllMetrics()
			if tc.passThrough {
				// For pass-through, there should be 1 metrics for each
				// of the b.N inputs, plus 1 merged metrics on shutdown.
				assert.Len(b, allMetrics, b.N+1)
			} else {
				// For non pass-through, metrics are merged internally
				// and only exported on shutdown.
				assert.NotEmpty(b, allMetrics)
			}
		})
	}
}

func TestWriteStallListener(t *testing.T) {
	testTel := componenttest.NewTelemetry()
	tb, err := metadata.NewTelemetryBuilder(testTel.NewTelemetrySettings())
	require.NoError(t, err)

	l := newWriteStallListener(tb)
	for range 2 {
		l.WriteStallBegin(pebble.WriteStallBeginInfo{Reason: "memtable count limit reached"})
		time.Sleep(5 * time.Millisecond)
		l.WriteStallEnd()
	}

	reasonAttr := attribute.NewSet(attribute.String("reason", "memtable count limit reached"))
	metadatatest.AssertEqualLsmintervalPebbleWriteStalls(t, testTel, []metricdata.DataPoint[int64]{
		{Value: 2, Attributes: reasonAttr},
	}, metricdatatest.IgnoreTimestamp())

	m, err := testTel.GetMetric("otelcol_lsminterval.pebble_write_stall_duration")
	require.NoError(t, err)
	dps := m.Data.(metricdata.Sum[float64]).DataPoints
	require.Len(t, dps, 1)
	assert.Equal(t, reasonAttr, dps[0].Attributes)
	assert.GreaterOrEqual(t, dps[0].Value, 0.01)
}

// TestPreAggregationConcurrentTotals sends delta sums from concurrent callers
// for several tenants across multiple processing windows and checks that no
// value is lost or double counted, with and without pre-aggregation.
func TestPreAggregationConcurrentTotals(t *testing.T) {
	const (
		callers           = 8
		requestsPerCaller = 300
		dpsPerRequest     = 5
		tenants           = 4
	)
	intervals := []config.IntervalConfig{{Duration: time.Second}, {Duration: 2 * time.Second}}
	for _, mode := range []config.PreAggregationConfig{
		{},
		{Enabled: true},
		{Enabled: true, MaxSize: 16 << 10}, // also exercises size-triggered flushes
	} {
		t.Run(fmt.Sprintf("enabled=%t/max_size=%d", mode.Enabled, mode.MaxSize), func(t *testing.T) {
			var exported atomic.Int64
			next, _ := consumer.NewMetrics(consumer.ConsumeMetricsFunc(
				func(_ context.Context, md pmetric.Metrics) error {
					rms := md.ResourceMetrics()
					for i := 0; i < rms.Len(); i++ {
						sms := rms.At(i).ScopeMetrics()
						for j := 0; j < sms.Len(); j++ {
							ms := sms.At(j).Metrics()
							for k := 0; k < ms.Len(); k++ {
								dps := ms.At(k).Sum().DataPoints()
								for l := 0; l < dps.Len(); l++ {
									exported.Add(dps.At(l).IntValue())
								}
							}
						}
					}
					return nil
				},
			))
			cfg := &config.Config{
				Directory:                      t.TempDir(),
				Intervals:                      intervals,
				MetadataKeys:                   []string{"tenant"},
				ExponentialHistogramMaxBuckets: 160,
				PreAggregation:                 mode,
			}
			settings := processortest.NewNopSettings(metadata.Type)
			p, err := NewFactory().CreateMetrics(context.Background(), settings, cfg, next)
			require.NoError(t, err)
			require.NoError(t, p.Start(context.Background(), componenttest.NewNopHost()))

			var wg sync.WaitGroup
			for c := 0; c < callers; c++ {
				wg.Add(1)
				go func(c int) {
					defer wg.Done()
					for r := 0; r < requestsPerCaller; r++ {
						md := pmetric.NewMetrics()
						m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
						m.SetName("requests")
						sum := m.SetEmptySum()
						sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
						sum.SetIsMonotonic(true)
						for d := 0; d < dpsPerRequest; d++ {
							dp := sum.DataPoints().AppendEmpty()
							dp.Attributes().PutInt("series", int64(d))
							dp.SetIntValue(1)
						}
						ctx := client.NewContext(context.Background(), client.Info{
							Metadata: client.NewMetadata(map[string][]string{
								"tenant": {fmt.Sprint("tenant-", (c+r)%tenants)},
							}),
						})
						assert.NoError(t, p.ConsumeMetrics(ctx, md))
						// Spread the requests over a few processing windows.
						time.Sleep(5 * time.Millisecond)
					}
				}(c)
			}
			wg.Wait()
			require.NoError(t, p.Shutdown(context.Background()))

			want := int64(callers * requestsPerCaller * dpsPerRequest * len(intervals))
			assert.Equal(t, want, exported.Load())
		})
	}
}
