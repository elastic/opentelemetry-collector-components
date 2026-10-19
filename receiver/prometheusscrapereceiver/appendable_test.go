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

package prometheusscrapereceiver

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
)

func newTestAppendable(t *testing.T, sink *consumertest.MetricsSink, ext labels.Labels) *appendable {
	t.Helper()
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             receivertest.NewNopSettings(typ).ID,
		Transport:              "http",
		ReceiverCreateSettings: receivertest.NewNopSettings(typ),
	})
	require.NoError(t, err)
	return newAppendable(zap.NewNop(), sink, obsrecv, ext)
}

func appendSample(t *testing.T, app storage.AppenderV2, family string, typ model.MetricType, ls labels.Labels, v float64) storage.SeriesRef {
	t.Helper()
	ref, err := app.Append(0, ls, 0, 1000, v, nil, nil, storage.AppendV2Options{
		MetricFamilyName: family,
		Metadata:         metadata.Metadata{Type: typ, Help: "help " + family, Unit: ""},
	})
	require.NoError(t, err)
	return ref
}

func findMetric(t *testing.T, md pmetric.Metrics, name string) pmetric.Metric {
	t.Helper()
	ms := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	for i := range ms.Len() {
		if ms.At(i).Name() == name {
			return ms.At(i)
		}
	}
	require.Failf(t, "metric not found", "name %q", name)
	return pmetric.Metric{}
}

func TestAppendTypeMapping(t *testing.T) {
	tests := []struct {
		name       string
		family     string
		typ        model.MetricType
		series     string
		wantSum    bool
		wantPromTy string
	}{
		{"counter", "reqs_total", model.MetricTypeCounter, "reqs_total", true, "counter"},
		{"gauge", "temp", model.MetricTypeGauge, "temp", false, "gauge"},
		{"unknown", "mystery", model.MetricTypeUnknown, "mystery", false, "unknown"},
		{"empty type", "mystery", "", "mystery", false, "unknown"},
		{"info", "build_info", model.MetricTypeInfo, "build_info", false, "info"},
		{"stateset", "state", model.MetricTypeStateset, "state", false, "stateset"},
		{"histogram bucket", "lat", model.MetricTypeHistogram, "lat_bucket", true, "histogram"},
		{"histogram sum", "lat", model.MetricTypeHistogram, "lat_sum", true, "histogram"},
		{"histogram count", "lat", model.MetricTypeHistogram, "lat_count", true, "histogram"},
		{"histogram created", "lat", model.MetricTypeHistogram, "lat_created", false, "histogram"},
		{"gaugehistogram bucket", "gh", model.MetricTypeGaugeHistogram, "gh_bucket", false, "gaugehistogram"},
		{"summary quantile", "rpc", model.MetricTypeSummary, "rpc", false, "summary"},
		{"summary sum", "rpc", model.MetricTypeSummary, "rpc_sum", true, "summary"},
		{"summary count", "rpc", model.MetricTypeSummary, "rpc_count", true, "summary"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := new(consumertest.MetricsSink)
			app := newTestAppendable(t, sink, labels.EmptyLabels()).AppenderV2(context.Background())
			ls := labels.FromStrings("__name__", tt.series, "le", "0.00001", "a__b", "x")
			ref := appendSample(t, app, tt.family, tt.typ, ls, 3)
			assert.NotZero(t, ref)
			require.NoError(t, app.Commit())

			require.Len(t, sink.AllMetrics(), 1)
			md := sink.AllMetrics()[0]
			assert.Equal(t, 0, md.ResourceMetrics().At(0).Resource().Attributes().Len())
			scope := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Scope()
			assert.Empty(t, scope.Name())
			assert.Empty(t, scope.Version())

			m := findMetric(t, md, tt.series)
			assert.Equal(t, "help "+tt.family, m.Description())
			assert.Empty(t, m.Unit())
			pt, _ := m.Metadata().Get("prometheus.type")
			assert.Equal(t, tt.wantPromTy, pt.Str())

			var dp pmetric.NumberDataPoint
			if tt.wantSum {
				require.Equal(t, pmetric.MetricTypeSum, m.Type())
				assert.True(t, m.Sum().IsMonotonic())
				assert.Equal(t, pmetric.AggregationTemporalityCumulative, m.Sum().AggregationTemporality())
				dp = m.Sum().DataPoints().At(0)
			} else {
				require.Equal(t, pmetric.MetricTypeGauge, m.Type())
				dp = m.Gauge().DataPoints().At(0)
			}
			assert.InDelta(t, 3, dp.DoubleValue(), 0)
			assert.EqualValues(t, 1_000_000_000, dp.Timestamp())
			assert.Zero(t, dp.StartTimestamp())
			assert.Zero(t, dp.Flags())
			le, _ := dp.Attributes().Get("le")
			assert.Equal(t, "0.00001", le.Str())
			ab, _ := dp.Attributes().Get("a__b")
			assert.Equal(t, "x", ab.Str())
			_, hasName := dp.Attributes().Get("__name__")
			assert.False(t, hasName)
		})
	}
}

func TestAppendUnit(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	app := newTestAppendable(t, sink, labels.EmptyLabels()).AppenderV2(context.Background())
	_, err := app.Append(0, labels.FromStrings("__name__", "x"), 0, 1, 1, nil, nil, storage.AppendV2Options{
		Metadata: metadata.Metadata{Type: model.MetricTypeGauge, Unit: "seconds"},
	})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	m := findMetric(t, sink.AllMetrics()[0], "x")
	assert.Empty(t, m.Unit())
	u, _ := m.Metadata().Get("prometheus.unit")
	assert.Equal(t, "seconds", u.Str())
}

func TestAppendStaleNaNKeptRaw(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	app := newTestAppendable(t, sink, labels.EmptyLabels()).AppenderV2(context.Background())
	stale := math.Float64frombits(value.StaleNaN)
	appendSample(t, app, "g", model.MetricTypeGauge, labels.FromStrings("__name__", "g"), stale)
	require.NoError(t, app.Commit())

	dp := findMetric(t, sink.AllMetrics()[0], "g").Gauge().DataPoints().At(0)
	assert.True(t, value.IsStaleNaN(dp.DoubleValue()))
	assert.Zero(t, dp.Flags())
}

func TestAppendExternalLabels(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	ext := labels.FromStrings("cluster", "c1", "region", "eu")
	app := newTestAppendable(t, sink, ext).AppenderV2(context.Background())
	appendSample(t, app, "g", model.MetricTypeGauge, labels.FromStrings("__name__", "g", "region", "us"), 1)
	require.NoError(t, app.Commit())

	attrs := findMetric(t, sink.AllMetrics()[0], "g").Gauge().DataPoints().At(0).Attributes()
	v, _ := attrs.Get("cluster")
	assert.Equal(t, "c1", v.Str())
	v, _ = attrs.Get("region")
	assert.Equal(t, "us", v.Str(), "existing labels win over external labels")
}

func TestAppendDrops(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	app := newTestAppendable(t, sink, labels.EmptyLabels()).AppenderV2(context.Background())

	// Custom bucket histograms (NHCB) are not exponential.
	ref, err := app.Append(0, labels.FromStrings("__name__", "nhcb"), 0, 1, 0, &histogram.Histogram{Schema: histogram.CustomBucketsSchema}, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	assert.NotZero(t, ref)
	ref, err = app.Append(0, labels.FromStrings("a", "b"), 0, 1, 1, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	assert.NotZero(t, ref)

	require.NoError(t, app.Commit())
	assert.Empty(t, sink.AllMetrics(), "nothing to consume when everything was dropped")
}

func TestAppendRollback(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	app := newTestAppendable(t, sink, labels.EmptyLabels()).AppenderV2(context.Background())
	appendSample(t, app, "g", model.MetricTypeGauge, labels.FromStrings("__name__", "g"), 1)
	require.NoError(t, app.Rollback())
	assert.Empty(t, sink.AllMetrics())
}

func TestAppendSameSeriesRefStable(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	a := newTestAppendable(t, sink, labels.EmptyLabels())
	ls := labels.FromStrings("__name__", "g", "a", "b")
	r1 := appendSample(t, a.AppenderV2(context.Background()), "g", model.MetricTypeGauge, ls, 1)
	r2 := appendSample(t, a.AppenderV2(context.Background()), "g", model.MetricTypeGauge, ls, 2)
	assert.Equal(t, r1, r2)
}

func TestAppendConcurrentAppenders(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	a := newTestAppendable(t, sink, labels.FromStrings("cluster", "c"))
	const n = 8
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			app := a.AppenderV2(context.Background())
			for j := range 100 {
				ls := labels.FromStrings("__name__", "m", "i", string(rune('a'+i)), "j", string(rune('a'+j%26)))
				if _, err := app.Append(0, ls, 0, int64(j), float64(j), nil, nil, storage.AppendV2Options{
					Metadata: metadata.Metadata{Type: model.MetricTypeCounter},
				}); err != nil {
					t.Error(err)
				}
			}
			if err := app.Commit(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	assert.Equal(t, n*100, sink.DataPointCount())
	assert.Len(t, sink.AllMetrics(), n)
}

func nativeHistogram() *histogram.Histogram {
	return &histogram.Histogram{
		Schema:        3,
		Count:         10,
		Sum:           12.5,
		ZeroThreshold: 1e-128,
		ZeroCount:     2,
		// Buckets at indexes 1,2 (counts 3,1) then 5 (count 2) as deltas: 3,-2,1.
		PositiveSpans:   []histogram.Span{{Offset: 1, Length: 2}, {Offset: 2, Length: 1}},
		PositiveBuckets: []int64{3, -2, 1},
		NegativeSpans:   []histogram.Span{{Offset: -2, Length: 1}},
		NegativeBuckets: []int64{2},
	}
}

func TestAppendNativeHistogram(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	app := newTestAppendable(t, sink, labels.FromStrings("cluster", "c")).AppenderV2(context.Background())
	ref, err := app.Append(0, labels.FromStrings("__name__", "rpc_duration", "job", "j"), 0, 1000, 0, nativeHistogram(), nil,
		storage.AppendV2Options{Metadata: metadata.Metadata{Type: model.MetricTypeHistogram, Help: "help"}})
	require.NoError(t, err)
	assert.NotZero(t, ref)
	require.NoError(t, app.Commit())

	m := findMetric(t, sink.AllMetrics()[0], "rpc_duration")
	require.Equal(t, pmetric.MetricTypeExponentialHistogram, m.Type())
	assert.Equal(t, pmetric.AggregationTemporalityCumulative, m.ExponentialHistogram().AggregationTemporality())
	assert.Equal(t, "help", m.Description())
	pt, _ := m.Metadata().Get("prometheus.type")
	assert.Equal(t, "histogram", pt.Str())

	dp := m.ExponentialHistogram().DataPoints().At(0)
	assert.EqualValues(t, 1_000_000_000, dp.Timestamp())
	assert.Zero(t, dp.Flags())
	assert.Equal(t, int32(3), dp.Scale())
	assert.EqualValues(t, 10, dp.Count())
	assert.InDelta(t, 12.5, dp.Sum(), 0)
	assert.EqualValues(t, 2, dp.ZeroCount())
	// OTel offsets refer to the lower bound: Prometheus offset - 1.
	assert.Equal(t, int32(0), dp.Positive().Offset())
	assert.Equal(t, []uint64{3, 1, 0, 0, 2}, dp.Positive().BucketCounts().AsRaw())
	assert.Equal(t, int32(-3), dp.Negative().Offset())
	assert.Equal(t, []uint64{2}, dp.Negative().BucketCounts().AsRaw())
	job, _ := dp.Attributes().Get("job")
	assert.Equal(t, "j", job.Str())
	cl, _ := dp.Attributes().Get("cluster")
	assert.Equal(t, "c", cl.Str())
}

func TestAppendNativeFloatHistogram(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	app := newTestAppendable(t, sink, labels.EmptyLabels()).AppenderV2(context.Background())
	fh := &histogram.FloatHistogram{
		Schema: 0, Count: 5, Sum: 3,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 2}},
		PositiveBuckets: []float64{2, 3},
	}
	_, err := app.Append(0, labels.FromStrings("__name__", "fh"), 0, 1, 0, nil, fh, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	dp := findMetric(t, sink.AllMetrics()[0], "fh").ExponentialHistogram().DataPoints().At(0)
	assert.Equal(t, []uint64{2, 3}, dp.Positive().BucketCounts().AsRaw())
	assert.EqualValues(t, 5, dp.Count())
}

// A staleness marker is a float StaleNaN appended with the reference returned for
// the series: it must stay an exponential histogram point with a StaleNaN sum.
func TestAppendNativeHistogramStale(t *testing.T) {
	a := newTestAppendable(t, new(consumertest.MetricsSink), labels.EmptyLabels())
	ls := labels.FromStrings("__name__", "nh", "job", "j")

	app := a.AppenderV2(context.Background())
	ref, err := app.Append(0, ls, 0, 1, 0, nativeHistogram(), nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	sink := new(consumertest.MetricsSink)
	a = newTestAppendable(t, sink, labels.EmptyLabels())
	app = a.AppenderV2(context.Background())
	_, err = app.Append(ref, ls, 0, 2, math.Float64frombits(value.StaleNaN), nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	m := findMetric(t, sink.AllMetrics()[0], "nh")
	require.Equal(t, pmetric.MetricTypeExponentialHistogram, m.Type())
	dp := m.ExponentialHistogram().DataPoints().At(0)
	assert.True(t, value.IsStaleNaN(dp.Sum()))
	assert.Zero(t, dp.Flags())
}

func TestAppendFloatSeriesRefHasNoHistogramBit(t *testing.T) {
	a := newTestAppendable(t, new(consumertest.MetricsSink), labels.EmptyLabels())
	ls := labels.FromStrings("__name__", "g")
	ref := appendSample(t, a.AppenderV2(context.Background()), "g", model.MetricTypeGauge, ls, 1)
	assert.Zero(t, uint64(ref)&nativeHistogramRefBit)
}
