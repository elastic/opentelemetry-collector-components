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

package prometheusscrapereceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/prometheusscrapereceiver"

import (
	"context"
	"math"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/receiver/prometheusscrapereceiver/internal/metadata"
)

const (
	// metadataKeyType is the metric metadata key holding the original Prometheus family type.
	metadataKeyType = "prometheus.type"
	// metadataKeyUnit is the metric metadata key holding the raw Prometheus UNIT.
	metadataKeyUnit = "prometheus.unit"
)

var _ storage.AppendableV2 = (*appendable)(nil)

// appendable hands out one independent appender per scrape loop iteration.
// It holds no mutable state, so it is safe for concurrent use.
type appendable struct {
	logger         *zap.Logger
	consumer       consumer.Metrics
	obsrecv        *receiverhelper.ObsReport
	externalLabels labels.Labels
}

func newAppendable(logger *zap.Logger, next consumer.Metrics, obsrecv *receiverhelper.ObsReport, externalLabels labels.Labels) *appendable {
	return &appendable{logger: logger, consumer: next, obsrecv: obsrecv, externalLabels: externalLabels}
}

// AppenderV2 implements storage.AppendableV2.
func (a *appendable) AppenderV2(ctx context.Context) storage.AppenderV2 {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	return &appender{
		ctx:     ctx,
		parent:  a,
		md:      md,
		metrics: sm.Metrics(),
		byKey:   map[metricKey]pmetric.Metric{},
	}
}

// metricKey identifies a metric within one scrape. Float and native histogram
// samples of the same name never share a metric.
type metricKey struct {
	name string
	typ  pmetric.MetricType
}

// nativeHistogramRefBit marks the series references of native histogram series.
// The scrape loop hands the reference back when it appends a staleness marker,
// which is a plain float sample, so this is how a stale native histogram series
// is told apart from a stale float series.
const nativeHistogramRefBit = uint64(1) << 63

// appender buffers the samples of a single scrape into one pmetric.Metrics.
// It is not goroutine-safe, as per the storage.AppenderV2 contract.
type appender struct {
	ctx    context.Context
	parent *appendable

	md      pmetric.Metrics
	metrics pmetric.MetricSlice
	byKey   map[metricKey]pmetric.Metric
	points  int

	// Custom bucket native histograms dropped in this scrape, reported once on Commit.
	droppedCustomBuckets int
	firstDropped         string
}

// Append implements storage.AppenderV2.
//
// Only a nil error or a partial error may be returned for dropped samples:
// any other error would fail the whole scrape.
func (a *appender) Append(ref storage.SeriesRef, ls labels.Labels, _, t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram, opts storage.AppendV2Options) (storage.SeriesRef, error) {
	name := ls.Get(model.MetricNameLabel)
	if name == "" {
		a.parent.logger.Debug("dropping sample without metric name", zap.String("series", ls.String()))
		return storage.SeriesRef(seriesHash(ls)), nil
	}

	switch {
	case h != nil || fh != nil:
		return a.appendNativeHistogram(ls, name, t, h, fh, opts), nil
	case uint64(ref)&nativeHistogramRefBit != 0 && value.IsStaleNaN(v):
		// Staleness marker of a native histogram series.
		return a.appendNativeHistogramStale(ls, name, t, opts), nil
	}

	m := a.metric(name, pmetric.MetricTypeGauge, opts)
	var dp pmetric.NumberDataPoint
	if m.Type() == pmetric.MetricTypeSum {
		dp = m.Sum().DataPoints().AppendEmpty()
	} else {
		dp = m.Gauge().DataPoints().AppendEmpty()
	}
	dp.SetTimestamp(pcommon.Timestamp(t * 1e6))
	// The raw value is kept, including the Prometheus StaleNaN bit pattern.
	dp.SetDoubleValue(v)
	a.putAttributes(dp.Attributes(), ls)

	a.points++
	return storage.SeriesRef(seriesHash(ls) &^ nativeHistogramRefBit), nil
}

// appendNativeHistogram converts a Prometheus native histogram into an
// ExponentialHistogram datapoint, following
// https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/#exponential-histograms
func (a *appender) appendNativeHistogram(ls labels.Labels, name string, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram, opts storage.AppendV2Options) storage.SeriesRef {
	ref := storage.SeriesRef(seriesHash(ls) | nativeHistogramRefBit)

	schema := int32(0)
	switch {
	case fh != nil:
		schema = fh.Schema
	case h != nil:
		schema = h.Schema
	}
	// Custom bucket (NHCB) histograms use schema -53 and are not exponential, so
	// they can't be represented as an ExponentialHistogram. They are counted and
	// reported once per scrape on Commit.
	if !histogram.IsExponentialSchema(schema) {
		if a.droppedCustomBuckets == 0 {
			a.firstDropped = ls.String()
		}
		a.droppedCustomBuckets++
		return ref
	}

	dp := a.exponentialPoint(ls, name, t, opts)
	// Min and Max are not available in native histograms.
	switch {
	case fh != nil:
		// Float histograms are rounded down to the integer counts of OTel.
		dp.SetScale(fh.Schema)
		dp.SetCount(uint64(fh.Count))
		dp.SetSum(fh.Sum)
		dp.SetZeroThreshold(fh.ZeroThreshold)
		dp.SetZeroCount(uint64(fh.ZeroCount))
		fillAbsoluteBuckets(dp.Positive(), fh.PositiveSpans, fh.PositiveBuckets)
		fillAbsoluteBuckets(dp.Negative(), fh.NegativeSpans, fh.NegativeBuckets)
	default:
		dp.SetScale(h.Schema)
		dp.SetCount(h.Count)
		dp.SetSum(h.Sum)
		dp.SetZeroThreshold(h.ZeroThreshold)
		dp.SetZeroCount(h.ZeroCount)
		fillDeltaBuckets(dp.Positive(), h.PositiveSpans, h.PositiveBuckets)
		fillDeltaBuckets(dp.Negative(), h.NegativeSpans, h.NegativeBuckets)
	}
	return ref
}

// appendNativeHistogramStale emits the staleness marker of a native histogram
// series. Prometheus identifies a stale native histogram by its sum, so the raw
// StaleNaN is kept there and no flag is set.
func (a *appender) appendNativeHistogramStale(ls labels.Labels, name string, t int64, opts storage.AppendV2Options) storage.SeriesRef {
	dp := a.exponentialPoint(ls, name, t, opts)
	dp.SetSum(math.Float64frombits(value.StaleNaN))
	return storage.SeriesRef(seriesHash(ls) | nativeHistogramRefBit)
}

func (a *appender) exponentialPoint(ls labels.Labels, name string, t int64, opts storage.AppendV2Options) pmetric.ExponentialHistogramDataPoint {
	m := a.metric(name, pmetric.MetricTypeExponentialHistogram, opts)
	dp := m.ExponentialHistogram().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(t * 1e6))
	a.putAttributes(dp.Attributes(), ls)
	a.points++
	return dp
}

func (a *appender) putAttributes(attrs pcommon.Map, ls labels.Labels) {
	ls.Range(func(l labels.Label) {
		if l.Name != model.MetricNameLabel {
			attrs.PutStr(l.Name, l.Value)
		}
	})
	a.parent.externalLabels.Range(func(l labels.Label) {
		if _, exists := attrs.Get(l.Name); !exists {
			attrs.PutStr(l.Name, l.Value)
		}
	})
}

// fillDeltaBuckets converts the delta encoded spans of an integer native
// histogram into dense OTel bucket counts. OTel offsets refer to the lower
// bound of a bucket, Prometheus offsets to the upper bound, hence the -1.
func fillDeltaBuckets(dst pmetric.ExponentialHistogramDataPointBuckets, spans []histogram.Span, deltas []int64) {
	if len(spans) == 0 {
		return
	}
	dst.SetOffset(spans[0].Offset - 1)
	counts := dst.BucketCounts()
	counts.EnsureCapacity(len(deltas))
	idx, count := 0, int64(0)
	for i, span := range spans {
		if i > 0 {
			for range span.Offset {
				counts.Append(0)
			}
		}
		for range span.Length {
			count += deltas[idx]
			idx++
			counts.Append(uint64(count))
		}
	}
}

func fillAbsoluteBuckets(dst pmetric.ExponentialHistogramDataPointBuckets, spans []histogram.Span, values []float64) {
	if len(spans) == 0 {
		return
	}
	dst.SetOffset(spans[0].Offset - 1)
	counts := dst.BucketCounts()
	counts.EnsureCapacity(len(values))
	idx := 0
	for i, span := range spans {
		if i > 0 {
			for range span.Offset {
				counts.Append(0)
			}
		}
		for range span.Length {
			counts.Append(uint64(values[idx]))
			idx++
		}
	}
}

// metric returns the metric for name, creating it on first use. defaultType
// selects between the Gauge/Sum mapping and the ExponentialHistogram one.
func (a *appender) metric(name string, defaultType pmetric.MetricType, opts storage.AppendV2Options) pmetric.Metric {
	k := metricKey{name: name, typ: defaultType}
	if m, ok := a.byKey[k]; ok {
		return m
	}
	m := a.newMetric(name, defaultType, opts)
	a.byKey[k] = m
	return m
}

func (a *appender) newMetric(name string, mt pmetric.MetricType, opts storage.AppendV2Options) pmetric.Metric {
	m := a.metrics.AppendEmpty()
	m.SetName(name)
	m.SetDescription(opts.Metadata.Help)

	typ := opts.Metadata.Type
	if typ == "" {
		typ = model.MetricTypeUnknown
	}
	m.Metadata().PutStr(metadataKeyType, string(typ))
	if opts.Metadata.Unit != "" {
		m.Metadata().PutStr(metadataKeyUnit, opts.Metadata.Unit)
	}

	switch {
	case mt == pmetric.MetricTypeExponentialHistogram:
		h := m.SetEmptyExponentialHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	case isCumulative(name, typ):
		s := m.SetEmptySum()
		s.SetIsMonotonic(true)
		s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	default:
		m.SetEmptyGauge()
	}
	return m
}

// isCumulative reports whether the series is a cumulative monotonic counter:
// counters and the _bucket/_sum/_count series of histograms and summaries.
// Everything else (gauges, summary quantiles, info, stateset, unknown,
// gaugehistogram and report series) is a gauge.
func isCumulative(name string, typ model.MetricType) bool {
	switch typ {
	case model.MetricTypeCounter:
		return true
	case model.MetricTypeHistogram:
		return strings.HasSuffix(name, "_bucket") || strings.HasSuffix(name, "_sum") || strings.HasSuffix(name, "_count")
	case model.MetricTypeSummary:
		return strings.HasSuffix(name, "_sum") || strings.HasSuffix(name, "_count")
	default:
		return false
	}
}

// seriesHash returns a non-zero series reference: a zero ref disables
// staleness tracking in the scrape loop.
func seriesHash(ls labels.Labels) uint64 {
	if h := ls.Hash() &^ nativeHistogramRefBit; h != 0 {
		return h
	}
	return 1
}

// Commit implements storage.AppenderV2.
func (a *appender) Commit() error {
	if a.droppedCustomBuckets > 0 {
		a.parent.logger.Warn("dropped native histograms with custom buckets (NHCB), only exponential native histograms are supported",
			zap.Int("dropped", a.droppedCustomBuckets), zap.String("first_series", a.firstDropped))
	}
	if a.points == 0 {
		return nil
	}
	ctx := a.parent.obsrecv.StartMetricsOp(a.ctx)
	err := a.parent.consumer.ConsumeMetrics(ctx, a.md)
	a.parent.obsrecv.EndMetricsOp(ctx, metadata.Type.String(), a.points, err)
	return err
}

// Rollback implements storage.AppenderV2.
func (a *appender) Rollback() error {
	a.md = pmetric.NewMetrics()
	a.metrics = pmetric.NewMetricSlice()
	a.byKey = nil
	a.points = 0
	a.droppedCustomBuckets = 0
	a.firstDropped = ""
	return nil
}

var _ storage.AppenderV2 = (*appender)(nil)
