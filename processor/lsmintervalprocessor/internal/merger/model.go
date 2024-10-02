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

package merger // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger"

import (
	"encoding/binary"
	"errors"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/identity"
)

// TODO (lahsivjar): Think about multitenancy, should be part of the key
type Key struct {
	Interval       time.Duration
	ProcessingTime time.Time
}

// NewKey creates a new instance of the merger key.
func NewKey(ivl time.Duration, pTime time.Time) Key {
	return Key{
		Interval:       ivl,
		ProcessingTime: pTime,
	}
}

// SizeBinary returns the size of the Key when binary encoded.
// The interval, represented by time.Duration, is encoded to
// 2 bytes by converting it into seconds. This allows a max of
// ~18 hours duration.
func (k *Key) SizeBinary() int {
	// 2 bytes for interval, 8 bytes for processing time
	return 10
}

// Marshal marshals the key into binary representation.
func (k *Key) Marshal() ([]byte, error) {
	ivlSeconds := uint16(k.Interval.Seconds())

	var (
		offset int
		d      [10]byte
	)
	binary.BigEndian.PutUint16(d[offset:], ivlSeconds)
	offset += 2

	binary.BigEndian.PutUint64(d[offset:], uint64(k.ProcessingTime.Unix()))

	return d[:], nil
}

// Unmarshal unmarshals the binary representation of the Key.
func (k *Key) Unmarshal(d []byte) error {
	if len(d) != 10 {
		return errors.New("failed to unmarshal key, invalid sized buffer provided")
	}
	var offset int
	k.Interval = time.Duration(binary.BigEndian.Uint16(d[offset:2])) * time.Second
	offset += 2

	k.ProcessingTime = time.Unix(int64(binary.BigEndian.Uint64(d[offset:offset+8])), 0)
	return nil
}

// Not safe for concurrent use.
type Value struct {
	Metrics pmetric.Metrics

	dynamicMapBuilt bool
	resLookup       map[identity.Resource]pmetric.ResourceMetrics
	scopeLookup     map[identity.Scope]pmetric.ScopeMetrics
	metricLookup    map[identity.Metric]pmetric.Metric
	numberLookup    map[identity.Stream]pmetric.NumberDataPoint
	summaryLookup   map[identity.Stream]pmetric.SummaryDataPoint
	histoLookup     map[identity.Stream]pmetric.HistogramDataPoint
	expHistoLookup  map[identity.Stream]pmetric.ExponentialHistogramDataPoint
}

func (v *Value) SizeBinary() int {
	// TODO (lahsivjar): Possible optimization, can take marshaler
	// as input and reuse with MarshalProto if this causes allocations.
	var marshaler pmetric.ProtoMarshaler
	return marshaler.MetricsSize(v.Metrics)
}

func (v *Value) MarshalProto() ([]byte, error) {
	var marshaler pmetric.ProtoMarshaler
	return marshaler.MarshalMetrics(v.Metrics)
}

func (v *Value) UnmarshalProto(data []byte) (err error) {
	var unmarshaler pmetric.ProtoUnmarshaler
	v.Metrics, err = unmarshaler.UnmarshalMetrics(data)
	return
}

func (v *Value) Merge(op Value) error {
	// Dynamic maps allow quick lookups to aid merging.
	// We build the map only once and maintain it while
	// merging by updating as required.
	v.buildDynamicMaps()

	rms := op.Metrics.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				v.MergeMetric(rm, sm, metrics.At(k))
			}
		}
	}
	return nil
}

func (v *Value) MergeMetric(
	rm pmetric.ResourceMetrics,
	sm pmetric.ScopeMetrics,
	m pmetric.Metric,
) {
	// Dynamic maps allow quick lookups to aid merging.
	// We build the map only once and maintain it while
	// merging by updating as required.
	v.buildDynamicMaps()

	switch m.Type() {
	case pmetric.MetricTypeSum:
		mClone, metricID := v.getOrCloneMetric(rm, sm, m)
		merge(
			m.Sum().DataPoints(),
			mClone.Sum().DataPoints(),
			metricID,
			v.numberLookup,
			m.Sum().AggregationTemporality(),
		)
	case pmetric.MetricTypeSummary:
		mClone, metricID := v.getOrCloneMetric(rm, sm, m)
		merge(
			m.Summary().DataPoints(),
			mClone.Summary().DataPoints(),
			metricID,
			v.summaryLookup,
			// Assume summary to be cumulative temporality
			pmetric.AggregationTemporalityCumulative,
		)
	case pmetric.MetricTypeHistogram:
		mClone, metricID := v.getOrCloneMetric(rm, sm, m)
		merge(
			m.Histogram().DataPoints(),
			mClone.Histogram().DataPoints(),
			metricID,
			v.histoLookup,
			m.Histogram().AggregationTemporality(),
		)
	case pmetric.MetricTypeExponentialHistogram:
		mClone, metricID := v.getOrCloneMetric(rm, sm, m)
		merge(
			m.ExponentialHistogram().DataPoints(),
			mClone.ExponentialHistogram().DataPoints(),
			metricID,
			v.expHistoLookup,
			m.ExponentialHistogram().AggregationTemporality(),
		)
	}
}

func (v *Value) buildDynamicMaps() {
	if v.dynamicMapBuilt {
		return
	}
	v.dynamicMapBuilt = true

	v.resLookup = make(map[identity.Resource]pmetric.ResourceMetrics)
	v.scopeLookup = make(map[identity.Scope]pmetric.ScopeMetrics)
	v.metricLookup = make(map[identity.Metric]pmetric.Metric)
	v.numberLookup = make(map[identity.Stream]pmetric.NumberDataPoint)
	v.summaryLookup = make(map[identity.Stream]pmetric.SummaryDataPoint)
	v.histoLookup = make(map[identity.Stream]pmetric.HistogramDataPoint)
	v.expHistoLookup = make(map[identity.Stream]pmetric.ExponentialHistogramDataPoint)

	rms := v.Metrics.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		res := identity.OfResource(rm.Resource())
		v.resLookup[res] = rm

		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			iscope := identity.OfScope(res, sm.Scope())
			v.scopeLookup[iscope] = sm

			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				imetric := identity.OfMetric(iscope, metric)

				switch metric.Type() {
				case pmetric.MetricTypeSum:
					dps := metric.Sum().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						v.numberLookup[identity.OfStream(imetric, dp)] = dp
					}
				case pmetric.MetricTypeSummary:
					dps := metric.Summary().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						v.summaryLookup[identity.OfStream(imetric, dp)] = dp
					}
				case pmetric.MetricTypeHistogram:
					dps := metric.Histogram().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						v.histoLookup[identity.OfStream(imetric, dp)] = dp
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := metric.ExponentialHistogram().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						v.expHistoLookup[identity.OfStream(imetric, dp)] = dp
					}
				}
			}
		}
	}
}

func (v *Value) getOrCloneMetric(
	rm pmetric.ResourceMetrics,
	sm pmetric.ScopeMetrics,
	m pmetric.Metric,
) (pmetric.Metric, identity.Metric) {
	// Find the ResourceMetrics
	resID := identity.OfResource(rm.Resource())
	rmClone, ok := v.resLookup[resID]
	if !ok {
		// We need to clone it *without* the ScopeMetricsSlice data
		rmClone = v.Metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().CopyTo(rmClone.Resource())
		rmClone.SetSchemaUrl(rm.SchemaUrl())
		v.resLookup[resID] = rmClone
	}

	// Find the ScopeMetrics
	scopeID := identity.OfScope(resID, sm.Scope())
	smClone, ok := v.scopeLookup[scopeID]
	if !ok {
		// We need to clone it *without* the MetricSlice data
		smClone = rmClone.ScopeMetrics().AppendEmpty()
		sm.Scope().CopyTo(smClone.Scope())
		smClone.SetSchemaUrl(sm.SchemaUrl())
		v.scopeLookup[scopeID] = smClone
	}

	// Find the Metric
	metricID := identity.OfMetric(scopeID, m)
	mClone, ok := v.metricLookup[metricID]
	if !ok {
		// We need to clone it *without* the datapoint data
		mClone = smClone.Metrics().AppendEmpty()
		mClone.SetName(m.Name())
		mClone.SetDescription(m.Description())
		mClone.SetUnit(m.Unit())

		switch m.Type() {
		case pmetric.MetricTypeGauge:
			mClone.SetEmptyGauge()
		case pmetric.MetricTypeSummary:
			mClone.SetEmptySummary()
		case pmetric.MetricTypeSum:
			src := m.Sum()

			dest := mClone.SetEmptySum()
			dest.SetAggregationTemporality(src.AggregationTemporality())
			dest.SetIsMonotonic(src.IsMonotonic())
		case pmetric.MetricTypeHistogram:
			src := m.Histogram()

			dest := mClone.SetEmptyHistogram()
			dest.SetAggregationTemporality(src.AggregationTemporality())
		case pmetric.MetricTypeExponentialHistogram:
			src := m.ExponentialHistogram()

			dest := mClone.SetEmptyExponentialHistogram()
			dest.SetAggregationTemporality(src.AggregationTemporality())
		}

		v.metricLookup[metricID] = mClone
	}

	return mClone, metricID
}

func merge[DPS DataPointSlice[DP], DP DataPoint[DP]](
	from, to DPS,
	mID identity.Metric,
	lookup map[identity.Stream]DP,
	temporality pmetric.AggregationTemporality,
) {
	switch temporality {
	case pmetric.AggregationTemporalityCumulative:
		mergeCumulative(from, to, mID, lookup)
	case pmetric.AggregationTemporalityDelta:
		mergeDelta(from, to, mID, lookup)
	}
}

func mergeCumulative[DPS DataPointSlice[DP], DP DataPoint[DP]](
	from, to DPS,
	mID identity.Metric,
	lookup map[identity.Stream]DP,
) {
	for i := 0; i < from.Len(); i++ {
		fromDP := from.At(i)

		streamID := identity.OfStream(mID, fromDP)
		toDP, ok := lookup[streamID]
		if !ok {
			toDP = to.AppendEmpty()
			fromDP.CopyTo(toDP)
			lookup[streamID] = toDP
			continue
		}

		if fromDP.Timestamp() > toDP.Timestamp() {
			fromDP.CopyTo(toDP)
		}
	}
}

func mergeDelta[DPS DataPointSlice[DP], DP DataPoint[DP]](
	from, to DPS,
	mID identity.Metric,
	lookup map[identity.Stream]DP,
) {
	for i := 0; i < from.Len(); i++ {
		fromDP := from.At(i)

		streamID := identity.OfStream(mID, fromDP)
		toDP, ok := lookup[streamID]
		if !ok {
			toDP = to.AppendEmpty()
			fromDP.CopyTo(toDP)
			lookup[streamID] = toDP
			continue
		}

		switch fromDP := any(fromDP).(type) {
		case pmetric.NumberDataPoint:
			mergeDeltaSumDP(fromDP, any(toDP).(pmetric.NumberDataPoint))
		case pmetric.HistogramDataPoint:
			mergeDeltaHistogramDP(fromDP, any(toDP).(pmetric.HistogramDataPoint))
		case pmetric.ExponentialHistogramDataPoint:
			mergeDeltaExponentialHistogramDP(fromDP, any(toDP).(pmetric.ExponentialHistogramDataPoint))
		}

		// Keep the highest timestamp for the aggregated metric
		if fromDP.Timestamp() > toDP.Timestamp() {
			toDP.SetTimestamp(fromDP.Timestamp())
		}
	}
}

func mergeDeltaSumDP(from, to pmetric.NumberDataPoint) {
	switch from.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		to.SetIntValue(to.IntValue() + from.IntValue())
	case pmetric.NumberDataPointValueTypeDouble:
		to.SetDoubleValue(to.DoubleValue() + from.DoubleValue())
	}
}

func mergeDeltaHistogramDP(from, to pmetric.HistogramDataPoint) {
	// Explicit bounds histogram should have same pre-defined buckets.
	// However, it is possible that the boundaries got updated. In such
	// scenarios we can't calculate the histogram for conflicting
	// boundaries without assuming the distribution of the bucket. In
	// practical situations, we should not see such cases because if the
	// service restarts to apply the new boundaries then some of the
	// resource attributes will change which will change the identification
	// for the metric, however, it is possible to observe such cases if the
	// service has multiple replicas and we are aggregating the replicas.
	// A rolling update with a change in the histogram definition will
	// trigger this situation.
	//
	// Here we protect our code by checking the size of the counts slices.
	// TODO (lahsivjar): merge histograms with conflicting boundaries by
	// assuming the distribution of the bucket.
	fromCounts := from.BucketCounts()
	toCounts := to.BucketCounts()
	if fromCounts.Len() != toCounts.Len() {
		return
	}
	for i := 0; i < toCounts.Len(); i++ {
		toCounts.SetAt(i, fromCounts.At(i)+toCounts.At(i))
	}
}

func mergeDeltaExponentialHistogramDP(from, to pmetric.ExponentialHistogramDataPoint) {
	if from.Count() == 0 {
		return
	}
	if to.Count() == 0 {
		from.CopyTo(to)
		return
	}

	// Since min/max are optional, set them only when both have it, else reset it
	if to.HasMin() && from.HasMin() {
		to.SetMin(min(to.Min(), from.Min()))
	} else {
		to.RemoveMin()
	}
	if to.HasMax() && from.HasMax() {
		to.SetMax(max(to.Max(), from.Max()))
	} else {
		to.RemoveMax()
	}
	to.SetSum(to.Sum() + from.Sum())
	to.SetCount(to.Count() + from.Count())
	// TODO (lahsivjar): Zero count depends on the value of zero threshold.
	// If zero threshold are not the same then we can't merge them directly,
	// instead, we will need to expand the zero threshold to be inclusive
	// of the smaller zero count. If a zero threshold encroaches a bucket,
	// then we will need to expand the zero count till the end of the bucket.
	to.SetZeroCount(to.ZeroCount() + from.ZeroCount())

	// Downscale the resolution and use perfect-subsetting
	minScale := min(to.Scale(), from.Scale())
	maybeScaleDownHistogram(to, minScale)
	maybeScaleDownHistogram(from, minScale)

	// All histogram buckets are now on same scale, merge them
	mergeExponentialHistogramBuckets(from.Positive(), to.Positive())
	mergeExponentialHistogramBuckets(from.Negative(), to.Negative())
}

func mergeExponentialHistogramBuckets(from, to pmetric.ExponentialHistogramDataPointBuckets) {
	fromBuckets := from.BucketCounts()
	toBuckets := to.BucketCounts()
	fromSize := int(from.Offset()) + fromBuckets.Len()
	toSize := int(to.Offset()) + toBuckets.Len()

	if to.Offset() > from.Offset() {
		// Expand `to` to accomodate the merged buckets with offset as `from.Offset()`
		newBucketsSize := max(fromSize, toSize) - int(from.Offset())
		newBuckets := pcommon.NewUInt64Slice()
		newBuckets.Append(make([]uint64, newBucketsSize)...)

		// Copy `to` to the new bucket
		for i := 0; i < toBuckets.Len(); i++ {
			newBuckets.SetAt(int(to.Offset())+i, toBuckets.At(i))
		}
		newBuckets.MoveTo(to.BucketCounts())
		to.SetOffset(from.Offset())

		toBuckets = to.BucketCounts()
	} else if fromSize > toSize {
		toBuckets.Append(make([]uint64, fromSize-toSize)...)
	}
	for i := 0; i < fromBuckets.Len(); i++ {
		toOffset := int(from.Offset()-to.Offset()) + i
		toBuckets.SetAt(toOffset, toBuckets.At(toOffset)+fromBuckets.At(i))
	}
}

// maybeScaleDownHistogram attempts to scale down histgram if the proper scale is passed.
func maybeScaleDownHistogram(hist pmetric.ExponentialHistogramDataPoint, newScale int32) {
	diff := hist.Scale() - newScale
	switch {
	case diff < 0:
		panic("unexpected state, histogram cannot be upscaled")
	case diff == 0:
		return
	}

	scaleDownHistogramBuckets(hist.Positive(), diff)
	scaleDownHistogramBuckets(hist.Negative(), diff)
}

// scaleDownHistogramBuckets scales down the histogram buckets by collapsing 2^(scaleDiff)
// number of buckets into one bucket while handling the offset as required.
// TODO (lahsivjar): Currently we don't trim the trailing zero entries from the buckets.
// This is because OTel doesn't expose the data-model to efficiently slice the buckets,
// instead, trimming trailing zero entries will require reallocation of the whole slice.
// Is it worth it? (See test cases)
func scaleDownHistogramBuckets(hist pmetric.ExponentialHistogramDataPointBuckets, scaleDiff int32) {
	if scaleDiff == 0 {
		return
	}

	// collapse is the number of buckets to collapse into lower scale bucket
	// per higher scale bucket for downscaling
	collapse := int64(1) << scaleDiff
	buckets := hist.BucketCounts()

	// higher represents the index position in the buckets count slice for higher
	// scale histogram and lower represents the same for lower scale histogram
	var higher, lower int
	for higher < buckets.Len() {
		// count represents number of higher buckets that will be collpased
		// depending on the current index (may differ from `collapse` due to offset)
		count := (int64(hist.Offset()) + int64(higher)) % collapse
		for i := int64(count); i < collapse && higher < buckets.Len(); i++ {
			if lower != higher {
				buckets.SetAt(lower, buckets.At(higher)+buckets.At(lower))
				buckets.SetAt(higher, 0)
			}
			higher++
		}
		lower++
	}
	hist.SetOffset(hist.Offset() >> scaleDiff)
}
