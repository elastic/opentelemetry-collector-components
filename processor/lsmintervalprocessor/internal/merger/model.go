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

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/identity"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger/limits"
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
	store *limits.Store
}

func NewValue(cfg *config.Config) Value {
	return Value{
		store: limits.NewStore(cfg),
	}
}

func (v *Value) Get() pmetric.Metrics {
	return v.store.Get()
}

func (v *Value) Finalize() (pmetric.Metrics, error) {
	return v.store.Finalize()
}

func (v *Value) MarshalProto() ([]byte, error) {
	return v.store.MarshalProto()
}

func (v *Value) UnmarshalProto(data []byte) error {
	return v.store.UnmarshalProto(data)
}

func (v *Value) Merge(op Value) error {
	rms := op.Get().ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		resID := v.store.AddResourceMetrics(rm)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			scopeID := v.store.AddScopeMetrics(resID, sm)
			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				v.mergeMetric(resID, scopeID, metrics.At(k))
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
	resID := v.store.AddResourceMetrics(rm)
	scopeID := v.store.AddScopeMetrics(resID, sm)
	v.mergeMetric(resID, scopeID, m)
}

func (v *Value) mergeMetric(
	resID identity.Resource,
	scopeID identity.Scope,
	m pmetric.Metric,
) {
	metricID := v.store.AddMetric(scopeID, m)

	switch m.Type() {
	case pmetric.MetricTypeSum:
		merge(
			m.Sum().DataPoints(),
			metricID,
			v.store.AddSumDataPoint,
			m.Sum().AggregationTemporality(),
		)
	case pmetric.MetricTypeSummary:
		merge(
			m.Summary().DataPoints(),
			metricID,
			v.store.AddSummaryDataPoint,
			// Assume summary to be cumulative temporality
			pmetric.AggregationTemporalityCumulative,
		)
	case pmetric.MetricTypeHistogram:
		merge(
			m.Histogram().DataPoints(),
			metricID,
			v.store.AddHistogramDataPoint,
			m.Histogram().AggregationTemporality(),
		)
	case pmetric.MetricTypeExponentialHistogram:
		merge(
			m.ExponentialHistogram().DataPoints(),
			metricID,
			v.store.AddExponentialHistogramDataPoint,
			m.ExponentialHistogram().AggregationTemporality(),
		)
	}
}

func merge[DPS DataPointSlice[DP], DP DataPoint[DP]](
	from DPS,
	toMetricID identity.Metric,
	addDP func(identity.Metric, DP) (DP, bool),
	temporality pmetric.AggregationTemporality,
) {
	switch temporality {
	case pmetric.AggregationTemporalityCumulative:
		mergeCumulative(from, toMetricID, addDP)
	case pmetric.AggregationTemporalityDelta:
		mergeDelta(from, toMetricID, addDP)
	}
}

func mergeCumulative[DPS DataPointSlice[DP], DP DataPoint[DP]](
	from DPS,
	toMetricID identity.Metric,
	addDP func(identity.Metric, DP) (DP, bool),
) {
	var zero DP
	for i := 0; i < from.Len(); i++ {
		fromDP := from.At(i)
		toDP, ok := addDP(toMetricID, fromDP)
		if toDP == zero {
			// Overflow, discard the datapoint
			continue
		}
		if ok {
			// New data point is created so we can copy the old data directly
			fromDP.CopyTo(toDP)
		}
		if fromDP.Timestamp() > toDP.Timestamp() {
			fromDP.CopyTo(toDP)
		}
	}
}

func mergeDelta[DPS DataPointSlice[DP], DP DataPoint[DP]](
	from DPS,
	toMetricID identity.Metric,
	addDP func(identity.Metric, DP) (DP, bool),
) {
	var zero DP
	for i := 0; i < from.Len(); i++ {
		fromDP := from.At(i)
		toDP, ok := addDP(toMetricID, fromDP)
		if toDP == zero {
			// Overflow, discard the datapoint
			continue
		}
		if ok {
			// New data point is created so we can copy the old data directly
			fromDP.CopyTo(toDP)
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
	}
}

func mergeDeltaSumDP(from, to pmetric.NumberDataPoint) {
	toDP := data.Number{NumberDataPoint: to}
	fromDP := data.Number{NumberDataPoint: from}

	toDP.Add(fromDP)
}

func mergeDeltaHistogramDP(from, to pmetric.HistogramDataPoint) {
	if from.Count() == 0 {
		return
	}
	if to.Count() == 0 {
		from.CopyTo(to)
		return
	}

	toDP := data.Histogram{HistogramDataPoint: to}
	fromDP := data.Histogram{HistogramDataPoint: from}

	toDP.Add(fromDP)
}

func mergeDeltaExponentialHistogramDP(from, to pmetric.ExponentialHistogramDataPoint) {
	if from.Count() == 0 {
		return
	}
	if to.Count() == 0 {
		from.CopyTo(to)
		return
	}

	toDP := data.ExpHistogram{DataPoint: to}
	fromDP := data.ExpHistogram{DataPoint: from}

	toDP.Add(fromDP)
}
