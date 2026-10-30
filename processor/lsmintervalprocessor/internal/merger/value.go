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
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/identity"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger/limits"
)

const (
	// versionV1 values hold the pmetric protobuf and the limit trackers. They
	// remain the production write format during the reader-first rollout.
	versionV1 = uint8(1)
	// versionV2 values hold one entry per resource, scope, metric, and
	// stream, each with its identity hash, its attributes as opaque bytes,
	// and, for streams, the datapoint encoded without attributes.
	versionV2 = uint8(2)

	overflowMetricName = "_overflow_metric"
	overflowMetricDesc = "Overflow metric count due to metric limit"

	overflowDatapointMetricName = "_overflow_datapoints"
	overflowDatapointMetricDesc = "Overflow datapoint count due to datapoint limit"
)

// Value defines the data structure used to perform merges and other operations
// for the underlying LSM database.
//
// A value holds the aggregated metrics as a tree of resources, scopes,
// metrics, and streams (datapoints), in insertion order, indexed by identity.
// Each entry keeps its identity hash and its attributes in encoded form, and
// streams keep their datapoint either encoded or, once merged into, decoded.
// Merging two values therefore only decodes the datapoints of the streams
// they share, and never decodes attributes; attributes and datapoints are
// turned into pmetric only by Finalize.
//
// Value also tracks overflows based on defined limits. Once the overflow limit
// is breached, the new metrics are handled as per the defined overflow
// behaviour.
//
// A value created by Unmarshal retains the unmarshaled bytes, which must not
// be modified while the value is in use.
//
// Value is not safe for concurrent use.
type Value struct {
	resourceLimitCfg               config.LimitConfig
	scopeLimitCfg                  config.LimitConfig
	metricLimitCfg                 config.LimitConfig
	datapointLimitCfg              config.LimitConfig
	maxExponentialHistogramBuckets int

	// resourceTracker is nil until the first resource is added.
	resourceTracker *limits.Tracker
	resources       []*resourceEntry
	streamCount     int

	resourceIndex map[identity.Resource]*resourceEntry
	scopeIndex    map[identity.Scope]*scopeEntry
	metricIndex   map[identity.Metric]*metricEntry
	streamIndex   map[identity.Stream]*streamEntry

	// Overflow bucket identities and attributes, built once per value.
	overflowInit       bool
	overflowResID      identity.Resource
	overflowResAttrs   []byte
	overflowScopeHash  [16]byte
	overflowScopeAttrs []byte

	scratch []byte
}

type resourceEntry struct {
	id        identity.Resource
	schemaURL string
	// attrs holds the encoded resource: attributes, dropped attribute
	// count, and entity references.
	attrs        []byte
	scopeTracker *limits.Tracker
	scopes       []*scopeEntry
}

type scopeEntry struct {
	id            identity.Scope
	schemaURL     string
	attrs         []byte
	metricTracker *limits.Tracker
	metrics       []*metricEntry
}

type metricEntry struct {
	id          identity.Metric
	description string
	dpTracker   *limits.Tracker
	streams     []*streamEntry
}

type streamEntry struct {
	attrsHash [16]byte
	attrs     []byte
	// Exactly one of payload and dp is set. dp is one of the pmetric
	// datapoint types, without attributes.
	payload []byte
	dp      any
}

// OverflowStats holds the estimated overflow counts at each level
// of the cardinality tracking hierarchy.
type OverflowStats struct {
	Resources  uint64
	Scopes     uint64
	Metrics    uint64
	Datapoints uint64
}

// NewValue creates a new instance of the value with the configured limiters.
func NewValue(
	resLimit, scopeLimit, metricLimit, datapointLimit config.LimitConfig,
	maxExponentialHistogramBuckets int,
) *Value {
	return &Value{
		resourceLimitCfg:               resLimit,
		scopeLimitCfg:                  scopeLimit,
		metricLimitCfg:                 metricLimit,
		datapointLimitCfg:              datapointLimit,
		maxExponentialHistogramBuckets: maxExponentialHistogramBuckets,
	}
}

func (v *Value) initIndex() {
	if v.resourceIndex != nil {
		return
	}
	v.resourceIndex = make(map[identity.Resource]*resourceEntry)
	v.scopeIndex = make(map[identity.Scope]*scopeEntry)
	v.metricIndex = make(map[identity.Metric]*metricEntry)
	v.streamIndex = make(map[identity.Stream]*streamEntry)
}

func (v *Value) newResourceTracker() *limits.Tracker {
	return limits.NewTracker(uint64(v.resourceLimitCfg.MaxCardinality))
}

func (v *Value) newScopeTracker() *limits.Tracker {
	return limits.NewTracker(uint64(v.scopeLimitCfg.MaxCardinality))
}

func (v *Value) newMetricTracker() *limits.Tracker {
	return limits.NewTracker(uint64(v.metricLimitCfg.MaxCardinality))
}

func (v *Value) newDatapointTracker() *limits.Tracker {
	return limits.NewTracker(uint64(v.datapointLimitCfg.MaxCardinality))
}

// AppendBinary marshals the value into its binary representation,
// and appends it to b.
func (v *Value) AppendBinary(b []byte) ([]byte, error) {
	// Reader-first release: do not persist v2 until the previous supported
	// release can read it. There is deliberately no production config bypass.
	return v.appendBinaryV1(b)
}

// appendBinaryV2 is exercised by compatibility tests; production writes v1.
func (v *Value) appendBinaryV2(b []byte) ([]byte, error) {
	b = append(b, versionV2)
	if v.streamCount == 0 {
		// Nothing to marshal
		return b, nil
	}
	var err error
	if b, err = v.resourceTracker.AppendBinary(b); err != nil {
		return nil, fmt.Errorf("failed to marshal trackers: %w", err)
	}
	b = binary.AppendUvarint(b, uint64(len(v.resources)))
	for _, re := range v.resources {
		h := re.id.AttrsHash()
		b = append(b, h[:]...)
		b = appendString(b, re.schemaURL)
		b = appendBytes(b, re.attrs)
		if b, err = re.scopeTracker.AppendBinary(b); err != nil {
			return nil, fmt.Errorf("failed to marshal trackers: %w", err)
		}
		b = binary.AppendUvarint(b, uint64(len(re.scopes)))
		for _, se := range re.scopes {
			h := se.id.AttrsHash()
			b = append(b, h[:]...)
			b = appendString(b, se.id.Name())
			b = appendString(b, se.id.Version())
			b = appendString(b, se.schemaURL)
			b = appendBytes(b, se.attrs)
			if b, err = se.metricTracker.AppendBinary(b); err != nil {
				return nil, fmt.Errorf("failed to marshal trackers: %w", err)
			}
			b = binary.AppendUvarint(b, uint64(len(se.metrics)))
			for _, me := range se.metrics {
				b = appendString(b, me.id.Name())
				b = appendString(b, me.description)
				b = appendString(b, me.id.Unit())
				var monotonic byte
				if me.id.Monotonic() {
					monotonic = 1
				}
				b = append(b, byte(me.id.Type()), byte(me.id.Temporality()), monotonic)
				if b, err = me.dpTracker.AppendBinary(b); err != nil {
					return nil, fmt.Errorf("failed to marshal trackers: %w", err)
				}
				b = binary.AppendUvarint(b, uint64(len(me.streams)))
				for _, st := range me.streams {
					b = append(b, st.attrsHash[:]...)
					b = appendBytes(b, st.attrs)
					if st.dp == nil {
						b = appendBytes(b, st.payload)
						continue
					}
					v.scratch = appendDP(v.scratch[:0], st.dp)
					b = appendBytes(b, v.scratch)
				}
			}
		}
	}
	return b, nil
}

// Unmarshal unmarshals the binary into the value struct. The value retains
// data, which must not be modified while the value is in use.
func (v *Value) Unmarshal(data []byte) error {
	if len(data) == 0 {
		return errors.New("failed to unmarshal value, invalid length")
	}
	switch data[0] {
	case versionV1:
		return v.unmarshalV1(data[1:])
	case versionV2:
		return v.unmarshalV2(data[1:])
	default:
		return fmt.Errorf("unsupported version: %d", data[0])
	}
}

func (v *Value) unmarshalV2(data []byte) error {
	v.initIndex()
	if len(data) == 0 {
		return nil
	}
	d := decoder{b: data}
	v.resourceTracker = d.tracker(v.newResourceTracker())
	nRes := d.uvarint()
	for i := uint64(0); i < nRes && d.err == nil; i++ {
		re := &resourceEntry{id: identity.ResourceFromAttrsHash(d.hash16())}
		re.schemaURL = d.string()
		re.attrs = d.bytes()
		re.scopeTracker = d.tracker(v.newScopeTracker())
		v.resources = append(v.resources, re)
		v.resourceIndex[re.id] = re

		nScopes := d.uvarint()
		for j := uint64(0); j < nScopes && d.err == nil; j++ {
			attrsHash := d.hash16()
			name, version := d.string(), d.string()
			se := &scopeEntry{id: identity.NewScope(re.id, name, version, attrsHash)}
			se.schemaURL = d.string()
			se.attrs = d.bytes()
			se.metricTracker = d.tracker(v.newMetricTracker())
			re.scopes = append(re.scopes, se)
			v.scopeIndex[se.id] = se

			nMetrics := d.uvarint()
			for k := uint64(0); k < nMetrics && d.err == nil; k++ {
				name, desc, unit := d.string(), d.string(), d.string()
				ty := pmetric.MetricType(d.byte())
				temporality := pmetric.AggregationTemporality(d.byte())
				monotonic := d.byte() == 1
				me := &metricEntry{
					id:          identity.NewMetric(se.id, name, unit, ty, monotonic, temporality),
					description: desc,
				}
				me.dpTracker = d.tracker(v.newDatapointTracker())
				se.metrics = append(se.metrics, me)
				v.metricIndex[me.id] = me

				nStreams := d.uvarint()
				for l := uint64(0); l < nStreams && d.err == nil; l++ {
					st := &streamEntry{attrsHash: d.hash16()}
					st.attrs = d.bytes()
					st.payload = d.bytes()
					me.streams = append(me.streams, st)
					v.streamIndex[identity.NewStream(me.id, st.attrsHash)] = st
					v.streamCount++
				}
			}
		}
	}
	if d.err != nil {
		return fmt.Errorf("failed to unmarshal value: %w", d.err)
	}
	return nil
}

func (d *decoder) tracker(t *limits.Tracker) *limits.Tracker {
	if d.err != nil {
		return t
	}
	n, err := t.Unmarshal(d.b)
	if err != nil {
		d.fail(fmt.Errorf("failed to unmarshal limits: %w", err))
		return t
	}
	d.b = d.b[n:]
	return t
}

// Merge merges the provided value to the current value instance.
func (v *Value) Merge(op *Value) error {
	if op == nil || op.streamCount == 0 {
		// Nothing to merge
		return nil
	}
	v.initIndex()
	for _, ore := range op.resources {
		attrs := ore.attrs
		re, err := v.addResource(ore.id, ore.schemaURL, func() []byte { return attrs })
		if err != nil {
			return fmt.Errorf("failed while merging resource metrics: %w", err)
		}
		if err := re.scopeTracker.MergeEstimators(ore.scopeTracker); err != nil {
			return fmt.Errorf("failed to merge scope overflow estimators: %w", err)
		}
		for _, ose := range ore.scopes {
			scopeID := identity.NewScope(re.id, ose.id.Name(), ose.id.Version(), ose.id.AttrsHash())
			attrs := ose.attrs
			se, err := v.addScope(re, scopeID, ose.schemaURL, func() []byte { return attrs })
			if err != nil {
				return fmt.Errorf("failed while merging scope metrics: %w", err)
			}
			if err := se.metricTracker.MergeEstimators(ose.metricTracker); err != nil {
				return fmt.Errorf("failed to merge scope datapoints overflow estimators: %w", err)
			}
			for _, ome := range ose.metrics {
				oid := ome.id
				metricID := identity.NewMetric(se.id, oid.Name(), oid.Unit(), oid.Type(), oid.Monotonic(), oid.Temporality())
				me, overflow := v.addMetric(se, metricID, ome.description)
				if overflow {
					// On metric overflow, we discard any datapoint overflow estimator
					// since metric overflow is accounts only for unique metrics.
					continue
				}
				if err := me.dpTracker.MergeEstimators(ome.dpTracker); err != nil {
					return fmt.Errorf("failed to merge datapoint overflow estimators: %w", err)
				}
				if err := v.mergeStreams(me, ome); err != nil {
					return fmt.Errorf("failed to merge metric: %w", err)
				}
			}
		}
	}
	if op.resourceTracker != nil {
		if v.resourceTracker == nil {
			v.resourceTracker = v.newResourceTracker()
		}
		if err := v.resourceTracker.MergeEstimators(op.resourceTracker); err != nil {
			return fmt.Errorf("failed to merge resource overflow estimators: %w", err)
		}
	}
	return nil
}

// MergeMetric adds a metric with a provided resource metric and scope
// metric. Note that overflows during addition will be applied as per
// the specifications for overflow handling.
func (v *Value) MergeMetric(
	otherRm pmetric.ResourceMetrics,
	otherSm pmetric.ScopeMetrics,
	otherM pmetric.Metric,
) error {
	if metricDPsCount(otherM) == 0 {
		// Nothing to merge as either there are 0 or only unsupported metrics
		return nil
	}
	v.initIndex()
	res := otherRm.Resource()
	re, err := v.addResource(identity.OfResource(res), otherRm.SchemaUrl(), func() []byte {
		return encodeResource(res)
	})
	if err != nil {
		return err
	}
	scope := otherSm.Scope()
	se, err := v.addScope(re, identity.OfScope(re.id, scope), otherSm.SchemaUrl(), func() []byte {
		return encodeAttrs(scope.Attributes(), scope.DroppedAttributesCount())
	})
	if err != nil {
		return err
	}
	me, overflow := v.addMetric(se, identity.OfMetric(se.id, otherM), otherM.Description())
	if overflow {
		return nil
	}
	return v.mergeMetricDataPoints(me, otherM)
}

// Finalize finalizes all overflows in the metrics to prepare it for
// harvest. It also returns the estimated overflow counts at each level.
// This method must be called only once for harvest.
func (v *Value) Finalize() (pmetric.Metrics, OverflowStats, error) {
	return v.finalize(true)
}

func (v *Value) finalize(includeOverflow bool) (pmetric.Metrics, OverflowStats, error) {
	md := pmetric.NewMetrics()
	var stats OverflowStats
	if v.resourceTracker != nil {
		stats.Resources = v.resourceTracker.EstimateOverflow()
	}
	for _, re := range v.resources {
		stats.Scopes += re.scopeTracker.EstimateOverflow()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.SetSchemaUrl(re.schemaURL)
		if err := decodeResourceInto(re.attrs, rm.Resource()); err != nil {
			return pmetric.Metrics{}, OverflowStats{}, fmt.Errorf("failed to decode resource: %w", err)
		}
		for _, se := range re.scopes {
			sm := rm.ScopeMetrics().AppendEmpty()
			sm.SetSchemaUrl(se.schemaURL)
			scope := sm.Scope()
			scope.SetName(se.id.Name())
			scope.SetVersion(se.id.Version())
			dropped, err := decodeAttrsInto(se.attrs, scope.Attributes())
			if err != nil {
				return pmetric.Metrics{}, OverflowStats{}, fmt.Errorf("failed to decode scope: %w", err)
			}
			scope.SetDroppedAttributesCount(dropped)
			for _, me := range se.metrics {
				if err := finalizeMetric(sm.Metrics().AppendEmpty(), me); err != nil {
					return pmetric.Metrics{}, OverflowStats{}, fmt.Errorf("failed to finalize merged metric: %w", err)
				}
			}
			if includeOverflow && se.metricTracker.HasOverflow() {
				overflowCount := se.metricTracker.EstimateOverflow()
				stats.Metrics += overflowCount
				// Add overflow metric due to metric limit breached
				if err := fillOverflowMetric(
					sm.Metrics().AppendEmpty(),
					overflowMetricName,
					overflowMetricDesc,
					overflowCount,
					v.metricLimitCfg.Overflow.Attributes,
				); err != nil {
					return pmetric.Metrics{}, OverflowStats{}, fmt.Errorf("failed to finalize merged metric: %w", err)
				}
			}
			for _, me := range se.metrics {
				if !includeOverflow || !me.dpTracker.HasOverflow() {
					continue
				}
				overflowCount := me.dpTracker.EstimateOverflow()
				stats.Datapoints += overflowCount
				// Add overflow metric due to datapoint limit breached
				if err := fillOverflowMetric(
					sm.Metrics().AppendEmpty(),
					overflowDatapointMetricName,
					overflowDatapointMetricDesc,
					overflowCount,
					v.datapointLimitCfg.Overflow.Attributes,
				); err != nil {
					return pmetric.Metrics{}, OverflowStats{}, fmt.Errorf("failed to finalize merged metric: %w", err)
				}
			}
		}
	}
	return md, stats, nil
}

func finalizeMetric(m pmetric.Metric, me *metricEntry) error {
	m.SetName(me.id.Name())
	m.SetDescription(me.description)
	m.SetUnit(me.id.Unit())
	switch me.id.Type() {
	case pmetric.MetricTypeGauge:
		m.SetEmptyGauge()
	case pmetric.MetricTypeSummary:
		dps := m.SetEmptySummary().DataPoints()
		for _, st := range me.streams {
			if err := finalizeStream(st, dps.AppendEmpty()); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeSum:
		sum := m.SetEmptySum()
		sum.SetAggregationTemporality(me.id.Temporality())
		sum.SetIsMonotonic(me.id.Monotonic())
		dps := sum.DataPoints()
		for _, st := range me.streams {
			if err := finalizeStream(st, dps.AppendEmpty()); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeHistogram:
		hist := m.SetEmptyHistogram()
		hist.SetAggregationTemporality(me.id.Temporality())
		dps := hist.DataPoints()
		for _, st := range me.streams {
			if err := finalizeStream(st, dps.AppendEmpty()); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeExponentialHistogram:
		exp := m.SetEmptyExponentialHistogram()
		exp.SetAggregationTemporality(me.id.Temporality())
		dps := exp.DataPoints()
		for _, st := range me.streams {
			if err := finalizeStream(st, dps.AppendEmpty()); err != nil {
				return err
			}
		}
	}
	return nil
}

// finalizeStream fills dst, a datapoint of the stream's type, from the
// stream entry.
func finalizeStream[DP dataPoint[DP]](st *streamEntry, dst DP) error {
	if st.dp != nil {
		st.dp.(DP).CopyTo(dst)
		dst.Attributes().Clear()
	} else if err := decodeDP(st.payload, any(dst)); err != nil {
		return err
	}
	_, err := decodeAttrsInto(st.attrs, dst.Attributes())
	return err
}

// addResource returns the entry for the resource, adding it while applying
// the resource limit. If the limit is breached, the overflow resource is
// returned. attrs is called only for a new, non-overflow resource.
func (v *Value) addResource(
	id identity.Resource,
	schemaURL string,
	attrs func() []byte,
) (*resourceEntry, error) {
	if re, ok := v.resourceIndex[id]; ok {
		return re, nil
	}
	if v.resourceTracker == nil {
		v.resourceTracker = v.newResourceTracker()
	}
	if err := v.initOverflowIdentities(); err != nil {
		return nil, err
	}
	if id == v.overflowResID || v.resourceTracker.CheckOverflow(id.Hash) {
		// Overflow, get/prepare an overflow bucket
		if re, ok := v.resourceIndex[v.overflowResID]; ok {
			return re, nil
		}
		id, attrs = v.overflowResID, func() []byte { return v.overflowResAttrs }
	}
	re := &resourceEntry{
		id:           id,
		schemaURL:    schemaURL,
		attrs:        attrs(),
		scopeTracker: v.newScopeTracker(),
	}
	v.resources = append(v.resources, re)
	v.resourceIndex[id] = re
	return re, nil
}

// addScope returns the entry for the scope of re, adding it while applying
// the scope limit. If the limit is breached, the overflow scope of re is
// returned. attrs is called only for a new, non-overflow scope.
func (v *Value) addScope(
	re *resourceEntry,
	id identity.Scope,
	schemaURL string,
	attrs func() []byte,
) (*scopeEntry, error) {
	if se, ok := v.scopeIndex[id]; ok {
		return se, nil
	}
	if err := v.initOverflowIdentities(); err != nil {
		return nil, err
	}
	overflowID := identity.NewScope(re.id, "", "", v.overflowScopeHash)
	if id == overflowID || re.scopeTracker.CheckOverflow(id.Hash) {
		// Overflow, get/prepare an overflow bucket
		if se, ok := v.scopeIndex[overflowID]; ok {
			return se, nil
		}
		id, attrs = overflowID, func() []byte { return v.overflowScopeAttrs }
	}
	se := &scopeEntry{
		id:            id,
		schemaURL:     schemaURL,
		attrs:         attrs(),
		metricTracker: v.newMetricTracker(),
	}
	re.scopes = append(re.scopes, se)
	v.scopeIndex[id] = se
	return se, nil
}

// addMetric returns the entry for the metric of se, adding it while applying
// the metric limit. The returned bool is true if the metric overflowed, in
// which case it is discarded: the overflow is recorded in the limit tracker
// and reported as a sum metric on Finalize.
func (v *Value) addMetric(
	se *scopeEntry,
	id identity.Metric,
	description string,
) (*metricEntry, bool) {
	if me, ok := v.metricIndex[id]; ok {
		return me, false
	}
	if se.metricTracker.CheckOverflow(id.Hash) {
		return nil, true
	}
	me := &metricEntry{
		id:          id,
		description: description,
		dpTracker:   v.newDatapointTracker(),
	}
	se.metrics = append(se.metrics, me)
	v.metricIndex[id] = me
	return me, false
}

// admitStream applies the datapoint limit to a new stream of me. It returns
// false if the limit is breached, in which case the stream is discarded:
// the number of unique datapoints that overflowed is recorded in the limit
// tracker and reported as a sum metric on Finalize.
func admitStream(me *metricEntry, id identity.Stream) bool {
	return !me.dpTracker.CheckOverflow(id.Hash)
}

// insertStream records a new, admitted stream of me.
func (v *Value) insertStream(me *metricEntry, id identity.Stream, st *streamEntry) {
	me.streams = append(me.streams, st)
	v.streamIndex[id] = st
	v.streamCount++
}

// mergeMetricDataPoints merges the datapoints of m, a pmetric metric, into
// the streams of me.
func (v *Value) mergeMetricDataPoints(me *metricEntry, m pmetric.Metric) error {
	switch typ := m.Type(); typ {
	case pmetric.MetricTypeSum:
		return mergeDataPoints(v, me, m.Sum().DataPoints(), m.Sum().AggregationTemporality())
	case pmetric.MetricTypeSummary:
		// Assume summary to be cumulative temporality
		return mergeDataPoints(v, me, m.Summary().DataPoints(), pmetric.AggregationTemporalityCumulative)
	case pmetric.MetricTypeHistogram:
		return mergeDataPoints(v, me, m.Histogram().DataPoints(), m.Histogram().AggregationTemporality())
	case pmetric.MetricTypeExponentialHistogram:
		return mergeDataPoints(v, me, m.ExponentialHistogram().DataPoints(), m.ExponentialHistogram().AggregationTemporality())
	default:
		return fmt.Errorf("unsupported metric type: %s", typ)
	}
}

func mergeDataPoints[DPS dataPointSlice[DP], DP dataPoint[DP]](
	v *Value,
	me *metricEntry,
	from DPS,
	temporality pmetric.AggregationTemporality,
) error {
	if err := checkTemporality(temporality); err != nil {
		return err
	}
	var errs []error
	for i := 0; i < from.Len(); i++ {
		fromDP := from.At(i)
		id := identity.OfStream(me.id, fromDP)
		st, ok := v.streamIndex[id]
		if !ok {
			if !admitStream(me, id) {
				continue
			}
			dp := newDP[DP]()
			fromDP.CopyTo(dp)
			dp.Attributes().Clear()
			v.insertStream(me, id, &streamEntry{
				attrsHash: id.AttrsHash(),
				attrs:     encodeAttrs(fromDP.Attributes(), 0),
				dp:        dp,
			})
			continue
		}
		if err := v.mergeIntoStream(st, fromDP, temporality); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to merge delta datapoints: %w", errors.Join(errs...))
	}
	return nil
}

// mergeStreams merges the streams of ome, a metric of another value, into
// the streams of me. Only the datapoints of streams present in both are
// decoded; other streams are copied in their encoded form.
func (v *Value) mergeStreams(me *metricEntry, ome *metricEntry) error {
	temporality := ome.id.Temporality()
	if ome.id.Type() == pmetric.MetricTypeSummary {
		// Assume summary to be cumulative temporality
		temporality = pmetric.AggregationTemporalityCumulative
	}
	if err := checkTemporality(temporality); err != nil {
		return err
	}
	var errs []error
	for _, ost := range ome.streams {
		id := identity.NewStream(me.id, ost.attrsHash)
		st, ok := v.streamIndex[id]
		if !ok {
			if admitStream(me, id) {
				v.insertStream(me, id, &streamEntry{
					attrsHash: ost.attrsHash,
					attrs:     ost.attrs,
					payload:   ost.payload,
					dp:        cloneDP(ost.dp),
				})
			}
			continue
		}
		if temporality == pmetric.AggregationTemporalityCumulative {
			// The newest datapoint wins; compare timestamps without
			// decoding either datapoint.
			fromTS, err := ost.timestamp()
			if err != nil {
				return err
			}
			toTS, err := st.timestamp()
			if err != nil {
				return err
			}
			wins := fromTS > toTS
			if fromTS == toTS {
				from, err := ost.decodedType(me.id.Type())
				if err != nil {
					return err
				}
				to, err := st.decodedType(me.id.Type())
				if err != nil {
					return err
				}
				wins = data.PreferCumulative(from, to)
			}
			if wins {
				st.payload, st.dp = ost.payload, cloneDP(ost.dp)
			}
			continue
		}
		from := ost.dp
		if from == nil {
			var err error
			if from, err = decodeNewDP(me.id.Type(), ost.payload); err != nil {
				return err
			}
		}
		if err := v.mergeIntoStream(st, from, temporality); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to merge delta datapoints: %w", errors.Join(errs...))
	}
	return nil
}

// mergeIntoStream merges from, a datapoint of the stream's type, into the
// stream, following the rules for its temporality.
func (v *Value) mergeIntoStream(st *streamEntry, from any, temporality pmetric.AggregationTemporality) error {
	if temporality == pmetric.AggregationTemporalityCumulative {
		// The newest datapoint wins.
		toTS, err := st.timestamp()
		if err != nil {
			return err
		}
		wins := dpTimestamp(from) > toTS
		if dpTimestamp(from) == toTS {
			to, err := st.decoded(from)
			if err != nil {
				return err
			}
			wins = data.PreferCumulative(from, to)
		}
		if wins {
			st.payload, st.dp = nil, cloneDP(from)
		}
		return nil
	}
	to, err := st.decoded(from)
	if err != nil {
		return err
	}
	err = mergeDeltaDP(from, to, v.maxExponentialHistogramBuckets)
	setDPTimestamp(to, dpTimestamp(from))
	return err
}

// decoded returns the stream's datapoint, decoding it into a new datapoint
// of the same type as like if it is encoded.
func (st *streamEntry) decoded(like any) (any, error) {
	if st.dp != nil {
		return st.dp, nil
	}
	dp := newDPLike(like)
	if err := decodeDP(st.payload, dp); err != nil {
		return nil, err
	}
	st.dp, st.payload = dp, nil
	return dp, nil
}

func (st *streamEntry) timestamp() (pcommon.Timestamp, error) {
	if st.dp != nil {
		return dpTimestamp(st.dp), nil
	}
	return payloadTimestamp(st.payload)
}

func checkTemporality(t pmetric.AggregationTemporality) error {
	switch t {
	case pmetric.AggregationTemporalityCumulative, pmetric.AggregationTemporalityDelta:
		return nil
	default:
		return fmt.Errorf("unsupported aggregation temporality: %s", t)
	}
}

func (v *Value) initOverflowIdentities() error {
	if v.overflowInit {
		return nil
	}
	r := pcommon.NewResource()
	if err := decorate(
		r.Attributes(),
		v.resourceLimitCfg.Overflow.Attributes,
	); err != nil {
		return fmt.Errorf("failed to create overflow bucket: %w", err)
	}
	scope := pcommon.NewInstrumentationScope()
	if err := decorate(
		scope.Attributes(),
		v.scopeLimitCfg.Overflow.Attributes,
	); err != nil {
		return fmt.Errorf("failed to create overflow bucket: %w", err)
	}
	v.overflowResID = identity.OfResource(r)
	v.overflowResAttrs = encodeResource(r)
	v.overflowScopeHash = identity.OfScope(v.overflowResID, scope).AttrsHash()
	v.overflowScopeAttrs = encodeAttrs(scope.Attributes(), 0)
	v.overflowInit = true
	return nil
}

func fillOverflowMetric(
	m pmetric.Metric,
	name, desc string,
	count uint64,
	extraAttrs []config.Attribute,
) error {
	m.SetName(name)
	m.SetDescription(desc)
	sum := m.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := sum.DataPoints().AppendEmpty()
	dp.SetIntValue(int64(count))
	if err := decorate(dp.Attributes(), extraAttrs); err != nil {
		return fmt.Errorf("failed to append configured attributes to overflow metric: %w", err)
	}
	return nil
}

func decorate(target pcommon.Map, src []config.Attribute) error {
	if len(src) == 0 {
		return nil
	}

	var errs []error
	target.EnsureCapacity(len(src))
	for _, attr := range src {
		v := target.PutEmpty(attr.Key)
		if err := v.FromRaw(attr.Value); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf(
			"failed to prepare resource overflow bucket: %w",
			errors.Join(errs...),
		)
	}
	return nil
}

func metricDPsCount(m pmetric.Metric) uint64 {
	switch m.Type() {
	case pmetric.MetricTypeSum:
		return uint64(m.Sum().DataPoints().Len())
	case pmetric.MetricTypeSummary:
		return uint64(m.Summary().DataPoints().Len())
	case pmetric.MetricTypeHistogram:
		return uint64(m.Histogram().DataPoints().Len())
	case pmetric.MetricTypeExponentialHistogram:
		return uint64(m.ExponentialHistogram().DataPoints().Len())
	default:
		// Includes non supported metric types
		return 0
	}
}

func (st *streamEntry) decodedType(ty pmetric.MetricType) (any, error) {
	if st.dp != nil {
		return st.dp, nil
	}
	dp, err := decodeNewDP(ty, st.payload)
	if err != nil {
		return nil, err
	}
	st.dp, st.payload = dp, nil
	return dp, nil
}
