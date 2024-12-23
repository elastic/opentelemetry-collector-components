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
	overflowMetricName = "_other"
	overflowMetricDesc = "Overflow count due to datapoints limit"
)

// Value defines the data structure used to perform merges and other operations
// for the underlying LSM database. The basic representation of the Value is
// based on pmetric datastructure. To aid in merging and operations, the
// pmetric datastructure is expanded into multiple lookup maps as required.
//
// Value also tracks overflows based on defined limits. Once the overflow limit
// is breached, the new metrics are handled as per the defined overflow
// behaviour. The Value can be in two states:
//
// 1) Unexpanded state, in this state the lookup maps are not created. The
// limit trackers, if present, are encoded into a separate field. This state
// is immutable i.e. the pmetric structure and the metrics cannot be modified
// in this state.
//
// 2) Expanded state, in this state lookup maps are created and the limit
// trackers are decoded and put together with their corresponding pmetric
// datastructure. The value is automatically upgraded to expanded state
// when a merge operation is performed.
//
// Value is not safe for concurrent use.
type Value struct {
	resourceLimitCfg config.LimitConfig
	scopeLimitCfg    config.LimitConfig
	scopeDPLimitCfg  config.LimitConfig

	source   pmetric.Metrics
	trackers *limits.Trackers

	// Lookup tables created from source
	lookupsInitialized bool
	resLookup          map[identity.Resource]resourceMetrics
	scopeLookup        map[identity.Scope]scopeMetrics
	metricLookup       map[identity.Metric]metric
	numberLookup       map[identity.Stream]numberDataPoint
	summaryLookup      map[identity.Stream]summaryDataPoint
	histoLookup        map[identity.Stream]histogramDataPoint
	expHistoLookup     map[identity.Stream]exponentialHistogramDataPoint
}

type resourceMetrics struct {
	pmetric.ResourceMetrics

	// Keeps track of scope overflows within each resource metric
	scopeLimits *limits.Tracker
}

type scopeMetrics struct {
	pmetric.ScopeMetrics

	// Keeps track of datapoints limits within each scope metric
	datapointsLimits *limits.Tracker
}

type metric = pmetric.Metric

type numberDataPoint = pmetric.NumberDataPoint

type summaryDataPoint = pmetric.SummaryDataPoint

type histogramDataPoint = pmetric.HistogramDataPoint

type exponentialHistogramDataPoint = pmetric.ExponentialHistogramDataPoint

// NewValue creates a new instance of the value with the configured limiters.
func NewValue(resLimit, scopeLimit, scopeDPLimit config.LimitConfig) *Value {
	return &Value{
		resourceLimitCfg: resLimit,
		scopeLimitCfg:    scopeLimit,
		scopeDPLimitCfg:  scopeDPLimit,
		source:           pmetric.NewMetrics(),
	}
}

// Marshal marshals the value into binary. Limit trackers and pmetric are marshaled
// into the same binary representation.
func (s *Value) Marshal() ([]byte, error) {
	if s.source.DataPointCount() == 0 {
		// Nothing to marshal
		return nil, nil
	}
	tb, err := s.trackers.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metric: %w", err)
	}
	var marshaler pmetric.ProtoMarshaler
	pmb, err := marshaler.MarshalMetrics(s.source)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metric: %w", err)
	}
	b := make([]byte, 8+len(tb)+len(pmb))
	binary.BigEndian.PutUint64(b, uint64(len(tb)))
	offset := 8
	offset += copy(b[offset:], tb)
	copy(b[offset:], pmb)
	return b, nil
}

// Unmarshal unmarshals the binary into the value struct. The value consists
// of the pmetric data structure and a set of limits tracking the overflows
// for each of the pmetric children (resource, scope, and datapoints). The
// limits are marshaled and encoded separately from the pmetric datastructure.
func (s *Value) Unmarshal(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if len(data) < 8 {
		// For non-nil value, tracker must be marshaled
		return errors.New("failed to unmarshal value, invalid length")
	}
	trackersLen := int(binary.BigEndian.Uint64(data[:8]))
	data = data[8:]
	if trackersLen > 0 {
		// Unmarshal trackers
		s.trackers = limits.NewTrackers(
			uint64(s.resourceLimitCfg.MaxCardinality),
			uint64(s.scopeLimitCfg.MaxCardinality),
			uint64(s.scopeDPLimitCfg.MaxCardinality),
		)
		err := s.trackers.Unmarshal(data[:trackersLen])
		if err != nil {
			return fmt.Errorf("failed to unmarshal limits: %w", err)
		}
		data = data[trackersLen:]
	}
	// Unmarshal pmetric.Metrics
	var unmarshaler pmetric.ProtoUnmarshaler
	var err error
	s.source, err = unmarshaler.UnmarshalMetrics(data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}
	return nil
}

// Merge merges the provided value to the current value instance.
func (v *Value) Merge(op *Value) error {
	if op == nil || op.source.DataPointCount() == 0 {
		// Nothing to merge
		return nil
	}
	// Initialize the destination lookup table
	v.initLookupTables()
	// Iterate over the source's pmetric structure and merge into destination
	rmsOther := op.source.ResourceMetrics()
	for i := 0; i < rmsOther.Len(); i++ {
		rmOther := rmsOther.At(i)
		resID, err := v.addResourceMetrics(rmOther)
		if err != nil {
			return fmt.Errorf("failed while merging resource metrics: %w", err)
		}
		scopeLimits := op.trackers.GetScopeTracker(i)
		if scopeLimits != nil {
			if err := v.resLookup[resID].scopeLimits.MergeEstimators(scopeLimits); err != nil {
				return fmt.Errorf("failed to merge scope overflow estimators: %w", err)
			}
		}
		smsOther := rmOther.ScopeMetrics()
		for j := 0; j < smsOther.Len(); j++ {
			smOther := smsOther.At(j)
			scopeID, err := v.addScopeMetrics(resID, smOther)
			if err != nil {
				return fmt.Errorf("failed while merging scope metrics: %w", err)
			}
			scopeDPsLimits := op.trackers.GetScopeDPsTracker(j)
			if scopeDPsLimits != nil {
				if err := v.scopeLookup[scopeID].datapointsLimits.MergeEstimators(scopeDPsLimits); err != nil {
					return fmt.Errorf("failed to merge scope datapoints overflow estimators: %w", err)
				}
			}
			msOther := smOther.Metrics()
			for k := 0; k < msOther.Len(); k++ {
				v.mergeMetric(resID, scopeID, msOther.At(k))
			}
		}
	}
	if op.trackers != nil {
		if err := v.trackers.GetResourceTracker().MergeEstimators(
			op.trackers.GetResourceTracker(),
		); err != nil {
			return fmt.Errorf("failed to merge resource overflow estimators: %w", err)
		}
	}
	return nil
}

// MergeMetric adds a metric with a provided resource metric and scope
// metric. Note that overflows during addition will be applied as per
// the specifications for overflow handling.
func (v *Value) MergeMetric(
	rm pmetric.ResourceMetrics,
	sm pmetric.ScopeMetrics,
	m pmetric.Metric,
) error {
	if metricDPsCount(m) == 0 {
		// Nothing to merge as either there are 0 or only unsupported metrics
		return nil
	}
	v.initLookupTables()
	// TODO: Precheck the metric for datapoints existence, if none exists
	// then don't add resource/scope metrics. This will help to remove the
	// remove if checks from the finalize method.
	// OR
	// Do this check in the prior call where we know the metric type.
	resID, err := v.addResourceMetrics(rm)
	if err != nil {
		return err
	}
	scopeID, err := v.addScopeMetrics(resID, sm)
	if err != nil {
		return err
	}
	v.mergeMetric(resID, scopeID, m)
	return nil
}

// Finalize finalizes all overflows in the metrics to prepare it for
// harvest. This method must be called only once for harvest.
func (s *Value) Finalize() (pmetric.Metrics, error) {
	// At this point we need to assume that the metrics are returned
	// as a final step in the store, thus, prepare the final metric.
	// In the final metric we have to add datapoint limits. Also, we
	// need to ensure that lookup tables, and thus limits, are
	// initialized.
	s.initLookupTables()
	for _, sm := range s.scopeLookup {
		if !sm.datapointsLimits.HasOverflow() {
			continue
		}
		// Add overflow metric to the scope
		overflowMetric := sm.ScopeMetrics.Metrics().AppendEmpty()
		overflowMetric.SetName(overflowMetricName)
		overflowMetric.SetDescription(overflowMetricDesc)
		overflowSum := overflowMetric.SetEmptySum()
		overflowSum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		overflowDP := overflowSum.DataPoints().AppendEmpty()
		if err := decorate(
			overflowDP.Attributes(),
			s.scopeDPLimitCfg.Overflow.Attributes,
		); err != nil {
			return pmetric.Metrics{}, fmt.Errorf("failed to finalize merged metric: %w", err)
		}
		overflowDP.SetIntValue(int64(sm.datapointsLimits.EstimateOverflow()))
	}
	// Remove any hanging resource or scope which failed to have any entries
	// due to children reaching their limits. Note that after this the trackers
	// will not be associated with the pmetric, but, we don't them anyway.
	// TODO (lahsivjar): We can probably optimize to not require this loop by
	// adding to source metric only at finalize.
	s.source.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					return m.Gauge().DataPoints().Len() == 0
				case pmetric.MetricTypeSum:
					return m.Sum().DataPoints().Len() == 0
				case pmetric.MetricTypeHistogram:
					return m.Histogram().DataPoints().Len() == 0
				case pmetric.MetricTypeExponentialHistogram:
					return m.ExponentialHistogram().DataPoints().Len() == 0
				case pmetric.MetricTypeSummary:
					return m.Summary().DataPoints().Len() == 0
				}
				return false
			})
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})
	return s.source, nil
}

func (s *Value) initLookupTables() {
	if s.lookupsInitialized {
		return
	}
	s.lookupsInitialized = true

	// If lookup tables are initialized we will need the lookup maps
	s.resLookup = make(map[identity.Resource]resourceMetrics)
	s.scopeLookup = make(map[identity.Scope]scopeMetrics)
	s.metricLookup = make(map[identity.Metric]metric)
	s.numberLookup = make(map[identity.Stream]numberDataPoint)
	s.summaryLookup = make(map[identity.Stream]summaryDataPoint)
	s.histoLookup = make(map[identity.Stream]histogramDataPoint)
	s.expHistoLookup = make(map[identity.Stream]exponentialHistogramDataPoint)

	rms := s.source.ResourceMetrics()
	if rms.Len() == 0 {
		// Nothing to merge
		return
	}

	// Initialize the lookup tables assuming that the limits were respected
	// for the marshaled data and no unexpected overflow will happen.
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		rmID := identity.OfResource(rm.Resource())
		s.resLookup[rmID] = resourceMetrics{
			ResourceMetrics: rm,
			scopeLimits:     s.trackers.GetScopeTracker(i),
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			scope := sm.Scope()
			smID := identity.OfScope(rmID, scope)
			s.scopeLookup[smID] = scopeMetrics{
				ScopeMetrics:     sm,
				datapointsLimits: s.trackers.GetScopeDPsTracker(j),
			}
			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				metricID := identity.OfMetric(smID, metric)
				s.metricLookup[metricID] = metric

				//exhaustive:enforce
				switch metric.Type() {
				case pmetric.MetricTypeEmpty:
					continue
				case pmetric.MetricTypeGauge:
					// TODO (lahsivjar): implement gauge support
				case pmetric.MetricTypeSum:
					dps := metric.Sum().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						streamID := identity.OfStream(metricID, dp)
						s.numberLookup[streamID] = dp
					}
				case pmetric.MetricTypeSummary:
					dps := metric.Summary().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						streamID := identity.OfStream(metricID, dp)
						s.summaryLookup[streamID] = dp
					}
				case pmetric.MetricTypeHistogram:
					dps := metric.Histogram().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						streamID := identity.OfStream(metricID, dp)
						s.histoLookup[streamID] = dp
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := metric.ExponentialHistogram().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						streamID := identity.OfStream(metricID, dp)
						s.expHistoLookup[streamID] = dp
					}
				}
			}
		}
	}
}

// addResourceMetrics adds a new resource metrics to the store while also
// applying resource limiters. If a limit is configured and breached by
// adding the provided resource metric, then, a new overflow resource
// metric is created and returned.
func (s *Value) addResourceMetrics(
	otherRm pmetric.ResourceMetrics,
) (identity.Resource, error) {
	resID := identity.OfResource(otherRm.Resource())
	if _, ok := s.resLookup[resID]; ok {
		return resID, nil
	}
	if s.trackers == nil {
		s.trackers = limits.NewTrackers(
			uint64(s.resourceLimitCfg.MaxCardinality),
			uint64(s.scopeLimitCfg.MaxCardinality),
			uint64(s.scopeDPLimitCfg.MaxCardinality),
		)
	}
	if s.trackers.GetResourceTracker().CheckOverflow(resID.Hash().Sum64()) {
		// Overflow, get/prepare an overflow bucket
		overflowResID, err := s.getOverflowResourceBucketID()
		if err != nil {
			return identity.Resource{}, err
		}
		if _, ok := s.resLookup[overflowResID]; !ok {
			overflowRm := s.source.ResourceMetrics().AppendEmpty()
			if err := decorate(
				overflowRm.Resource().Attributes(),
				s.resourceLimitCfg.Overflow.Attributes,
			); err != nil {
				return identity.Resource{}, err
			}
			s.resLookup[overflowResID] = resourceMetrics{
				ResourceMetrics: overflowRm,
				scopeLimits:     s.trackers.NewScopeTracker(),
			}
		}
		return overflowResID, nil
	}
	// Clone it *without* the ScopeMetricsSlice data
	rm := s.source.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl(otherRm.SchemaUrl())
	otherRm.Resource().CopyTo(rm.Resource())
	s.resLookup[resID] = resourceMetrics{
		ResourceMetrics: rm,
		scopeLimits:     s.trackers.NewScopeTracker(),
	}
	return resID, nil
}

// addScopeMetrics adds a new scope metrics to the store while also
// applying scope limiters. If a limit is configured and breached by
// adding the provided scope metric, then, a new overflow scope
// metric is created and returned.
func (s *Value) addScopeMetrics(
	resID identity.Resource,
	otherSm pmetric.ScopeMetrics,
) (identity.Scope, error) {
	scopeID := identity.OfScope(resID, otherSm.Scope())
	if _, ok := s.scopeLookup[scopeID]; ok {
		return scopeID, nil
	}
	res := s.resLookup[resID]
	if res.scopeLimits.CheckOverflow(scopeID.Hash().Sum64()) {
		// Overflow, get/prepare an overflow bucket
		overflowScopeID, err := s.getOverflowScopeBucketID(resID)
		if err != nil {
			return identity.Scope{}, err
		}
		if _, ok := s.scopeLookup[overflowScopeID]; !ok {
			overflowScope := res.ScopeMetrics().AppendEmpty()
			if err := decorate(
				overflowScope.Scope().Attributes(),
				s.scopeLimitCfg.Overflow.Attributes,
			); err != nil {
				return identity.Scope{}, err
			}
			s.scopeLookup[overflowScopeID] = scopeMetrics{
				ScopeMetrics:     overflowScope,
				datapointsLimits: s.trackers.NewScopeDPsTracker(),
			}
		}
		return overflowScopeID, nil
	}
	// Clone it *without* the MetricSlice data
	sm := res.ScopeMetrics().AppendEmpty()
	otherSm.Scope().CopyTo(sm.Scope())
	sm.SetSchemaUrl(otherSm.SchemaUrl())
	s.scopeLookup[scopeID] = scopeMetrics{
		ScopeMetrics:     sm,
		datapointsLimits: s.trackers.NewScopeDPsTracker(),
	}
	return scopeID, nil
}

// addMetric adds the given metric to the store while also considering
// datapoint limiters. If a limit is configured and breached by adding a new
// metric then the datapoint overflow is updated and the metric is discarded
// as when datapoint overflows, a new metric overflow sum metric is added
// with delta temporality tracking the cardinality estimate of the overflow.
func (s *Value) addMetric(
	scopeID identity.Scope,
	otherMetric pmetric.Metric,
) identity.Metric {
	metricID := identity.OfMetric(scopeID, otherMetric)
	if _, ok := s.metricLookup[metricID]; ok {
		return metricID
	}
	scope := s.scopeLookup[scopeID]

	// Metrics doesn't have overflows (only datapoints have)
	// Clone it *without* the datapoint data
	m := scope.Metrics().AppendEmpty()
	m.SetName(otherMetric.Name())
	m.SetDescription(otherMetric.Description())
	m.SetUnit(otherMetric.Unit())
	switch otherMetric.Type() {
	case pmetric.MetricTypeGauge:
		m.SetEmptyGauge()
	case pmetric.MetricTypeSummary:
		m.SetEmptySummary()
	case pmetric.MetricTypeSum:
		otherSum := otherMetric.Sum()

		sum := m.SetEmptySum()
		sum.SetAggregationTemporality(otherSum.AggregationTemporality())
		sum.SetIsMonotonic(otherSum.IsMonotonic())
	case pmetric.MetricTypeHistogram:
		otherHist := otherMetric.Histogram()

		hist := m.SetEmptyHistogram()
		hist.SetAggregationTemporality(otherHist.AggregationTemporality())
	case pmetric.MetricTypeExponentialHistogram:
		otherExp := otherMetric.ExponentialHistogram()

		exp := m.SetEmptyExponentialHistogram()
		exp.SetAggregationTemporality(otherExp.AggregationTemporality())
	}
	s.metricLookup[metricID] = m
	return metricID
}

// addSumDataPoint returns a data point entry in the store for the given metric
// and the external data point if it is present. If the data point is not
// present then either a new data point is added or if the data point overflows
// due to configured limit then an empty data point is returned. The returned
// bool value is `true` if a new data point is created and `false` otherwise.
func (s *Value) addSumDataPoint(
	metricID identity.Metric,
	otherDP pmetric.NumberDataPoint,
) (pmetric.NumberDataPoint, bool) {
	streamID := identity.OfStream(metricID, otherDP)
	if dp, ok := s.numberLookup[streamID]; ok {
		return dp, false
	}
	sm := s.scopeLookup[metricID.Scope()]
	metric := s.metricLookup[metricID]
	if sm.datapointsLimits.CheckOverflow(metricID.Hash().Sum64()) {
		// Datapoints overflow detected. In this case no action has to be
		// done at this point since data point overflow should create a new
		// overflow metric of sum type recording the number of unique
		// datapoints. This number will be recorded in the limit tracker
		// and the metric will be populated on demand.
		return pmetric.NumberDataPoint{}, false
	}
	dp := metric.Sum().DataPoints().AppendEmpty()
	s.numberLookup[streamID] = dp
	return dp, true
}

// addSummaryDataPoint returns a data point entry in the store for the given
// metric and the external data point if it is present. If the data point is
// not present then either a new data point is added or if the data point
// overflows due to configured limit then an empty data point is returned.
// The returned bool value is `true` if a new data point is created and
// `false` otherwise.
func (s *Value) addSummaryDataPoint(
	metricID identity.Metric,
	otherDP pmetric.SummaryDataPoint,
) (pmetric.SummaryDataPoint, bool) {
	streamID := identity.OfStream(metricID, otherDP)
	if dp, ok := s.summaryLookup[streamID]; ok {
		return dp, false
	}
	sm := s.scopeLookup[metricID.Scope()]
	metric := s.metricLookup[metricID]
	if sm.datapointsLimits.CheckOverflow(metricID.Hash().Sum64()) {
		// Datapoints overflow detected. In this case no action has to be
		// done at this point since data point overflow should create a new
		// overflow metric of sum type recording the number of unique
		// datapoints. This number will be recorded in the limit tracker
		// and the metric will be populated on demand.
		return pmetric.SummaryDataPoint{}, false
	}
	dp := metric.Summary().DataPoints().AppendEmpty()
	s.summaryLookup[streamID] = dp
	return dp, true
}

// addHistogramDataPoint returns a data point entry in the store for the given
// metric and the external data point if it is present. If the data point is
// not present then either a new data point is added or if the data point
// overflows due to configured limit then an empty data point is returned.
// The returned bool value is `true` if a new data point is created and
// `false` otherwise.
func (s *Value) addHistogramDataPoint(
	metricID identity.Metric,
	otherDP pmetric.HistogramDataPoint,
) (pmetric.HistogramDataPoint, bool) {
	streamID := identity.OfStream(metricID, otherDP)
	if dp, ok := s.histoLookup[streamID]; ok {
		return dp, false
	}
	sm := s.scopeLookup[metricID.Scope()]
	metric := s.metricLookup[metricID]
	if sm.datapointsLimits.CheckOverflow(metricID.Hash().Sum64()) {
		// Datapoints overflow detected. In this case no action has to be
		// done at this point since data point overflow should create a new
		// overflow metric of sum type recording the number of unique
		// datapoints. This number will be recorded in the limit tracker
		// and the metric will be populated on demand.
		return pmetric.HistogramDataPoint{}, false
	}
	dp := metric.Histogram().DataPoints().AppendEmpty()
	s.histoLookup[streamID] = dp
	return dp, true
}

// addExponentialHistogramDataPoint returns a data point entry in the store
// for the given metric and the external data point if it is present. If the
// data point is not present then either a new data point is added or if the
// data point overflows due to configured limit then an empty data point is
// returned. The returned bool value is `true` if a new data point is created
// and `false` otherwise.
func (s *Value) addExponentialHistogramDataPoint(
	metricID identity.Metric,
	otherDP pmetric.ExponentialHistogramDataPoint,
) (pmetric.ExponentialHistogramDataPoint, bool) {
	streamID := identity.OfStream(metricID, otherDP)
	if dp, ok := s.expHistoLookup[streamID]; ok {
		return dp, false
	}
	sm := s.scopeLookup[metricID.Scope()]
	metric := s.metricLookup[metricID]
	if sm.datapointsLimits.CheckOverflow(metricID.Hash().Sum64()) {
		// Datapoints overflow detected. In this case no action has to be
		// done at this point since data point overflow should create a new
		// overflow metric of sum type recording the number of unique
		// datapoints. This number will be recorded in the limit tracker
		// and the metric will be populated on demand.
		return pmetric.ExponentialHistogramDataPoint{}, false
	}
	dp := metric.ExponentialHistogram().DataPoints().AppendEmpty()
	s.expHistoLookup[streamID] = dp
	return dp, true
}

func (v *Value) mergeMetric(
	resID identity.Resource,
	scopeID identity.Scope,
	m metric,
) {
	metricID := v.addMetric(scopeID, m)

	switch m.Type() {
	case pmetric.MetricTypeSum:
		merge(
			m.Sum().DataPoints(),
			metricID,
			v.addSumDataPoint,
			m.Sum().AggregationTemporality(),
		)
	case pmetric.MetricTypeSummary:
		merge(
			m.Summary().DataPoints(),
			metricID,
			v.addSummaryDataPoint,
			// Assume summary to be cumulative temporality
			pmetric.AggregationTemporalityCumulative,
		)
	case pmetric.MetricTypeHistogram:
		merge(
			m.Histogram().DataPoints(),
			metricID,
			v.addHistogramDataPoint,
			m.Histogram().AggregationTemporality(),
		)
	case pmetric.MetricTypeExponentialHistogram:
		merge(
			m.ExponentialHistogram().DataPoints(),
			metricID,
			v.addExponentialHistogramDataPoint,
			m.ExponentialHistogram().AggregationTemporality(),
		)
	}
}

func (s *Value) getOverflowResourceBucketID() (identity.Resource, error) {
	r := pcommon.NewResource()
	if err := decorate(
		r.Attributes(),
		s.resourceLimitCfg.Overflow.Attributes,
	); err != nil {
		return identity.Resource{}, fmt.Errorf("failed to create overflow bucket: %w", err)
	}
	return identity.OfResource(r), nil
}

func (s *Value) getOverflowScopeBucketID(
	res identity.Resource,
) (identity.Scope, error) {
	scope := pcommon.NewInstrumentationScope()
	if err := decorate(
		scope.Attributes(),
		s.scopeLimitCfg.Overflow.Attributes,
	); err != nil {
		return identity.Scope{}, fmt.Errorf("failed to create overflow bucket: %w", err)
	}
	return identity.OfScope(res, scope), nil
}

type dataPointSlice[DP dataPoint[DP]] interface {
	Len() int
	At(i int) DP
	AppendEmpty() DP
}

type dataPoint[Self any] interface {
	pmetric.NumberDataPoint | pmetric.SummaryDataPoint | pmetric.HistogramDataPoint | pmetric.ExponentialHistogramDataPoint

	Timestamp() pcommon.Timestamp
	SetTimestamp(pcommon.Timestamp)
	Attributes() pcommon.Map
	CopyTo(dest Self)
}

func merge[DPS dataPointSlice[DP], DP dataPoint[DP]](
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

func mergeCumulative[DPS dataPointSlice[DP], DP dataPoint[DP]](
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
		if ok || fromDP.Timestamp() > toDP.Timestamp() {
			fromDP.CopyTo(toDP)
		}
	}
}

func mergeDelta[DPS dataPointSlice[DP], DP dataPoint[DP]](
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

func scopeDPsCount(scope scopeMetrics) (c uint64) {
	metrics := scope.Metrics()
	for i := 0; i < metrics.Len(); i++ {
		c += metricDPsCount(metrics.At(i))
	}
	return c
}

func metricDPsCount(m metric) uint64 {
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
