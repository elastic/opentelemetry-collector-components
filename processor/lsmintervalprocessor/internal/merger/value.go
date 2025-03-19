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
	"slices"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/identity"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger/limits"
)

const (
	version = uint8(1)

	overflowMetricName = "_overflow_metric"
	overflowMetricDesc = "Overflow metric count due to metric limit"

	overflowDatapointMetricName = "_overflow_datapoints"
	overflowDatapointMetricDesc = "Overflow datapoint count due to datapoint limit"
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
	resourceLimitCfg  config.LimitConfig
	scopeLimitCfg     config.LimitConfig
	metricLimitCfg    config.LimitConfig
	datapointLimitCfg config.LimitConfig

	source   pmetric.Metrics
	trackers *limits.Trackers

	// Lookup tables created from source
	lookupsInitialized bool
	resLookup          map[identity.Resource]pdataResourceMetrics
	scopeLookup        map[identity.Scope]pdataScopeMetrics
	metricLookup       map[identity.Metric]pdataMetric
	numberLookup       map[identity.Stream]pmetric.NumberDataPoint
	summaryLookup      map[identity.Stream]pmetric.SummaryDataPoint
	histoLookup        map[identity.Stream]pmetric.HistogramDataPoint
	expHistoLookup     map[identity.Stream]pmetric.ExponentialHistogramDataPoint
}

type pdataResourceMetrics struct {
	pmetric.ResourceMetrics

	// Keeps track of scopes within each resource metric
	scopeTracker *limits.ScopeTracker
}

type pdataScopeMetrics struct {
	pmetric.ScopeMetrics

	// Keeps track of metrics within each scope metric
	metricTracker *limits.MetricTracker
}

type pdataMetric struct {
	pmetric.Metric

	// Keeps track of datapoints within each metric
	datapointTracker *limits.Tracker
}

// NewValue creates a new instance of the value with the configured limiters.
func NewValue(resLimit, scopeLimit, metricLimit, datapointLimit config.LimitConfig) *Value {
	return &Value{
		resourceLimitCfg:  resLimit,
		scopeLimitCfg:     scopeLimit,
		metricLimitCfg:    metricLimit,
		datapointLimitCfg: datapointLimit,
		source:            pmetric.NewMetrics(),
	}
}

// AppendBinary marshals the value into its binary representation,
// and appends it to b.
//
// Limit trackers and pmetric are marshaled into the same binary
// representation.
func (s *Value) AppendBinary(b []byte) ([]byte, error) {
	b = append(b, version)

	if s.source.DataPointCount() == 0 {
		// Nothing to marshal
		return b, nil
	}

	var marshaler pmetric.ProtoMarshaler
	pmb, err := marshaler.MarshalMetrics(s.source)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metrics: %w", err)
	}

	lenOffset := len(b)
	b = slices.Grow(b, 4)
	b = b[:lenOffset+4] // leave space for the length of encoded trackers

	b, err = s.trackers.AppendBinary(b)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal trackers: %w", err)
	}
	trackersLen := len(b) - lenOffset - 4
	binary.BigEndian.PutUint32(b[lenOffset:lenOffset+4], uint32(trackersLen))
	b = append(b, pmb...)
	return b, nil
}

// Unmarshal unmarshals the binary into the value struct. The value consists
// of the pmetric data structure and a set of limits tracking the overflows
// for each of the pmetric children (resource, scope, and datapoints). The
// limits are marshaled and encoded separately from the pmetric datastructure.
func (s *Value) Unmarshal(data []byte) error {
	if len(data) == 0 {
		return errors.New("failed to unmarshal value, invalid length")
	}
	if data[0] != version {
		return fmt.Errorf("unsupported version: %d", data[0])
	}
	data = data[1:]

	if len(data) == 0 {
		return nil
	}
	if len(data) < 4 {
		// For non-nil value, tracker must be marshaled
		return errors.New("failed to unmarshal value, invalid length")
	}
	trackersLen := int(binary.BigEndian.Uint32(data[:4]))
	data = data[4:]
	if trackersLen > 0 {
		// Unmarshal trackers
		s.trackers = limits.NewTrackers(
			uint64(s.resourceLimitCfg.MaxCardinality),
			uint64(s.scopeLimitCfg.MaxCardinality),
			uint64(s.metricLimitCfg.MaxCardinality),
			uint64(s.datapointLimitCfg.MaxCardinality),
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
		resID, rm, err := v.addResourceMetrics(rmOther)
		if err != nil {
			return fmt.Errorf("failed while merging resource metrics: %w", err)
		}
		scopeTracker := op.trackers.GetScopeTracker(i)
		if scopeTracker != nil {
			if err := rm.scopeTracker.MergeEstimators(scopeTracker.Tracker); err != nil {
				return fmt.Errorf("failed to merge scope overflow estimators: %w", err)
			}
		}
		smsOther := rmOther.ScopeMetrics()
		for j := 0; j < smsOther.Len(); j++ {
			smOther := smsOther.At(j)
			scopeID, sm, err := v.addScopeMetrics(resID, rm, smOther)
			if err != nil {
				return fmt.Errorf("failed while merging scope metrics: %w", err)
			}
			metricTracker := scopeTracker.GetMetricTracker(j)
			if metricTracker != nil {
				if err := sm.metricTracker.MergeEstimators(metricTracker.Tracker); err != nil {
					return fmt.Errorf("failed to merge scope datapoints overflow estimators: %w", err)
				}
			}
			msOther := smOther.Metrics()
			for k := 0; k < msOther.Len(); k++ {
				mOther := msOther.At(k)
				metricID, m, overflow := v.addMetric(scopeID, sm, mOther)
				if overflow {
					// On metric overflow, we discard any datapoint overflow estimator
					// since metric overflow is accounts only for unique metrics.
					continue
				}
				dpTracker := metricTracker.GetDatapointTracker(k)
				if dpTracker != nil {
					if err := m.datapointTracker.MergeEstimators(dpTracker); err != nil {
						return fmt.Errorf("failed to merge datapoint overflow estimators: %w", err)
					}
				}
				v.mergeMetric(metricID, m, mOther)
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
	otherRm pmetric.ResourceMetrics,
	otherSm pmetric.ScopeMetrics,
	otherM pmetric.Metric,
) error {
	if metricDPsCount(otherM) == 0 {
		// Nothing to merge as either there are 0 or only unsupported metrics
		return nil
	}
	v.initLookupTables()
	// TODO: Precheck the metric for datapoints existence, if none exists
	// then don't add resource/scope metrics. This will help to remove the
	// remove if checks from the finalize method.
	// OR
	// Do this check in the prior call where we know the metric type.
	resID, rm, err := v.addResourceMetrics(otherRm)
	if err != nil {
		return err
	}
	scopeID, sm, err := v.addScopeMetrics(resID, rm, otherSm)
	if err != nil {
		return err
	}
	metricID, m, overflow := v.addMetric(scopeID, sm, otherM)
	if overflow {
		return nil
	}
	v.mergeMetric(metricID, m, otherM)
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
		if !sm.metricTracker.HasOverflow() {
			continue
		}
		// Add overflow metric due to metric limit breached
		if err := fillOverflowMetric(
			sm.ScopeMetrics.Metrics().AppendEmpty(),
			overflowMetricName,
			overflowMetricDesc,
			sm.metricTracker.EstimateOverflow(),
			s.metricLimitCfg.Overflow.Attributes,
		); err != nil {
			return pmetric.Metrics{}, fmt.Errorf("failed to finalize merged metric: %w", err)
		}
	}
	for mID, m := range s.metricLookup {
		if !m.datapointTracker.HasOverflow() {
			continue
		}
		// Add overflow metric due to datapoint limit breached
		sm := s.scopeLookup[mID.Scope()]
		if err := fillOverflowMetric(
			sm.ScopeMetrics.Metrics().AppendEmpty(),
			overflowDatapointMetricName,
			overflowDatapointMetricDesc,
			m.datapointTracker.EstimateOverflow(),
			s.datapointLimitCfg.Overflow.Attributes,
		); err != nil {
			return pmetric.Metrics{}, fmt.Errorf("failed to finalize merged metric: %w", err)
		}
	}
	return s.source, nil
}

func (s *Value) initLookupTables() {
	if s.lookupsInitialized {
		return
	}
	s.lookupsInitialized = true

	// If lookup tables are initialized we will need the lookup maps
	s.resLookup = make(map[identity.Resource]pdataResourceMetrics)
	s.scopeLookup = make(map[identity.Scope]pdataScopeMetrics)
	s.metricLookup = make(map[identity.Metric]pdataMetric)

	rms := s.source.ResourceMetrics()
	if rms.Len() == 0 {
		// Nothing to merge
		return
	}

	// Initialize the lookup tables assuming that the limits were respected
	// for the marshaled data and unexpected overflow will not happen.
	// Initialization is done by directly accessing the map and without
	// checking overflows to avoid accounting overflows as normal buckets.
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		rmID := identity.OfResource(rm.Resource())
		scopeTracker := s.trackers.GetScopeTracker(i)
		s.resLookup[rmID] = pdataResourceMetrics{
			ResourceMetrics: rm,
			scopeTracker:    scopeTracker,
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			scope := sm.Scope()
			smID := identity.OfScope(rmID, scope)
			metricTracker := scopeTracker.GetMetricTracker(j)
			s.scopeLookup[smID] = pdataScopeMetrics{
				ScopeMetrics:  sm,
				metricTracker: metricTracker,
			}
			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)
				mID := identity.OfMetric(smID, m)
				s.metricLookup[mID] = pdataMetric{
					Metric:           m,
					datapointTracker: metricTracker.GetDatapointTracker(k),
				}

				//exhaustive:enforce
				switch m.Type() {
				case pmetric.MetricTypeEmpty:
					continue
				case pmetric.MetricTypeGauge:
					// TODO (lahsivjar): implement gauge support
				case pmetric.MetricTypeSum:
					if s.numberLookup == nil {
						s.numberLookup = make(map[identity.Stream]pmetric.NumberDataPoint)
					}
					dps := m.Sum().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						streamID := identity.OfStream(mID, dp)
						s.numberLookup[streamID] = dp
					}
				case pmetric.MetricTypeSummary:
					if s.summaryLookup == nil {
						s.summaryLookup = make(map[identity.Stream]pmetric.SummaryDataPoint)
					}
					dps := m.Summary().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						streamID := identity.OfStream(mID, dp)
						s.summaryLookup[streamID] = dp
					}
				case pmetric.MetricTypeHistogram:
					if s.histoLookup == nil {
						s.histoLookup = make(map[identity.Stream]pmetric.HistogramDataPoint)
					}
					dps := m.Histogram().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						streamID := identity.OfStream(mID, dp)
						s.histoLookup[streamID] = dp
					}
				case pmetric.MetricTypeExponentialHistogram:
					if s.expHistoLookup == nil {
						s.expHistoLookup = make(map[identity.Stream]pmetric.ExponentialHistogramDataPoint)
					}
					dps := m.ExponentialHistogram().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						streamID := identity.OfStream(mID, dp)
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
) (identity.Resource, pdataResourceMetrics, error) {
	resID := identity.OfResource(otherRm.Resource())
	if rm, ok := s.resLookup[resID]; ok {
		return resID, rm, nil
	}
	if s.trackers == nil {
		s.trackers = limits.NewTrackers(
			uint64(s.resourceLimitCfg.MaxCardinality),
			uint64(s.scopeLimitCfg.MaxCardinality),
			uint64(s.metricLimitCfg.MaxCardinality),
			uint64(s.datapointLimitCfg.MaxCardinality),
		)
	}
	if s.trackers.GetResourceTracker().CheckOverflow(resID.Hash) {
		// Overflow, get/prepare an overflow bucket
		overflowResID, err := s.getOverflowResourceIdentity()
		if err != nil {
			return identity.Resource{}, pdataResourceMetrics{}, err
		}
		if rm, ok := s.resLookup[overflowResID]; ok {
			return overflowResID, rm, nil
		}
		overflowRm := s.source.ResourceMetrics().AppendEmpty()
		overflowRm.SetSchemaUrl(otherRm.SchemaUrl())
		if err := decorate(
			overflowRm.Resource().Attributes(),
			s.resourceLimitCfg.Overflow.Attributes,
		); err != nil {
			return identity.Resource{}, pdataResourceMetrics{}, err
		}
		rm := pdataResourceMetrics{
			ResourceMetrics: overflowRm,
			scopeTracker:    s.trackers.NewScopeTracker(),
		}
		s.resLookup[overflowResID] = rm
		return overflowResID, rm, nil
	}
	// Clone it *without* the ScopeMetricsSlice data
	rmOrig := s.source.ResourceMetrics().AppendEmpty()
	rmOrig.SetSchemaUrl(otherRm.SchemaUrl())
	otherRm.Resource().CopyTo(rmOrig.Resource())
	rm := pdataResourceMetrics{
		ResourceMetrics: rmOrig,
		scopeTracker:    s.trackers.NewScopeTracker(),
	}
	s.resLookup[resID] = rm
	return resID, rm, nil
}

// addScopeMetrics adds a new scope metrics to the store while also
// applying scope limiters. If a limit is configured and breached by
// adding the provided scope metric, then, a new overflow scope
// metric is created and returned.
func (s *Value) addScopeMetrics(
	resID identity.Resource,
	rm pdataResourceMetrics,
	otherSm pmetric.ScopeMetrics,
) (identity.Scope, pdataScopeMetrics, error) {
	scopeID := identity.OfScope(resID, otherSm.Scope())
	if sm, ok := s.scopeLookup[scopeID]; ok {
		return scopeID, sm, nil
	}
	if rm.scopeTracker.CheckOverflow(scopeID.Hash) {
		// Overflow, get/prepare an overflow bucket
		overflowScopeID, err := s.getOverflowScopeIdentity(resID)
		if err != nil {
			return identity.Scope{}, pdataScopeMetrics{}, err
		}
		if sm, ok := s.scopeLookup[overflowScopeID]; ok {
			return overflowScopeID, sm, nil
		}
		overflowScope := rm.ScopeMetrics().AppendEmpty()
		overflowScope.SetSchemaUrl(otherSm.SchemaUrl())
		if err := decorate(
			overflowScope.Scope().Attributes(),
			s.scopeLimitCfg.Overflow.Attributes,
		); err != nil {
			return identity.Scope{}, pdataScopeMetrics{}, err
		}
		sm := pdataScopeMetrics{
			ScopeMetrics:  overflowScope,
			metricTracker: rm.scopeTracker.NewMetricTracker(),
		}
		s.scopeLookup[overflowScopeID] = sm
		return overflowScopeID, sm, nil
	}
	// Clone it *without* the MetricSlice data
	smOrig := rm.ScopeMetrics().AppendEmpty()
	otherSm.Scope().CopyTo(smOrig.Scope())
	smOrig.SetSchemaUrl(otherSm.SchemaUrl())
	sm := pdataScopeMetrics{
		ScopeMetrics:  smOrig,
		metricTracker: rm.scopeTracker.NewMetricTracker(),
	}
	s.scopeLookup[scopeID] = sm
	return scopeID, sm, nil
}

// addMetric adds the given metric to the store while also considering
// datapoint limiters. If a limit is configured and breached by adding a new
// metric then the datapoint overflow is updated and the metric is discarded
// as when datapoint overflows, a new metric overflow sum metric is added
// with delta temporality tracking the cardinality estimate of the overflow.
// The returned bool is `true` if there is overflow and `false` otherwise.
func (s *Value) addMetric(
	scopeID identity.Scope,
	sm pdataScopeMetrics,
	otherM pmetric.Metric,
) (identity.Metric, pdataMetric, bool) {
	mID := identity.OfMetric(scopeID, otherM)
	if m, ok := s.metricLookup[mID]; ok {
		return mID, m, false
	}
	if sm.metricTracker.CheckOverflow(mID.Hash) {
		// Metric overflow detected. In this case no action has to be taken
		// at this point since metric overflow should create a new sum metric
		// recording the number of unique metric that overflowed. This number
		// will be recorded in the limit tracker and the metric will be
		// populated on demand.
		return identity.Metric{}, pdataMetric{}, true
	}

	// Clone it *without* the datapoint data
	mOrig := sm.Metrics().AppendEmpty()
	mOrig.SetName(otherM.Name())
	mOrig.SetDescription(otherM.Description())
	mOrig.SetUnit(otherM.Unit())
	switch otherM.Type() {
	case pmetric.MetricTypeGauge:
		mOrig.SetEmptyGauge()
	case pmetric.MetricTypeSummary:
		mOrig.SetEmptySummary()
	case pmetric.MetricTypeSum:
		otherSum := otherM.Sum()

		sum := mOrig.SetEmptySum()
		sum.SetAggregationTemporality(otherSum.AggregationTemporality())
		sum.SetIsMonotonic(otherSum.IsMonotonic())
	case pmetric.MetricTypeHistogram:
		otherHist := otherM.Histogram()

		hist := mOrig.SetEmptyHistogram()
		hist.SetAggregationTemporality(otherHist.AggregationTemporality())
	case pmetric.MetricTypeExponentialHistogram:
		otherExp := otherM.ExponentialHistogram()

		exp := mOrig.SetEmptyExponentialHistogram()
		exp.SetAggregationTemporality(otherExp.AggregationTemporality())
	}
	m := pdataMetric{
		Metric:           mOrig,
		datapointTracker: sm.metricTracker.NewDatapointTracker(),
	}
	s.metricLookup[mID] = m
	return mID, m, false
}

// addSumDataPoint returns a data point entry in the store for the given metric
// and the external data point if it is present. If the data point is not
// present then either a new data point is added or if the data point overflows
// due to configured limit then an empty data point is returned. The returned
// bool value is `true` if the datapoint already exists and `false` otherwise.
func (s *Value) addSumDataPoint(
	metricID identity.Metric,
	metric pdataMetric,
	otherDP pmetric.NumberDataPoint,
) (pmetric.NumberDataPoint, bool) {
	streamID := identity.OfStream(metricID, otherDP)
	if s.numberLookup == nil {
		s.numberLookup = make(map[identity.Stream]pmetric.NumberDataPoint)
	} else if dp, ok := s.numberLookup[streamID]; ok {
		return dp, true
	}
	if metric.datapointTracker.CheckOverflow(streamID.Hash) {
		// Datapoints overflow detected. In this case no action has to be
		// done at this point since data point overflow should create a new
		// overflow metric of sum type recording the number of unique
		// datapoints. This number will be recorded in the limit tracker
		// and the metric will be populated on demand.
		return pmetric.NumberDataPoint{}, false
	}
	dp := metric.Sum().DataPoints().AppendEmpty()
	// New datapoint created, so copy the otherDP to the new one
	otherDP.CopyTo(dp)
	s.numberLookup[streamID] = dp
	return dp, false
}

// addSummaryDataPoint returns a data point entry in the store for the given
// metric and the external data point if it is present. If the data point is
// not present then either a new data point is added or if the data point
// overflows due to configured limit then an empty data point is returned.
// The returned bool value is `true` if datapoint already exists and `false`
// otherwise.
func (s *Value) addSummaryDataPoint(
	metricID identity.Metric,
	metric pdataMetric,
	otherDP pmetric.SummaryDataPoint,
) (pmetric.SummaryDataPoint, bool) {
	streamID := identity.OfStream(metricID, otherDP)
	if s.summaryLookup == nil {
		s.summaryLookup = make(map[identity.Stream]pmetric.SummaryDataPoint)
	} else if dp, ok := s.summaryLookup[streamID]; ok {
		return dp, true
	}
	if metric.datapointTracker.CheckOverflow(streamID.Hash) {
		// Datapoints overflow detected. In this case no action has to be
		// done at this point since data point overflow should create a new
		// overflow metric of sum type recording the number of unique
		// datapoints. This number will be recorded in the limit tracker
		// and the metric will be populated on demand.
		return pmetric.SummaryDataPoint{}, false
	}
	dp := metric.Summary().DataPoints().AppendEmpty()
	// New datapoint created, so copy the otherDP to the new one
	otherDP.CopyTo(dp)
	s.summaryLookup[streamID] = dp
	return dp, false
}

// addHistogramDataPoint returns a data point entry in the store for the given
// metric and the external data point if it is present. If the data point is
// not present then either a new data point is added or if the data point
// overflows due to configured limit then an empty data point is returned.
// The returned bool value is `true` if datapoint already exists and `false`
// otherwise.
func (s *Value) addHistogramDataPoint(
	metricID identity.Metric,
	metric pdataMetric,
	otherDP pmetric.HistogramDataPoint,
) (pmetric.HistogramDataPoint, bool) {
	streamID := identity.OfStream(metricID, otherDP)
	if s.histoLookup == nil {
		s.histoLookup = make(map[identity.Stream]pmetric.HistogramDataPoint)
	} else if dp, ok := s.histoLookup[streamID]; ok {
		return dp, true
	}
	if metric.datapointTracker.CheckOverflow(streamID.Hash) {
		// Datapoints overflow detected. In this case no action has to be
		// done at this point since data point overflow should create a new
		// overflow metric of sum type recording the number of unique
		// datapoints. This number will be recorded in the limit tracker
		// and the metric will be populated on demand.
		return pmetric.HistogramDataPoint{}, false
	}
	dp := metric.Histogram().DataPoints().AppendEmpty()
	// New datapoint created, so copy the otherDP to the new one
	otherDP.CopyTo(dp)
	s.histoLookup[streamID] = dp
	return dp, false
}

// addExponentialHistogramDataPoint returns a data point entry in the store
// for the given metric and the external data point if it is present. If the
// data point is not present then either a new data point is added or if the
// data point overflows due to configured limit then an empty data point is
// returned. The returned bool value is `true` if datapoint already exists
// and `false` otherwise.
func (s *Value) addExponentialHistogramDataPoint(
	metricID identity.Metric,
	metric pdataMetric,
	otherDP pmetric.ExponentialHistogramDataPoint,
) (pmetric.ExponentialHistogramDataPoint, bool) {
	streamID := identity.OfStream(metricID, otherDP)
	if s.expHistoLookup == nil {
		s.expHistoLookup = make(map[identity.Stream]pmetric.ExponentialHistogramDataPoint)
	} else if dp, ok := s.expHistoLookup[streamID]; ok {
		return dp, true
	}
	if metric.datapointTracker.CheckOverflow(streamID.Hash) {
		// Datapoints overflow detected. In this case no action has to be
		// done at this point since data point overflow should create a new
		// overflow metric of sum type recording the number of unique
		// datapoints. This number will be recorded in the limit tracker
		// and the metric will be populated on demand.
		return pmetric.ExponentialHistogramDataPoint{}, false
	}
	dp := metric.ExponentialHistogram().DataPoints().AppendEmpty()
	// New datapoint created, so copy the otherDP to the new one
	otherDP.CopyTo(dp)
	s.expHistoLookup[streamID] = dp
	return dp, false
}

func (v *Value) mergeMetric(
	metricID identity.Metric,
	m pdataMetric,
	otherM pmetric.Metric,
) {
	switch otherM.Type() {
	case pmetric.MetricTypeSum:
		mergeDataPoints(
			otherM.Sum().DataPoints(),
			metricID,
			m,
			v.addSumDataPoint,
			otherM.Sum().AggregationTemporality(),
		)
	case pmetric.MetricTypeSummary:
		mergeDataPoints(
			otherM.Summary().DataPoints(),
			metricID,
			m,
			v.addSummaryDataPoint,
			// Assume summary to be cumulative temporality
			pmetric.AggregationTemporalityCumulative,
		)
	case pmetric.MetricTypeHistogram:
		mergeDataPoints(
			otherM.Histogram().DataPoints(),
			metricID,
			m,
			v.addHistogramDataPoint,
			otherM.Histogram().AggregationTemporality(),
		)
	case pmetric.MetricTypeExponentialHistogram:
		mergeDataPoints(
			otherM.ExponentialHistogram().DataPoints(),
			metricID,
			m,
			v.addExponentialHistogramDataPoint,
			otherM.ExponentialHistogram().AggregationTemporality(),
		)
	}
}

func (s *Value) getOverflowResourceIdentity() (identity.Resource, error) {
	r := pcommon.NewResource()
	if err := decorate(
		r.Attributes(),
		s.resourceLimitCfg.Overflow.Attributes,
	); err != nil {
		return identity.Resource{}, fmt.Errorf("failed to create overflow bucket: %w", err)
	}
	return identity.OfResource(r), nil
}

func (s *Value) getOverflowScopeIdentity(
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
