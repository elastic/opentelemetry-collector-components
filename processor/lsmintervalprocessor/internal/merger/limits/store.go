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

package limits // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger/limits"

import (
	"errors"
	"fmt"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/identity"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	resourceLimitsEncodingPrefix = "_resource"
	overflowMetricName           = "_other"
	overflowMetricDesc           = "Overflow count due to datapoints limit"
)

// The source metric must be modified only by the store once the store is created.
type Store struct {
	resourceLimitsCfg   config.LimitConfig
	scopeLimitsCfg      config.LimitConfig
	datapointsLimitsCfg config.LimitConfig

	source pmetric.Metrics
	// Keeps track of resource metrics overflow
	resourceLimits *Tracker[identity.Resource]

	// Lookup tables created from source
	resLookup      map[identity.Resource]resourceMetrics
	scopeLookup    map[identity.Scope]scopeMetrics
	metricLookup   map[identity.Metric]metric
	numberLookup   map[identity.Stream]numberDataPoint
	summaryLookup  map[identity.Stream]summaryDataPoint
	histoLookup    map[identity.Stream]histogramDataPoint
	expHistoLookup map[identity.Stream]exponentialHistogramDataPoint
}

func NewStore(cfg *config.Config) *Store {
	s := &Store{
		resourceLimitsCfg:   cfg.ResourceLimit,
		scopeLimitsCfg:      cfg.ScopeLimit,
		datapointsLimitsCfg: cfg.ScopeDatapointLimit,
		source:              pmetric.NewMetrics(),
		resourceLimits:      NewTracker[identity.Resource](cfg.ResourceLimit.MaxCardinality),
		resLookup:           make(map[identity.Resource]resourceMetrics),
		scopeLookup:         make(map[identity.Scope]scopeMetrics),
		metricLookup:        make(map[identity.Metric]metric),
		numberLookup:        make(map[identity.Stream]numberDataPoint),
		summaryLookup:       make(map[identity.Stream]summaryDataPoint),
		histoLookup:         make(map[identity.Stream]histogramDataPoint),
		expHistoLookup:      make(map[identity.Stream]exponentialHistogramDataPoint),
	}
	return s
}

// init initializes the lookup maps from the source. It assumes that the source
// will NOT overflow or if it has overflowed it already has the overflow
// buckets initialized.
func (s *Store) init(source pmetric.Metrics) error {
	s.source = source
	rms := s.source.ResourceMetrics()
	s.resourceLimits = NewTracker[identity.Resource](s.resourceLimitsCfg.MaxCardinality)
	if source.ResourceMetrics().Len() > 0 {
		if err := s.resourceLimits.UnmarshalWithPrefix(
			resourceLimitsEncodingPrefix,
			rms.At(0).Resource().Attributes(),
		); err != nil {
			return fmt.Errorf("failed to unmarshal resource limits: %w", err)
		}
	}
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		rmID := identity.OfResource(rm.Resource())
		scopeLimits := NewTracker[identity.Scope](s.scopeLimitsCfg.MaxCardinality)
		if err := scopeLimits.Unmarshal(rm.Resource().Attributes()); err != nil {
			return fmt.Errorf("failed to unmarshal scope limits: %w", err)
		}
		s.resLookup[rmID] = resourceMetrics{
			ResourceMetrics: rm,
			scopeLimits:     scopeLimits,
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			scope := sm.Scope()
			smID := identity.OfScope(rmID, scope)
			datapointsLimits := NewTracker[identity.Stream](s.datapointsLimitsCfg.MaxCardinality)
			if err := datapointsLimits.Unmarshal(scope.Attributes()); err != nil {
				return fmt.Errorf("failed to unmarshal datapoints limits: %w", err)
			}
			s.scopeLookup[smID] = scopeMetrics{
				ScopeMetrics:     sm,
				datapointsLimits: datapointsLimits,
			}
			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				metricID := identity.OfMetric(smID, metric)
				s.metricLookup[metricID] = metric

				switch metric.Type() {
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
	return nil
}

func (s *Store) MarshalProto() ([]byte, error) {
	rms := s.source.ResourceMetrics()
	if rms.Len() > 0 {
		// Encode resource tracker at the 0th resource metrics
		// TODO (lahsivjar): Is this safe? We don't ever remove
		// resource metrics so it should be but best to check.
		// Also, the limits checker should ensure max cardinality
		// is greater than zero.
		if err := s.resourceLimits.MarshalWithPrefix(
			resourceLimitsEncodingPrefix,
			rms.At(0).Resource().Attributes(),
		); err != nil {
			return nil, fmt.Errorf("failed to marshal resource limits: %w", err)
		}

		// Encode scope trackers in resource attributes
		for _, res := range s.resLookup {
			resAttrs := res.ResourceMetrics.Resource().Attributes()
			if err := res.scopeLimits.Marshal(resAttrs); err != nil {
				return nil, fmt.Errorf("failed to marshal scope limits: %w", err)
			}
		}

		// Encode datapoints trackers in scope attributes
		for _, scope := range s.scopeLookup {
			scopeAttrs := scope.ScopeMetrics.Scope().Attributes()
			if err := scope.datapointsLimits.Marshal(scopeAttrs); err != nil {
				return nil, fmt.Errorf("failed to marshal datapoints limits: %w", err)
			}
		}
	}
	var marshaler pmetric.ProtoMarshaler
	return marshaler.MarshalMetrics(s.source)
}

func (s *Store) UnmarshalProto(data []byte) error {
	var unmarshaler pmetric.ProtoUnmarshaler
	m, err := unmarshaler.UnmarshalMetrics(data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}
	return s.init(m)
}

// Get returns the current pmetric.Metrics. The returned metric will
// include datapoint overflow metrics if `Finalize` is called before
// and will not include these metrics if `Finalize` is not called.
func (s *Store) Get() pmetric.Metrics {
	return s.source
}

// Finalize finalizes all overflows in the metrics to prepare it for
// harvest. This method should be called for harvest and only once.
func (s *Store) Finalize() (pmetric.Metrics, error) {
	// At this point we need to assume that the metrics are returned
	// as a final step in the store, thus, prepare the final metric.
	// In the final metric we have to add datapoint limits.
	for _, sm := range s.scopeLookup {
		if sm.datapointsLimits.overflowCounts == nil {
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
			s.datapointsLimitsCfg.Overflow.Attributes,
		); err != nil {
			return pmetric.Metrics{}, fmt.Errorf("failed to finalize merged metric: %w", err)
		}
		overflowDP.SetIntValue(int64(sm.datapointsLimits.overflowCounts.Estimate()))
	}
	// Remove any hanging resource or scope which failed to have any entries
	// due to children reaching their limits.
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

// AddResourceMetrics adds a new resource metrics to the store while also
// applying resource limiters. If a limit is configured and breached by
// adding the provided resource metric, then, a new overflow resource
// metric is created and returned.
func (s *Store) AddResourceMetrics(
	otherRm pmetric.ResourceMetrics,
) (identity.Resource, error) {
	resID := identity.OfResource(otherRm.Resource())
	if _, ok := s.resLookup[resID]; ok {
		return resID, nil
	}
	if s.resourceLimits.CheckOverflow(
		resID.Hash().Sum64(),
		otherRm.Resource().Attributes(),
	) {
		// Overflow, get/prepare an overflow bucket
		overflowResID, err := s.getOverflowResourceBucketID()
		if err != nil {
			return identity.Resource{}, err
		}
		if _, ok := s.resLookup[overflowResID]; !ok {
			overflowRm := s.source.ResourceMetrics().AppendEmpty()
			if err := decorate(
				overflowRm.Resource().Attributes(),
				s.resourceLimitsCfg.Overflow.Attributes,
			); err != nil {
				return identity.Resource{}, err
			}
			s.resLookup[overflowResID] = resourceMetrics{
				ResourceMetrics: overflowRm,
				scopeLimits:     NewTracker[identity.Scope](s.scopeLimitsCfg.MaxCardinality),
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
		scopeLimits:     NewTracker[identity.Scope](s.scopeLimitsCfg.MaxCardinality),
	}
	return resID, nil
}

func (s *Store) AddScopeMetrics(
	resID identity.Resource,
	otherSm pmetric.ScopeMetrics,
) (identity.Scope, error) {
	scopeID := identity.OfScope(resID, otherSm.Scope())
	if _, ok := s.scopeLookup[scopeID]; ok {
		return scopeID, nil
	}
	res := s.resLookup[resID]
	if res.scopeLimits.CheckOverflow(
		scopeID.Hash().Sum64(),
		otherSm.Scope().Attributes(),
	) {
		// Overflow, get/prepare an overflow bucket
		overflowScopeID, err := s.getOverflowScopeBucketID(resID)
		if err != nil {
			return identity.Scope{}, err
		}
		if _, ok := s.scopeLookup[overflowScopeID]; !ok {
			overflowScope := res.ScopeMetrics().AppendEmpty()
			if err := decorate(
				overflowScope.Scope().Attributes(),
				s.scopeLimitsCfg.Overflow.Attributes,
			); err != nil {
				return identity.Scope{}, err
			}
			s.scopeLookup[overflowScopeID] = scopeMetrics{
				ScopeMetrics:     overflowScope,
				datapointsLimits: NewTracker[identity.Stream](s.datapointsLimitsCfg.MaxCardinality),
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
		datapointsLimits: NewTracker[identity.Stream](s.datapointsLimitsCfg.MaxCardinality),
	}
	return scopeID, nil
}

func (s *Store) AddMetric(
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

// AddSumDataPoint returns a data point entry in the store for the given metric
// and the external data point if it is present. If the data point is not
// present then either a new data point is added or if the data point overflows
// due to configured limit then an empty data point is returned. The returned
// bool value is `true` if a new data point is created and `false` otherwise.
func (s *Store) AddSumDataPoint(
	metricID identity.Metric,
	otherDP pmetric.NumberDataPoint,
) (pmetric.NumberDataPoint, bool) {
	streamID := identity.OfStream(metricID, otherDP)
	if dp, ok := s.numberLookup[streamID]; ok {
		return dp, false
	}
	sm := s.scopeLookup[metricID.Scope()]
	metric := s.metricLookup[metricID]
	if sm.datapointsLimits.CheckOverflow(
		metricID.Hash().Sum64(),
		otherDP.Attributes(),
	) {
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

// AddSummaryDataPoint returns a data point entry in the store for the given
// metric and the external data point if it is present. If the data point is
// not present then either a new data point is added or if the data point
// overflows due to configured limit then an empty data point is returned.
// The returned bool value is `true` if a new data point is created and
// `false` otherwise.
func (s *Store) AddSummaryDataPoint(
	metricID identity.Metric,
	otherDP pmetric.SummaryDataPoint,
) (pmetric.SummaryDataPoint, bool) {
	streamID := identity.OfStream(metricID, otherDP)
	if dp, ok := s.summaryLookup[streamID]; ok {
		return dp, false
	}
	sm := s.scopeLookup[metricID.Scope()]
	metric := s.metricLookup[metricID]
	if sm.datapointsLimits.CheckOverflow(
		metricID.Hash().Sum64(),
		otherDP.Attributes(),
	) {
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

// AddHistogramDataPoint returns a data point entry in the store for the given
// metric and the external data point if it is present. If the data point is
// not present then either a new data point is added or if the data point
// overflows due to configured limit then an empty data point is returned.
// The returned bool value is `true` if a new data point is created and
// `false` otherwise.
func (s *Store) AddHistogramDataPoint(
	metricID identity.Metric,
	otherDP pmetric.HistogramDataPoint,
) (pmetric.HistogramDataPoint, bool) {
	streamID := identity.OfStream(metricID, otherDP)
	if dp, ok := s.histoLookup[streamID]; ok {
		return dp, false
	}
	sm := s.scopeLookup[metricID.Scope()]
	metric := s.metricLookup[metricID]
	if sm.datapointsLimits.CheckOverflow(
		metricID.Hash().Sum64(),
		otherDP.Attributes(),
	) {
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

// AddExponentialHistogramDataPoint returns a data point entry in the store
// for the given metric and the external data point if it is present. If the
// data point is not present then either a new data point is added or if the
// data point overflows due to configured limit then an empty data point is
// returned. The returned bool value is `true` if a new data point is created
// and `false` otherwise.
func (s *Store) AddExponentialHistogramDataPoint(
	metricID identity.Metric,
	otherDP pmetric.ExponentialHistogramDataPoint,
) (pmetric.ExponentialHistogramDataPoint, bool) {
	streamID := identity.OfStream(metricID, otherDP)
	if dp, ok := s.expHistoLookup[streamID]; ok {
		return dp, false
	}
	sm := s.scopeLookup[metricID.Scope()]
	metric := s.metricLookup[metricID]
	if sm.datapointsLimits.CheckOverflow(
		metricID.Hash().Sum64(),
		otherDP.Attributes(),
	) {
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

func (s *Store) getOverflowResourceBucketID() (identity.Resource, error) {
	r := pcommon.NewResource()
	if err := decorate(
		r.Attributes(),
		s.resourceLimitsCfg.Overflow.Attributes,
	); err != nil {
		return identity.Resource{}, fmt.Errorf("failed to create overflow bucket: %w", err)
	}
	return identity.OfResource(r), nil
}

func (s *Store) getOverflowScopeBucketID(
	res identity.Resource,
) (identity.Scope, error) {
	scope := pcommon.NewInstrumentationScope()
	if err := decorate(
		scope.Attributes(),
		s.scopeLimitsCfg.Overflow.Attributes,
	); err != nil {
		return identity.Scope{}, fmt.Errorf("failed to create overflow bucket: %w", err)
	}
	return identity.OfScope(res, scope), nil
}

type resourceMetrics struct {
	pmetric.ResourceMetrics

	// Keeps track of scope overflows within each resource metric
	scopeLimits *Tracker[identity.Scope]
}
type scopeMetrics struct {
	pmetric.ScopeMetrics
	datapointsLimits *Tracker[identity.Stream]
}
type metric = pmetric.Metric
type numberDataPoint = pmetric.NumberDataPoint
type summaryDataPoint = pmetric.SummaryDataPoint
type histogramDataPoint = pmetric.HistogramDataPoint
type exponentialHistogramDataPoint = pmetric.ExponentialHistogramDataPoint

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
