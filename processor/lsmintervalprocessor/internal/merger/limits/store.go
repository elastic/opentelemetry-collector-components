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
	"fmt"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/identity"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	resourceLimitsAttrKey   = "_resource_limits"
	scopeLimitsAttrKey      = "_scope_limits"
	datapointsLimitsAttrKey = "_datapoints_limits"
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
		resourceLimitsCfg:   cfg.ResourceLimits,
		scopeLimitsCfg:      cfg.ScopeLimits,
		datapointsLimitsCfg: cfg.DatapointLimits,
		source:              pmetric.NewMetrics(),
		resourceLimits:      NewTracker[identity.Resource](cfg.ResourceLimits),
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
// TODO: Propogate error
func (s *Store) init(source pmetric.Metrics) error {
	s.source = source
	s.resourceLimits.SetOverflowBucketID(getOverflowResourceBucketID(s.resourceLimits))
	rms := s.source.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		rmID := identity.OfResource(rm.Resource())
		scopeLimits := NewTracker[identity.Scope](s.resourceLimitsCfg)
		scopeLimits.SetOverflowBucketID(getOverflowScopeBucketID(scopeLimits, rmID))
		rm.Resource().Attributes().RemoveIf(func(k string, v pcommon.Value) bool {
			switch k {
			case resourceLimitsAttrKey:
				s.resourceLimits.UnmarshalBinary(v.Bytes().AsRaw())
				return true
			case scopeLimitsAttrKey:
				scopeLimits.UnmarshalBinary(v.Bytes().AsRaw())
				return true
			}
			return false
		})
		s.resLookup[rmID] = resourceMetrics{
			ResourceMetrics: rm,
			scopeLimits:     scopeLimits,
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			scope := sm.Scope()
			smID := identity.OfScope(rmID, scope)
			datapointsLimits := NewTracker[identity.Stream](s.datapointsLimitsCfg)
			scope.Attributes().RemoveIf(func(k string, v pcommon.Value) bool {
				if k == datapointsLimitsAttrKey {
					datapointsLimits.UnmarshalBinary(v.Bytes().AsRaw())
					return true
				}
				return false
			})

			// TODO: Updating trackers
			// TODO: How do we deal with overflow bucket IDs in trackers? Are they deterministic based on configs?
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
		bLimits, err := s.resourceLimits.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal resource limits: %w", err)
		}
		// Encode resource tracker at the 0th resource metrics
		// TODO (lahsivjar): Is this safe? We don't ever remove
		// resource metrics so it should be but best to check.
		// Also, the limits checker should ensure max cardinality
		// is greater than zero.
		rmAttr := rms.At(0).Resource().Attributes()
		b := rmAttr.PutEmptyBytes(resourceLimitsAttrKey)
		b.FromRaw(bLimits)

		// Encode scope trackers in resource attributes
		for _, res := range s.resLookup {
			bLimits, err := res.scopeLimits.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal scope limits: %w", err)
			}
			resAttrs := res.ResourceMetrics.Resource().Attributes()
			b := resAttrs.PutEmptyBytes(scopeLimitsAttrKey)
			b.FromRaw(bLimits)
		}

		// Encode datapoints trackers in scope attributes
		for _, scope := range s.scopeLookup {
			bLimits, err := scope.datapointsLimits.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal datapoints limits: %w", err)
			}
			scopeAttrs := scope.ScopeMetrics.Scope().Attributes()
			b := scopeAttrs.PutEmptyBytes(datapointsLimitsAttrKey)
			b.FromRaw(bLimits)
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
		overflowMetric.SetName("_other")
		overflowMetric.SetDescription("Overflow count due to datapoints limit")
		overflowSum := overflowMetric.SetEmptySum()
		overflowSum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		overflowDP := overflowSum.DataPoints().AppendEmpty()
		if err := sm.datapointsLimits.Decorate(overflowDP.Attributes()); err != nil {
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
) identity.Resource {
	resID := identity.OfResource(otherRm.Resource())
	if _, ok := s.resLookup[resID]; ok {
		return resID
	}
	if s.resourceLimits.CheckOverflow(
		resID.Hash().Sum64(),
		otherRm.Resource().Attributes(),
	) {
		// Overflow, get/prepare an overflow bucket
		overflowResID := s.resourceLimits.GetOverflowBucketID()
		if overflowResID == (identity.Resource{}) {
			overflowRm := s.source.ResourceMetrics().AppendEmpty()
			s.resourceLimits.Decorate(overflowRm.Resource().Attributes())
			overflowResID = identity.OfResource(overflowRm.Resource())
			s.resourceLimits.SetOverflowBucketID(overflowResID)
			s.resLookup[overflowResID] = resourceMetrics{
				ResourceMetrics: overflowRm,
				scopeLimits:     NewTracker[identity.Scope](s.scopeLimitsCfg),
			}
		}
		return overflowResID
	}

	// Clone it *without* the ScopeMetricsSlice data
	rm := s.source.ResourceMetrics().AppendEmpty()
	rm.SetSchemaUrl(otherRm.SchemaUrl())
	otherRm.Resource().CopyTo(rm.Resource())
	s.resLookup[resID] = resourceMetrics{
		ResourceMetrics: rm,
		scopeLimits:     NewTracker[identity.Scope](s.scopeLimitsCfg),
	}
	return resID
}

func (s *Store) AddScopeMetrics(
	resID identity.Resource,
	otherSm pmetric.ScopeMetrics,
) identity.Scope {
	scopeID := identity.OfScope(resID, otherSm.Scope())
	if _, ok := s.scopeLookup[scopeID]; ok {
		return scopeID
	}
	res := s.resLookup[resID]
	if res.scopeLimits.CheckOverflow(
		scopeID.Hash().Sum64(),
		otherSm.Scope().Attributes(),
	) {
		// Overflow, get/prepare an overflow bucket
		overflowScopeID := res.scopeLimits.GetOverflowBucketID()
		if overflowScopeID == (identity.Scope{}) {
			overflowScope := res.ScopeMetrics().AppendEmpty()
			res.scopeLimits.Decorate(overflowScope.Scope().Attributes())
			overflowScopeID = identity.OfScope(resID, overflowScope.Scope())
			res.scopeLimits.SetOverflowBucketID(overflowScopeID)
			s.scopeLookup[overflowScopeID] = scopeMetrics{
				ScopeMetrics:     overflowScope,
				datapointsLimits: NewTracker[identity.Stream](s.datapointsLimitsCfg),
			}
		}
		return overflowScopeID
	}

	// Clone it *without* the MetricSlice data
	sm := res.ScopeMetrics().AppendEmpty()
	otherSm.Scope().CopyTo(sm.Scope())
	sm.SetSchemaUrl(otherSm.SchemaUrl())
	s.scopeLookup[scopeID] = scopeMetrics{
		ScopeMetrics:     sm,
		datapointsLimits: NewTracker[identity.Stream](s.datapointsLimitsCfg),
	}
	return scopeID
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

func getOverflowResourceBucketID(s *Tracker[identity.Resource]) identity.Resource {
	r := pcommon.NewResource()
	s.Decorate(r.Attributes())
	return identity.OfResource(r)
}

func getOverflowScopeBucketID(s *Tracker[identity.Scope], resID identity.Resource) identity.Scope {
	is := pcommon.NewInstrumentationScope()
	s.Decorate(is.Attributes())
	return identity.OfScope(resID, is)
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
