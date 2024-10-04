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

package aggregator // import "github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/aggregator"

import (
	"time"

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/model"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Aggregator provides a single interface to update all metrics
// datastructures. The required datastructure is selected using
// the metric definition.
type Aggregator[K any] struct {
	result pmetric.Metrics
	// smLookup maps resourceID against scope metrics since the aggregator
	// always produces a single scope.
	smLookup      map[[16]byte]pmetric.ScopeMetrics
	spanDurations map[model.MetricKey]map[[16]byte]map[[16]byte]*spanDurationDP
	counters      map[model.MetricKey]map[[16]byte]map[[16]byte]*counterDP
	timestamp     time.Time
}

// NewAggregator creates a new instance of aggregator.
func NewAggregator[K any](metrics pmetric.Metrics) *Aggregator[K] {
	return &Aggregator[K]{
		result:        metrics,
		smLookup:      make(map[[16]byte]pmetric.ScopeMetrics),
		spanDurations: make(map[model.MetricKey]map[[16]byte]map[[16]byte]*spanDurationDP),
		counters:      make(map[model.MetricKey]map[[16]byte]map[[16]byte]*counterDP),
		timestamp:     time.Now(),
	}
}

// Count aggregates the number of events of a specific type into the
// configured metrics.
func (a *Aggregator[K]) Count(
	md model.MetricDef[K],
	resAttrs, srcAttrs pcommon.Map,
	count uint64,
) error {
	if md.Counter == nil || count == 0 {
		// Nothing to do as the count is `0` or no counter is defined.
		return nil
	}

	resID := a.getResourceID(resAttrs)
	attrID := pdatautil.MapHash(srcAttrs)
	if _, ok := a.counters[md.Key]; !ok {
		a.counters[md.Key] = make(map[[16]byte]map[[16]byte]*counterDP)
	}
	if _, ok := a.counters[md.Key][resID]; !ok {
		a.counters[md.Key][resID] = make(map[[16]byte]*counterDP)
	}
	if _, ok := a.counters[md.Key][resID][attrID]; !ok {
		a.counters[md.Key][resID][attrID] = newCounterDP(srcAttrs)
	}
	a.counters[md.Key][resID][attrID].Count(int64(count))
	return nil
}

// ValueCount aggregates a span duration into the configured metrics. It
// also takes `adjustedCount` parameter to denote the total number of spans
// in the population that are represented by an individually sampled span.
func (a *Aggregator[K]) ValueCount(
	md model.MetricDef[K],
	resAttrs, srcAttrs pcommon.Map,
	value float64, count uint64,
) error {
	if (md.ValueCountMetric.ExponentialHistogram == nil &&
		md.ValueCountMetric.ExplicitHistogram == nil &&
		md.ValueCountMetric.Summary == nil &&
		md.ValueCountMetric.SumAndCount == nil) ||
		count == 0 {
		// TODO: Add check for all kinds of ds it can handle
		// Nothing to do as the span represents `0` spans or does not
		// define span aggregations.
		return nil
	}

	resID := a.getResourceID(resAttrs)
	attrID := pdatautil.MapHash(srcAttrs)
	if _, ok := a.spanDurations[md.Key]; !ok {
		a.spanDurations[md.Key] = make(map[[16]byte]map[[16]byte]*spanDurationDP)
	}
	if _, ok := a.spanDurations[md.Key][resID]; !ok {
		a.spanDurations[md.Key][resID] = make(map[[16]byte]*spanDurationDP)
	}
	if _, ok := a.spanDurations[md.Key][resID][attrID]; !ok {
		a.spanDurations[md.Key][resID][attrID] = newSpanDurationDP(md.ValueCountMetric, srcAttrs)
	}
	a.spanDurations[md.Key][resID][attrID].Aggregate(value, count)
	return nil
}

// Finalize finalizes the aggregations performed by the aggregator so far into
// the pmetric.Metrics used to create this instance of the aggregator. Finalize
// should be called once per aggregator instance and the aggregator instance
// should not be used after Finalize is called.
func (a *Aggregator[K]) Finalize(mds []model.MetricDef[K]) {
	for _, md := range mds {
		for resID, dpMap := range a.spanDurations[md.Key] {
			metrics := a.smLookup[resID].Metrics()
			var (
				destExpHist      pmetric.ExponentialHistogram
				destExplicitHist pmetric.Histogram
				destSummary      pmetric.Summary
				destSum          pmetric.Sum
				destCount        pmetric.Sum
			)
			if md.ValueCountMetric.ExponentialHistogram != nil {
				destMetric := metrics.AppendEmpty()
				destMetric.SetName(md.Key.Name)
				destMetric.SetDescription(md.Key.Description)
				destExpHist = destMetric.SetEmptyExponentialHistogram()
				destExpHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				destExpHist.DataPoints().EnsureCapacity(len(dpMap))
			}
			if md.ValueCountMetric.ExplicitHistogram != nil {
				destMetric := metrics.AppendEmpty()
				destMetric.SetName(md.Key.Name)
				destMetric.SetDescription(md.Key.Description)
				destExplicitHist = destMetric.SetEmptyHistogram()
				destExplicitHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				destExplicitHist.DataPoints().EnsureCapacity(len(dpMap))
			}
			if md.ValueCountMetric.Summary != nil {
				destMetric := metrics.AppendEmpty()
				destMetric.SetName(md.Key.Name)
				destMetric.SetDescription(md.Key.Description)
				destSummary = destMetric.SetEmptySummary()
				destSummary.DataPoints().EnsureCapacity(len(dpMap))
			}
			if md.ValueCountMetric.SumAndCount != nil {
				destMetricSum := metrics.AppendEmpty()
				// sum_and_count metric for sum
				destMetricSum.SetName(md.Key.Name + md.ValueCountMetric.SumAndCount.SumSuffix)
				destMetricSum.SetDescription(md.Key.Description)
				destSum = destMetricSum.SetEmptySum()
				destSum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				destSum.DataPoints().EnsureCapacity(len(dpMap))
				// sum_and_count metric for count
				destMetricCount := metrics.AppendEmpty()
				destMetricCount.SetName(md.Key.Name + md.ValueCountMetric.SumAndCount.CountSuffix)
				destMetricCount.SetDescription(md.Key.Description)
				destCount = destMetricCount.SetEmptySum()
				destCount.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				destCount.DataPoints().EnsureCapacity(len(dpMap))
			}
			for _, dp := range dpMap {
				dp.Copy(
					a.timestamp,
					destExpHist,
					destExplicitHist,
					destSummary,
					destSum,
					destCount,
				)
			}
		}
		for resID, dpMap := range a.counters[md.Key] {
			if md.Counter == nil {
				continue
			}
			metrics := a.smLookup[resID].Metrics()
			destMetric := metrics.AppendEmpty()
			destMetric.SetName(md.Key.Name)
			destMetric.SetDescription(md.Key.Description)
			destCounter := destMetric.SetEmptySum()
			destCounter.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			destCounter.DataPoints().EnsureCapacity(len(dpMap))
			for _, dp := range dpMap {
				dp.Copy(a.timestamp, destCounter.DataPoints().AppendEmpty())
			}
		}
		// If there are two metric defined with the same key required by metricKey
		// then they will be aggregated within the same metric and produced
		// together. Deleting the key ensures this while preventing duplicates.
		delete(a.spanDurations, md.Key)
	}
}

func (a *Aggregator[K]) getResourceID(resourceAttrs pcommon.Map) [16]byte {
	resID := pdatautil.MapHash(resourceAttrs)
	if _, ok := a.smLookup[resID]; !ok {
		destResourceMetric := a.result.ResourceMetrics().AppendEmpty()
		destResAttrs := destResourceMetric.Resource().Attributes()
		destResAttrs.EnsureCapacity(resourceAttrs.Len() + 1)
		resourceAttrs.CopyTo(destResAttrs)
		destScopeMetric := destResourceMetric.ScopeMetrics().AppendEmpty()
		destScopeMetric.Scope().SetName(metadata.ScopeName)
		a.smLookup[resID] = destScopeMetric
	}
	return resID
}
