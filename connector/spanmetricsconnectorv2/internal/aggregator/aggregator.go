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

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/config"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/model"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	scopeName            = "otelcol/spanmetricsconnectorv2"
	ephemeralResourceKey = "spanmetricsv2_ephemeral_id"
)

// metricUnitToDivider gives a value that could used to divide the
// nano precision duration to the required unit specified in config.
var metricUnitToDivider = map[config.MetricUnit]float64{
	config.MetricUnitNs: float64(time.Nanosecond.Nanoseconds()),
	config.MetricUnitUs: float64(time.Microsecond.Nanoseconds()),
	config.MetricUnitMs: float64(time.Millisecond.Nanoseconds()),
	config.MetricUnitS:  float64(time.Second.Nanoseconds()),
}

// Aggregator provides a single interface to update all metrics
// datastructures. The required datastructure is selected using
// the metric definition.
type Aggregator struct {
	result      pmetric.Metrics
	ephemeralID string
	// smLookup maps resourceID against scope metrics since the aggregator
	// always produces a single scope.
	smLookup   map[[16]byte]pmetric.ScopeMetrics
	datapoints map[model.MetricKey]map[[16]byte]map[[16]byte]*aggregatorDP
	timestamp  time.Time
}

// NewAggregator creates a new instance of aggregator.
func NewAggregator(metrics pmetric.Metrics, ephemeralID string) *Aggregator {
	return &Aggregator{
		result:      metrics,
		ephemeralID: ephemeralID,
		smLookup:    make(map[[16]byte]pmetric.ScopeMetrics),
		datapoints:  make(map[model.MetricKey]map[[16]byte]map[[16]byte]*aggregatorDP),
		timestamp:   time.Now(),
	}
}

// Add adds a span duration into the configured metrics. It also takes
// `adjustedCount` parameter to denote the total number of spans in the
// population that are represented by an individually sampled span.
// The adjusted count is is calculated as per:
// https://opentelemetry.io/docs/specs/otel/trace/tracestate-probability-sampling/#adjusted-count
func (a *Aggregator) Add(
	md model.MetricDef,
	resAttrs, srcAttrs pcommon.Map,
	spanDuration time.Duration,
	adjustedCount uint64,
) error {
	if adjustedCount == 0 {
		// Nothing to do as the span represents `0` spans
		return nil
	}

	srcAttrs = getFilteredAttributes(srcAttrs, md.Attributes)
	// If all the configured attributes are not present in source
	// metric then don't count them.
	if srcAttrs.Len() != len(md.Attributes) {
		return nil
	}
	attrID := pdatautil.MapHash(srcAttrs)

	if len(md.IncludeResourceAttributes) > 0 {
		resAttrs = getFilteredAttributes(resAttrs, md.IncludeResourceAttributes)
	}
	resID := pdatautil.MapHash(resAttrs)
	if _, ok := a.smLookup[resID]; !ok {
		destResourceMetric := a.result.ResourceMetrics().AppendEmpty()
		destResAttrs := destResourceMetric.Resource().Attributes()
		destResAttrs.EnsureCapacity(resAttrs.Len() + 1)
		resAttrs.CopyTo(destResAttrs)
		if md.EphemeralResourceAttribute {
			destResAttrs.PutStr(ephemeralResourceKey, a.ephemeralID)
		}
		destScopeMetric := destResourceMetric.ScopeMetrics().AppendEmpty()
		destScopeMetric.Scope().SetName(scopeName)
		a.smLookup[resID] = destScopeMetric
	}

	if _, ok := a.datapoints[md.Key]; !ok {
		a.datapoints[md.Key] = make(map[[16]byte]map[[16]byte]*aggregatorDP)
	}
	if _, ok := a.datapoints[md.Key][resID]; !ok {
		a.datapoints[md.Key][resID] = make(map[[16]byte]*aggregatorDP)
	}
	if _, ok := a.datapoints[md.Key][resID][attrID]; !ok {
		a.datapoints[md.Key][resID][attrID] = newAggregatorDP(md, srcAttrs)
	}
	value := float64(spanDuration.Nanoseconds()) / metricUnitToDivider[md.Unit]
	a.datapoints[md.Key][resID][attrID].Add(value, adjustedCount)
	return nil
}

// Finalize finalizes the aggregations performed by the aggregator so far into
// the pmetric.Metrics used to create this instance of the aggregator. Finalize
// should be called once per aggregator instance and the aggregator instance
// should not be used after Finalize is called.
func (a *Aggregator) Finalize(mds []model.MetricDef) {
	for _, md := range mds {
		for resID, dpMap := range a.datapoints[md.Key] {
			metrics := a.smLookup[resID].Metrics()
			var (
				destExpHist       pmetric.ExponentialHistogram
				destExplicitHist  pmetric.Histogram
				destSummary       pmetric.Summary
				destCountersSum   pmetric.Sum
				destCountersCount pmetric.Sum
			)
			if md.ExponentialHistogram != nil {
				destMetric := metrics.AppendEmpty()
				destMetric.SetName(md.Key.Name)
				destMetric.SetDescription(md.Key.Description)
				destExpHist = destMetric.SetEmptyExponentialHistogram()
				destExpHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				destExpHist.DataPoints().EnsureCapacity(len(dpMap))
			}
			if md.ExplicitHistogram != nil {
				destMetric := metrics.AppendEmpty()
				destMetric.SetName(md.Key.Name)
				destMetric.SetDescription(md.Key.Description)
				destExplicitHist = destMetric.SetEmptyHistogram()
				destExplicitHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				destExplicitHist.DataPoints().EnsureCapacity(len(dpMap))
			}
			if md.Summary != nil {
				destMetric := metrics.AppendEmpty()
				destMetric.SetName(md.Key.Name)
				destMetric.SetDescription(md.Key.Description)
				destSummary = destMetric.SetEmptySummary()
				destSummary.DataPoints().EnsureCapacity(len(dpMap))
			}
			if md.Counters != nil {
				destMetricSum := metrics.AppendEmpty()
				// counter metric for sum
				destMetricSum.SetName(md.Key.Name + md.Counters.SumSuffix)
				destMetricSum.SetDescription(md.Key.Description)
				destCountersSum = destMetricSum.SetEmptySum()
				destCountersSum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				destCountersSum.DataPoints().EnsureCapacity(len(dpMap))
				// counter metric for count
				destMetricCount := metrics.AppendEmpty()
				destMetricCount.SetName(md.Key.Name + md.Counters.CountSuffix)
				destMetricCount.SetDescription(md.Key.Description)
				destCountersCount = destMetricCount.SetEmptySum()
				destCountersCount.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				destCountersCount.DataPoints().EnsureCapacity(len(dpMap))
			}
			for _, dp := range dpMap {
				dp.Copy(
					a.timestamp,
					destExpHist,
					destExplicitHist,
					destSummary,
					destCountersSum,
					destCountersCount,
				)
			}
		}
		// If there are two metric defined with the same key required by metricKey
		// then they will be aggregated within the same histogram and produced
		// together. Deleting the key ensures this while preventing duplicates.
		delete(a.datapoints, md.Key)
	}
}

type aggregatorDP struct {
	expHistogramDP      *exponentialHistogramDP
	explicitHistogramDP *explicitHistogramDP
	summaryDP           *summaryDP
	countersDP          *countersDP
}

func newAggregatorDP(
	md model.MetricDef,
	attrs pcommon.Map,
) *aggregatorDP {
	var dp aggregatorDP
	if md.ExponentialHistogram != nil {
		dp.expHistogramDP = newExponentialHistogramDP(
			attrs, md.ExponentialHistogram.MaxSize,
		)
	}
	if md.ExplicitHistogram != nil {
		dp.explicitHistogramDP = newExplicitHistogramDP(
			attrs, md.ExplicitHistogram.Buckets,
		)
	}
	if md.Summary != nil {
		dp.summaryDP = newSummaryDP(attrs)
	}
	if md.Counters != nil {
		dp.countersDP = newCountersDP(attrs)
	}
	return &dp
}

func (dp *aggregatorDP) Add(value float64, count uint64) {
	if dp.expHistogramDP != nil {
		dp.expHistogramDP.Add(value, count)
	}
	if dp.explicitHistogramDP != nil {
		dp.explicitHistogramDP.Add(value, count)
	}
	if dp.summaryDP != nil {
		dp.summaryDP.Add(value, count)
	}
	if dp.countersDP != nil {
		dp.countersDP.Add(value, count)
	}
}

func (dp *aggregatorDP) Copy(
	timestamp time.Time,
	destExpHist pmetric.ExponentialHistogram,
	destExplicitHist pmetric.Histogram,
	destSummary pmetric.Summary,
	destCountersSum pmetric.Sum,
	destCountersCount pmetric.Sum,
) {
	if dp.expHistogramDP != nil {
		dp.expHistogramDP.Copy(timestamp, destExpHist.DataPoints().AppendEmpty())
	}
	if dp.explicitHistogramDP != nil {
		dp.explicitHistogramDP.Copy(timestamp, destExplicitHist.DataPoints().AppendEmpty())
	}
	if dp.summaryDP != nil {
		dp.summaryDP.Copy(timestamp, destSummary.DataPoints().AppendEmpty())
	}
	if dp.countersDP != nil {
		dp.countersDP.Copy(
			timestamp,
			destCountersSum.DataPoints().AppendEmpty(),
			destCountersCount.DataPoints().AppendEmpty(),
		)
	}
}

func getFilteredAttributes(attrs pcommon.Map, filters []model.AttributeKeyValue) pcommon.Map {
	filteredAttrs := pcommon.NewMap()
	for _, filter := range filters {
		if attr, ok := attrs.Get(filter.Key); ok {
			attr.CopyTo(filteredAttrs.PutEmpty(filter.Key))
			continue
		}
		if filter.DefaultValue.Type() != pcommon.ValueTypeEmpty {
			filter.DefaultValue.CopyTo(filteredAttrs.PutEmpty(filter.Key))
		}
	}
	return filteredAttrs
}
