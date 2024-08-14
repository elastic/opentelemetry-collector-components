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
	datapoints map[model.MetricKey]map[[16]byte]*aggregatorDP
	timestamp  time.Time
}

// NewAggregator creates a new instance of aggregator.
func NewAggregator() *Aggregator {
	return &Aggregator{
		datapoints: make(map[model.MetricKey]map[[16]byte]*aggregatorDP),
		timestamp:  time.Now(),
	}
}

// Add adds a span duration into the configured metrics.
func (a *Aggregator) Add(
	md model.MetricDef,
	srcAttrs pcommon.Map,
	spanDuration time.Duration,
) error {
	filteredAttrs := pcommon.NewMap()
	for _, definedAttr := range md.Attributes {
		if srcAttr, ok := srcAttrs.Get(definedAttr.Key); ok {
			srcAttr.CopyTo(filteredAttrs.PutEmpty(definedAttr.Key))
			continue
		}
		if definedAttr.DefaultValue.Type() != pcommon.ValueTypeEmpty {
			definedAttr.DefaultValue.CopyTo(filteredAttrs.PutEmpty(definedAttr.Key))
		}
	}

	// If all the configured attributes are not present in source
	// metric then don't count them.
	if filteredAttrs.Len() != len(md.Attributes) {
		return nil
	}

	if _, ok := a.datapoints[md.Key]; !ok {
		a.datapoints[md.Key] = make(map[[16]byte]*aggregatorDP)
	}

	var attrKey [16]byte
	if filteredAttrs.Len() > 0 {
		attrKey = pdatautil.MapHash(filteredAttrs)
	}

	if _, ok := a.datapoints[md.Key][attrKey]; !ok {
		a.datapoints[md.Key][attrKey] = newAggregatorDP(md, filteredAttrs)
	}
	value := float64(spanDuration.Nanoseconds()) / metricUnitToDivider[md.Unit]
	a.datapoints[md.Key][attrKey].Add(value)
	return nil
}

// Move moves the metrics for a given metric definition to a metric slice.
// Note that move also deletes the the cached data after moving.
func (a *Aggregator) Move(
	md model.MetricDef,
	dest pmetric.MetricSlice,
) {
	srcDPs, ok := a.datapoints[md.Key]
	if !ok || len(srcDPs) == 0 {
		return
	}

	var (
		destExpHist      pmetric.ExponentialHistogram
		destExplicitHist pmetric.Histogram
		destSummary      pmetric.Summary
	)
	if md.ExponentialHistogram != nil {
		destMetric := dest.AppendEmpty()
		destMetric.SetName(md.Key.Name)
		destMetric.SetDescription(md.Key.Description)
		destExpHist = destMetric.SetEmptyExponentialHistogram()
		destExpHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		destExpHist.DataPoints().EnsureCapacity(len(srcDPs))
	}
	if md.ExplicitHistogram != nil {
		destMetric := dest.AppendEmpty()
		destMetric.SetName(md.Key.Name)
		destMetric.SetDescription(md.Key.Description)
		destExplicitHist = destMetric.SetEmptyHistogram()
		destExplicitHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		destExplicitHist.DataPoints().EnsureCapacity(len(srcDPs))
	}
	if md.Summary != nil {
		destMetric := dest.AppendEmpty()
		destMetric.SetName(md.Key.Name)
		destMetric.SetDescription(md.Key.Description)
		destSummary = destMetric.SetEmptySummary()
		destSummary.DataPoints().EnsureCapacity(len(srcDPs))
	}

	for _, srcDP := range srcDPs {
		srcDP.Copy(a.timestamp, destExpHist, destExplicitHist, destSummary)
	}

	// If there are two metric defined with the same key required by metricKey
	// then they will be aggregated within the same histogram and produced
	// together. Deleting the key ensures this while preventing duplicates.
	delete(a.datapoints, md.Key)
}

// Empty returns true if there are no aggregations available.
func (a *Aggregator) Empty() bool {
	return len(a.datapoints) == 0
}

// Reset resets the aggregator for another usage.
func (a *Aggregator) Reset() {
	clear(a.datapoints)
}

type aggregatorDP struct {
	expHistogramDP      *exponentialHistogramDP
	explicitHistogramDP *explicitHistogramDP
	summaryDP           *summaryDP
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
	return &dp
}

func (dp *aggregatorDP) Add(value float64) {
	if dp.expHistogramDP != nil {
		dp.expHistogramDP.Add(value)
	}
	if dp.explicitHistogramDP != nil {
		dp.explicitHistogramDP.Add(value)
	}
	if dp.summaryDP != nil {
		dp.summaryDP.Add(value)
	}
}

func (dp *aggregatorDP) Copy(
	timestamp time.Time,
	destExpHist pmetric.ExponentialHistogram,
	destExplicitHist pmetric.Histogram,
	destSummary pmetric.Summary,
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
}
