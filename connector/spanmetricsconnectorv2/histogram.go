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

package spanmetricsconnectorv2 // import "github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2"

import (
	"context"
	"errors"
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
)

var (
	noAttributes = [16]byte{}

	// metricUnitToDivider gives a value that could used to divide the
	// nano precision duration to the required unit specified in config.
	metricUnitToDivider = map[MetricUnit]float64{
		MetricUnitNs: float64(time.Nanosecond.Nanoseconds()),
		MetricUnitUs: float64(time.Microsecond.Nanoseconds()),
		MetricUnitMs: float64(time.Millisecond.Nanoseconds()),
		MetricUnitS:  float64(time.Second.Nanoseconds()),
	}
)

func newExplicitHistogram(metricDefs []metricDef) *explicitHistogram {
	return &explicitHistogram{
		metricDefs: metricDefs,
		data:       make(map[explicitHistogramKey]map[[16]byte]*attrExplicitHistogram, len(metricDefs)),
		timestamp:  time.Now(),
	}
}

type explicitHistogram struct {
	metricDefs []metricDef
	data       map[explicitHistogramKey]map[[16]byte]*attrExplicitHistogram
	timestamp  time.Time
}

type explicitHistogramKey struct {
	Name string
	Desc string
}

type attrExplicitHistogram struct {
	attrs pcommon.Map

	sum   float64
	count uint64

	// bounds represents the explicitly defined boundaries for the histogram
	// bucket. The boundaries for a bucket at index i are:
	//
	// (-Inf, bounds[i]] for i == 0
	// (bounds[i-1], bounds[i]] for 0 < i < len(bounds)
	// (bounds[i-1], +Inf) for i == len(bounds)
	//
	// Based on above representation, a bounds of length n represents n+1 buckets.
	bounds []float64

	// counts represents the count values of histogram for each bucket. The sum of
	// counts across all buckets must be equal to the count variable. The length of
	// counts must be one greather than the length of bounds slice.
	counts []uint64
}

func newAttrExplicitHistogram(attrs pcommon.Map, bounds []float64) *attrExplicitHistogram {
	return &attrExplicitHistogram{
		attrs:  attrs,
		bounds: bounds,
		counts: make([]uint64, len(bounds)+1),
	}
}

func (c *explicitHistogram) update(ctx context.Context, attrs pcommon.Map, value time.Duration) error {
	var multiError error
	for _, md := range c.metricDefs {
		definedAttrs := pcommon.NewMap()
		for _, definedAttr := range md.Attributes {
			if attrVal, ok := attrs.Get(definedAttr.Key); ok {
				attrVal.CopyTo(definedAttrs.PutEmpty(definedAttr.Key))
			} else if definedAttr.DefaultValue.Type() != pcommon.ValueTypeEmpty {
				definedAttr.DefaultValue.CopyTo(definedAttrs.PutEmpty(definedAttr.Key))
			}
		}

		// Missing necessary attributes to be counted
		if definedAttrs.Len() != len(md.Attributes) {
			continue
		}
		finalValue := float64(value.Nanoseconds()) / metricUnitToDivider[md.Unit]
		multiError = errors.Join(multiError, c.increment(md.Name, md.Description, definedAttrs, finalValue, md.Histogram))
	}
	return multiError
}

func (c *explicitHistogram) increment(
	name, desc string, attrs pcommon.Map, value float64, hCfg HistogramConfig,
) error {
	key := explicitHistogramKey{Name: name, Desc: desc}
	if _, ok := c.data[key]; !ok {
		c.data[key] = make(map[[16]byte]*attrExplicitHistogram)
	}

	attrKey := noAttributes
	if attrs.Len() > 0 {
		attrKey = pdatautil.MapHash(attrs)
	}

	if _, ok := c.data[key][attrKey]; !ok {
		c.data[key][attrKey] = newAttrExplicitHistogram(attrs, hCfg.Explicit.Buckets)
	}

	hist := c.data[key][attrKey]
	hist.count++
	hist.sum += value
	index := sort.SearchFloat64s(hist.bounds, value)
	hist.counts[index]++
	return nil
}

func (c *explicitHistogram) appendMetricsTo(metricSlice pmetric.MetricSlice) {
	metricSlice.EnsureCapacity(len(c.data))
	for _, md := range c.metricDefs {
		key := explicitHistogramKey{Name: md.Name, Desc: md.Description}
		if len(c.data[key]) == 0 {
			continue
		}
		destMetric := metricSlice.AppendEmpty()
		destMetric.SetName(md.Name)
		destMetric.SetDescription(md.Description)
		histo := destMetric.SetEmptyHistogram()
		// The delta value is always positive, so a value accumulated downstream is monotonic
		histo.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		histo.DataPoints().EnsureCapacity(len(c.data[key]))
		for _, dpCount := range c.data[key] {
			dp := histo.DataPoints().AppendEmpty()
			dpCount.attrs.CopyTo(dp.Attributes())
			dp.ExplicitBounds().FromRaw(dpCount.bounds)
			dp.BucketCounts().FromRaw(dpCount.counts)
			dp.SetCount(dpCount.count)
			dp.SetSum(dpCount.sum)
			// TODO determine appropriate start time
			dp.SetTimestamp(pcommon.NewTimestampFromTime(c.timestamp))
		}
		// Deleting the key prevents duplicates if same metric name and definition
		// is used with 2 different metric definitions while putting them together
		// in the same metric slice.
		delete(c.data, key)
	}
}
