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

var noAttributes = [16]byte{}

func newExplicitHistogram(metricDefs map[string]metricDef) *explicitHistogram {
	return &explicitHistogram{
		metricDefs: metricDefs,
		counts:     make(map[string]map[[16]byte]*attrExplicitHistogram, len(metricDefs)),
		timestamp:  time.Now(),
	}
}

type explicitHistogram struct {
	metricDefs map[string]metricDef
	counts     map[string]map[[16]byte]*attrExplicitHistogram
	timestamp  time.Time
}

type attrExplicitHistogram struct {
	attrs pcommon.Map

	sum   float64
	count uint64

	bounds []float64
	counts []uint64
}

func newAttrExplicitHistogram(attrs pcommon.Map, bounds []float64) *attrExplicitHistogram {
	return &attrExplicitHistogram{
		attrs:  attrs,
		bounds: bounds,
		counts: make([]uint64, len(bounds)+1),
	}
}

func (c *explicitHistogram) update(ctx context.Context, attrs pcommon.Map, value float64) error {
	var multiError error
	for name, md := range c.metricDefs {
		countAttrs := pcommon.NewMap()
		for _, attr := range md.attrs {
			if attrVal, ok := attrs.Get(attr.Key); ok {
				switch typeAttr := attrVal.Type(); typeAttr {
				case pcommon.ValueTypeInt:
					countAttrs.PutInt(attr.Key, attrVal.Int())
				case pcommon.ValueTypeDouble:
					countAttrs.PutDouble(attr.Key, attrVal.Double())
				default:
					countAttrs.PutStr(attr.Key, attrVal.Str())
				}
			} else if attr.DefaultValue != nil {
				switch v := attr.DefaultValue.(type) {
				case string:
					if v != "" {
						countAttrs.PutStr(attr.Key, v)
					}
				case int:
					if v != 0 {
						countAttrs.PutInt(attr.Key, int64(v))
					}
				case float64:
					if v != 0 {
						countAttrs.PutDouble(attr.Key, float64(v))
					}
				}
			}
		}

		// Missing necessary attributes to be counted
		if countAttrs.Len() != len(md.attrs) {
			continue
		}
		value /= md.unitDivider
		multiError = errors.Join(multiError, c.increment(name, countAttrs, value, md.histogram))
	}
	return multiError
}

func (c *explicitHistogram) increment(
	metricName string, attrs pcommon.Map, value float64, hCfg HistogramConfig,
) error {
	if _, ok := c.counts[metricName]; !ok {
		c.counts[metricName] = make(map[[16]byte]*attrExplicitHistogram)
	}

	key := noAttributes
	if attrs.Len() > 0 {
		key = pdatautil.MapHash(attrs)
	}

	if _, ok := c.counts[metricName][key]; !ok {
		c.counts[metricName][key] = newAttrExplicitHistogram(attrs, hCfg.Explicit.Buckets)
	}

	hist := c.counts[metricName][key]
	hist.count++
	hist.sum += value
	index := sort.SearchFloat64s(hist.bounds, value)
	hist.counts[index]++
	return nil
}

func (c *explicitHistogram) appendMetricsTo(metricSlice pmetric.MetricSlice) {
	for name, md := range c.metricDefs {
		if len(c.counts[name]) == 0 {
			continue
		}
		destMetric := metricSlice.AppendEmpty()
		destMetric.SetName(name)
		destMetric.SetDescription(md.desc)
		histo := destMetric.SetEmptyHistogram()
		// The delta value is always positive, so a value accumulated downstream is monotonic
		histo.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		histo.DataPoints().EnsureCapacity(len(c.counts[name]))
		for _, dpCount := range c.counts[name] {
			dp := histo.DataPoints().AppendEmpty()
			dpCount.attrs.CopyTo(dp.Attributes())
			dp.ExplicitBounds().FromRaw(dpCount.bounds)
			dp.BucketCounts().FromRaw(dpCount.counts)
			dp.SetCount(dpCount.count)
			dp.SetSum(dpCount.sum)
			// TODO determine appropriate start time
			dp.SetTimestamp(pcommon.NewTimestampFromTime(c.timestamp))
		}
	}
}
