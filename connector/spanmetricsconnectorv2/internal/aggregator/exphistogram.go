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
	"github.com/lightstep/go-expohisto/structure"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// exponentialHistogram is a representation of exponential histogram for
// calculating histograms for span durations.
type exponentialHistogram struct {
	// TODO (lahsivjar): Attribute hash collisions are not considered
	datapoints map[model.MetricKey]map[[16]byte]*exponentialHistogramDP
	timestamp  time.Time
}

func newExponentialHistogram() *exponentialHistogram {
	return &exponentialHistogram{
		datapoints: make(map[model.MetricKey]map[[16]byte]*exponentialHistogramDP),
		timestamp:  time.Now(),
	}
}

func (h *exponentialHistogram) Add(
	key model.MetricKey,
	value float64,
	attributes pcommon.Map,
	histoCfg config.ExponentialHistogram,
) error {
	if _, ok := h.datapoints[key]; !ok {
		h.datapoints[key] = make(map[[16]byte]*exponentialHistogramDP)
	}

	var attrKey [16]byte
	if attributes.Len() > 0 {
		attrKey = pdatautil.MapHash(attributes)
	}

	if _, ok := h.datapoints[key][attrKey]; !ok {
		h.datapoints[key][attrKey] = newExponentialHistogramDP(attributes, histoCfg.MaxSize)
	}

	dp := h.datapoints[key][attrKey]
	dp.Update(value)
	return nil
}

func (h *exponentialHistogram) Move(
	key model.MetricKey,
	dest pmetric.MetricSlice,
) {
	srcDps, ok := h.datapoints[key]
	if !ok || len(srcDps) == 0 {
		return
	}

	destMetric := dest.AppendEmpty()
	destMetric.SetName(key.Name)
	destMetric.SetDescription(key.Description)
	destHist := destMetric.SetEmptyExponentialHistogram()
	destHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	destHist.DataPoints().EnsureCapacity(len(srcDps))
	for _, srcDp := range srcDps {
		destDp := destHist.DataPoints().AppendEmpty()
		srcDp.attrs.CopyTo(destDp.Attributes())
		destDp.SetZeroCount(srcDp.ZeroCount())
		destDp.SetScale(srcDp.Scale())
		destDp.SetCount(srcDp.Count())
		destDp.SetSum(srcDp.Sum())
		if srcDp.Count() > 0 {
			destDp.SetMin(srcDp.Min())
			destDp.SetMax(srcDp.Max())
		}
		// TODO determine appropriate start time
		destDp.SetTimestamp(pcommon.NewTimestampFromTime(h.timestamp))

		copyBucketRange(srcDp.Positive(), destDp.Positive())
		copyBucketRange(srcDp.Negative(), destDp.Negative())
	}
	// If there are two metric defined with the same key required by metricKey
	// then they will be aggregated within the same histogram and produced
	// together. Deleting the key ensures this while preventing duplicates.
	delete(h.datapoints, key)
}

func (h *exponentialHistogram) Size() int {
	return len(h.datapoints)
}

func (h *exponentialHistogram) Reset() {
	clear(h.datapoints)
}

type exponentialHistogramDP struct {
	attrs pcommon.Map

	*structure.Histogram[float64]
}

func newExponentialHistogramDP(attrs pcommon.Map, maxSize int32) *exponentialHistogramDP {
	return &exponentialHistogramDP{
		attrs: attrs,
		Histogram: structure.NewFloat64(
			// If config is not valid then it defaults to the closest
			// valid configuration.
			structure.NewConfig(structure.WithMaxSize(maxSize)),
		),
	}
}

// copyBucketRange copies a bucket range from exponential histogram
// datastructure to the OTel representation.
func copyBucketRange(
	src *structure.Buckets,
	dest pmetric.ExponentialHistogramDataPointBuckets,
) {
	dest.SetOffset(src.Offset())
	dest.BucketCounts().EnsureCapacity(int(src.Len()))
	for i := uint32(0); i < src.Len(); i++ {
		dest.BucketCounts().Append(src.At(i))
	}
}
