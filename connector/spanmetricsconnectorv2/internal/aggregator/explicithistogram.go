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
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type explicitHistogramDP struct {
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

func newExplicitHistogramDP(attrs pcommon.Map, bounds []float64) *explicitHistogramDP {
	return &explicitHistogramDP{
		attrs:  attrs,
		bounds: bounds,
		counts: make([]uint64, len(bounds)+1),
	}
}

func (dp *explicitHistogramDP) Aggregate(value float64, count uint64) {
	dp.sum += value * float64(count)
	dp.count += count
	dp.counts[sort.SearchFloat64s(dp.bounds, value)] += count
}

func (dp *explicitHistogramDP) Copy(
	timestamp time.Time,
	dest pmetric.HistogramDataPoint,
) {
	dp.attrs.CopyTo(dest.Attributes())
	dest.ExplicitBounds().FromRaw(dp.bounds)
	dest.BucketCounts().FromRaw(dp.counts)
	dest.SetCount(dp.count)
	dest.SetSum(dp.sum)
	// TODO determine appropriate start time
	dest.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
}
