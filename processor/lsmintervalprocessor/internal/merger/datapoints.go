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
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/identity"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type dataPointSlice[DP dataPoint[DP]] interface {
	Len() int
	At(i int) DP
	AppendEmpty() DP
}

type dataPoint[Self any] interface {
	pmetric.NumberDataPoint | pmetric.SummaryDataPoint | pmetric.HistogramDataPoint | pmetric.ExponentialHistogramDataPoint

	Timestamp() pcommon.Timestamp
	SetTimestamp(pcommon.Timestamp)
	Attributes() pcommon.Map
	CopyTo(dest Self)
}

func mergeDataPoints[DPS dataPointSlice[DP], DP dataPoint[DP]](
	from DPS,
	toMetricID identity.Metric,
	toMetric pdataMetric,
	addDP func(identity.Metric, pdataMetric, DP) (DP, bool),
	temporality pmetric.AggregationTemporality,
	maxExponentialHistogramBuckets int,
) {
	switch temporality {
	case pmetric.AggregationTemporalityCumulative:
		mergeCumulative(from, toMetricID, toMetric, addDP)
	case pmetric.AggregationTemporalityDelta:
		mergeDelta(from, toMetricID, toMetric, addDP, maxExponentialHistogramBuckets)
	}
}

type addDPFunc[DP dataPoint[DP]] func(identity.Metric, pdataMetric, DP) (DP, bool)

func mergeCumulative[DPS dataPointSlice[DP], DP dataPoint[DP]](
	from DPS,
	toMetricID identity.Metric,
	toMetric pdataMetric,
	addDP addDPFunc[DP],
) {
	for i := 0; i < from.Len(); i++ {
		fromDP := from.At(i)
		toDP, exists := addDP(toMetricID, toMetric, fromDP)
		if exists && fromDP.Timestamp() > toDP.Timestamp() {
			fromDP.CopyTo(toDP)
		}
	}
}

func mergeDelta[DPS dataPointSlice[DP], DP dataPoint[DP]](
	from DPS,
	toMetricID identity.Metric,
	toMetric pdataMetric,
	addDP addDPFunc[DP],
	maxExponentialHistogramBuckets int,
) {
	for i := 0; i < from.Len(); i++ {
		fromDP := from.At(i)
		if toDP, exists := addDP(toMetricID, toMetric, fromDP); exists {
			switch fromDP := any(fromDP).(type) {
			case pmetric.NumberDataPoint:
				mergeDeltaSumDP(fromDP, any(toDP).(pmetric.NumberDataPoint))
			case pmetric.HistogramDataPoint:
				mergeDeltaHistogramDP(fromDP, any(toDP).(pmetric.HistogramDataPoint))
			case pmetric.ExponentialHistogramDataPoint:
				mergeDeltaExponentialHistogramDP(
					fromDP, any(toDP).(pmetric.ExponentialHistogramDataPoint),
					maxExponentialHistogramBuckets,
				)
			}
			toDP.SetTimestamp(fromDP.Timestamp())
		}
	}
}

func mergeDeltaSumDP(from, to pmetric.NumberDataPoint) {
	data.Adder{}.Numbers(to, from)
}

func mergeDeltaHistogramDP(from, to pmetric.HistogramDataPoint) {
	if from.Count() == 0 {
		return
	}
	if to.Count() == 0 {
		from.CopyTo(to)
		return
	}

	data.Adder{}.Histograms(to, from)
}

func mergeDeltaExponentialHistogramDP(
	from, to pmetric.ExponentialHistogramDataPoint,
	maxBuckets int,
) {
	if from.Count() == 0 {
		return
	}
	if to.Count() == 0 {
		from.CopyTo(to)
		return
	}

	data.NewAdder(maxBuckets).Exponential(to, from)
}
