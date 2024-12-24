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
	toScope scopeMetrics,
	toMetricID identity.Metric,
	toMetric pmetric.Metric,
	addDP func(scopeMetrics, identity.Metric, pmetric.Metric, DP) (DP, bool),
	temporality pmetric.AggregationTemporality,
) {
	switch temporality {
	case pmetric.AggregationTemporalityCumulative:
		mergeCumulative(from, toScope, toMetricID, toMetric, addDP)
	case pmetric.AggregationTemporalityDelta:
		mergeDelta(from, toScope, toMetricID, toMetric, addDP)
	}
}

type addDPFunc[DP dataPoint[DP]] func(scopeMetrics, identity.Metric, pmetric.Metric, DP) (DP, bool)

func mergeCumulative[DPS dataPointSlice[DP], DP dataPoint[DP]](
	from DPS,
	toScope scopeMetrics,
	toMetricID identity.Metric,
	toMetric pmetric.Metric,
	addDP addDPFunc[DP],
) {
	var zero DP
	for i := 0; i < from.Len(); i++ {
		fromDP := from.At(i)
		toDP, ok := addDP(toScope, toMetricID, toMetric, fromDP)
		if toDP == zero {
			// Overflow, discard the datapoint
			continue
		}
		if ok || fromDP.Timestamp() > toDP.Timestamp() {
			fromDP.CopyTo(toDP)
		}
	}
}

func mergeDelta[DPS dataPointSlice[DP], DP dataPoint[DP]](
	from DPS,
	toScope scopeMetrics,
	toMetricID identity.Metric,
	toMetric pmetric.Metric,
	addDP addDPFunc[DP],
) {
	var zero DP
	for i := 0; i < from.Len(); i++ {
		fromDP := from.At(i)
		toDP, ok := addDP(toScope, toMetricID, toMetric, fromDP)
		if toDP == zero {
			// Overflow, discard the datapoint
			continue
		}
		if ok {
			// New data point is created so we can copy the old data directly
			fromDP.CopyTo(toDP)
			continue
		}

		switch fromDP := any(fromDP).(type) {
		case pmetric.NumberDataPoint:
			mergeDeltaSumDP(fromDP, any(toDP).(pmetric.NumberDataPoint))
		case pmetric.HistogramDataPoint:
			mergeDeltaHistogramDP(fromDP, any(toDP).(pmetric.HistogramDataPoint))
		case pmetric.ExponentialHistogramDataPoint:
			mergeDeltaExponentialHistogramDP(fromDP, any(toDP).(pmetric.ExponentialHistogramDataPoint))
		}
	}
}

func mergeDeltaSumDP(from, to pmetric.NumberDataPoint) {
	toDP := data.Number{NumberDataPoint: to}
	fromDP := data.Number{NumberDataPoint: from}

	toDP.Add(fromDP)
}

func mergeDeltaHistogramDP(from, to pmetric.HistogramDataPoint) {
	if from.Count() == 0 {
		return
	}
	if to.Count() == 0 {
		from.CopyTo(to)
		return
	}

	toDP := data.Histogram{HistogramDataPoint: to}
	fromDP := data.Histogram{HistogramDataPoint: from}

	toDP.Add(fromDP)
}

func mergeDeltaExponentialHistogramDP(from, to pmetric.ExponentialHistogramDataPoint) {
	if from.Count() == 0 {
		return
	}
	if to.Count() == 0 {
		from.CopyTo(to)
		return
	}

	toDP := data.ExpHistogram{DataPoint: to}
	fromDP := data.ExpHistogram{DataPoint: from}

	toDP.Add(fromDP)
}
