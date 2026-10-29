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
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data"
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

// The functions below operate on datapoints held as any, which is one of
// the four pmetric datapoint types.

func newDP[DP dataPoint[DP]]() DP {
	var zero DP
	return newDPLike(zero).(DP)
}

// newDPLike returns a new, empty datapoint of the same type as like.
func newDPLike(like any) any {
	switch like.(type) {
	case pmetric.NumberDataPoint:
		return pmetric.NewNumberDataPoint()
	case pmetric.HistogramDataPoint:
		return pmetric.NewHistogramDataPoint()
	case pmetric.ExponentialHistogramDataPoint:
		return pmetric.NewExponentialHistogramDataPoint()
	case pmetric.SummaryDataPoint:
		return pmetric.NewSummaryDataPoint()
	default:
		panic(fmt.Sprintf("unexpected datapoint type %T", like))
	}
}

// cloneDP returns a copy of dp without attributes, or nil if dp is nil.
func cloneDP(dp any) any {
	switch dp := dp.(type) {
	case nil:
		return nil
	case pmetric.NumberDataPoint:
		c := pmetric.NewNumberDataPoint()
		dp.CopyTo(c)
		c.Attributes().Clear()
		return c
	case pmetric.HistogramDataPoint:
		c := pmetric.NewHistogramDataPoint()
		dp.CopyTo(c)
		c.Attributes().Clear()
		return c
	case pmetric.ExponentialHistogramDataPoint:
		c := pmetric.NewExponentialHistogramDataPoint()
		dp.CopyTo(c)
		c.Attributes().Clear()
		return c
	case pmetric.SummaryDataPoint:
		c := pmetric.NewSummaryDataPoint()
		dp.CopyTo(c)
		c.Attributes().Clear()
		return c
	default:
		panic(fmt.Sprintf("unexpected datapoint type %T", dp))
	}
}

func dpTimestamp(dp any) pcommon.Timestamp {
	switch dp := dp.(type) {
	case pmetric.NumberDataPoint:
		return dp.Timestamp()
	case pmetric.HistogramDataPoint:
		return dp.Timestamp()
	case pmetric.ExponentialHistogramDataPoint:
		return dp.Timestamp()
	case pmetric.SummaryDataPoint:
		return dp.Timestamp()
	default:
		panic(fmt.Sprintf("unexpected datapoint type %T", dp))
	}
}

func setDPTimestamp(dp any, ts pcommon.Timestamp) {
	switch dp := dp.(type) {
	case pmetric.NumberDataPoint:
		dp.SetTimestamp(ts)
	case pmetric.HistogramDataPoint:
		dp.SetTimestamp(ts)
	case pmetric.ExponentialHistogramDataPoint:
		dp.SetTimestamp(ts)
	case pmetric.SummaryDataPoint:
		dp.SetTimestamp(ts)
	default:
		panic(fmt.Sprintf("unexpected datapoint type %T", dp))
	}
}

// appendDP encodes dp, without its attributes, and appends it to b.
func appendDP(b []byte, dp any) []byte {
	switch dp := dp.(type) {
	case pmetric.NumberDataPoint:
		return appendNumberDP(b, dp)
	case pmetric.HistogramDataPoint:
		return appendHistogramDP(b, dp)
	case pmetric.ExponentialHistogramDataPoint:
		return appendExpHistogramDP(b, dp)
	case pmetric.SummaryDataPoint:
		return appendSummaryDP(b, dp)
	default:
		panic(fmt.Sprintf("unexpected datapoint type %T", dp))
	}
}

// decodeDP decodes payload into dp, an empty datapoint of the payload's
// type.
func decodeDP(payload []byte, dp any) error {
	var err error
	switch dp := dp.(type) {
	case pmetric.NumberDataPoint:
		err = decodeNumberDP(payload, dp)
	case pmetric.HistogramDataPoint:
		err = decodeHistogramDP(payload, dp)
	case pmetric.ExponentialHistogramDataPoint:
		err = decodeExpHistogramDP(payload, dp)
	case pmetric.SummaryDataPoint:
		err = decodeSummaryDP(payload, dp)
	default:
		panic(fmt.Sprintf("unexpected datapoint type %T", dp))
	}
	if err != nil {
		return fmt.Errorf("failed to decode datapoint: %w", err)
	}
	return nil
}

// decodeNewDP decodes payload into a new datapoint for a metric of type t.
func decodeNewDP(t pmetric.MetricType, payload []byte) (any, error) {
	var dp any
	switch t {
	case pmetric.MetricTypeSum:
		dp = pmetric.NewNumberDataPoint()
	case pmetric.MetricTypeHistogram:
		dp = pmetric.NewHistogramDataPoint()
	case pmetric.MetricTypeExponentialHistogram:
		dp = pmetric.NewExponentialHistogramDataPoint()
	case pmetric.MetricTypeSummary:
		dp = pmetric.NewSummaryDataPoint()
	default:
		return nil, fmt.Errorf("unsupported metric type: %s", t)
	}
	return dp, decodeDP(payload, dp)
}

// mergeDeltaDP merges from into to, two delta datapoints of the same type.
func mergeDeltaDP(from, to any, maxExponentialHistogramBuckets int) error {
	switch from := from.(type) {
	case pmetric.NumberDataPoint:
		return mergeDeltaSumDP(from, to.(pmetric.NumberDataPoint))
	case pmetric.HistogramDataPoint:
		return mergeDeltaHistogramDP(from, to.(pmetric.HistogramDataPoint))
	case pmetric.ExponentialHistogramDataPoint:
		return mergeDeltaExponentialHistogramDP(
			from, to.(pmetric.ExponentialHistogramDataPoint),
			maxExponentialHistogramBuckets,
		)
	default:
		return nil
	}
}

func mergeDeltaSumDP(from, to pmetric.NumberDataPoint) error {
	if err := (data.Adder{}).Numbers(to, from); err != nil {
		return fmt.Errorf("failed to merge sum datapoint: %w", err)
	}
	return nil
}

func mergeDeltaHistogramDP(from, to pmetric.HistogramDataPoint) error {
	if from.Count() == 0 {
		return nil
	}
	if to.Count() == 0 {
		from.CopyTo(to)
		return nil
	}

	if err := (data.Adder{}).Histograms(to, from); err != nil {
		return fmt.Errorf("failed to merge histogram datapoint: %w", err)
	}
	return nil
}

func mergeDeltaExponentialHistogramDP(
	from, to pmetric.ExponentialHistogramDataPoint,
	maxBuckets int,
) error {
	if from.Count() == 0 {
		return nil
	}
	if to.Count() == 0 {
		from.CopyTo(to)
		return nil
	}

	if err := data.NewAdder(maxBuckets).Exponential(to, from); err != nil {
		return fmt.Errorf("failed to merge exponential histogram datapoint: %w", err)
	}
	return nil
}
