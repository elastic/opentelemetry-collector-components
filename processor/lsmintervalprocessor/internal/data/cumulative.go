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

package data

import (
	"bytes"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"sort"
)

// PreferCumulative breaks conflicting equal-timestamp samples by the
// lexicographically greater canonical OTLP encoding, excluding stream
// attributes. Unlike arrival order this is stable across buffering, replay,
// and Pebble's merge directions. Call only when timestamps are equal.
func PreferCumulative(from, to any) bool {
	return bytes.Compare(cumulativeBytes(from), cumulativeBytes(to)) > 0
}

func cumulativeBytes(dp any) []byte {
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	var exemplars pmetric.ExemplarSlice
	switch dp := dp.(type) {
	case pmetric.NumberDataPoint:
		dst := m.SetEmptySum().DataPoints().AppendEmpty()
		dp.CopyTo(dst)
		dst.Attributes().Clear()
		exemplars = dst.Exemplars()
	case pmetric.HistogramDataPoint:
		dst := m.SetEmptyHistogram().DataPoints().AppendEmpty()
		dp.CopyTo(dst)
		dst.Attributes().Clear()
		exemplars = dst.Exemplars()
	case pmetric.ExponentialHistogramDataPoint:
		dst := m.SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
		dp.CopyTo(dst)
		dst.Attributes().Clear()
		exemplars = dst.Exemplars()
	case pmetric.SummaryDataPoint:
		dst := m.SetEmptySummary().DataPoints().AppendEmpty()
		dp.CopyTo(dst)
		dst.Attributes().Clear()
	default:
		panic("unsupported cumulative datapoint")
	}
	if exemplars != (pmetric.ExemplarSlice{}) {
		for i := 0; i < exemplars.Len(); i++ {
			canonicalMap(exemplars.At(i).FilteredAttributes())
		}
	}
	// ProtoMarshaler cannot fail for these supported pdata values.
	b, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	if err != nil {
		panic(err)
	}
	return b
}

func canonicalMap(m pcommon.Map) {
	keys := make([]string, 0, m.Len())
	m.Range(func(k string, v pcommon.Value) bool { keys = append(keys, k); canonicalValue(v); return true })
	sort.Strings(keys)
	copyMap := pcommon.NewMap()
	for _, k := range keys {
		v, _ := m.Get(k)
		v.CopyTo(copyMap.PutEmpty(k))
	}
	copyMap.MoveTo(m)
}

func canonicalValue(v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeMap:
		canonicalMap(v.Map())
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		for i := 0; i < s.Len(); i++ {
			canonicalValue(s.At(i))
		}
	}
}
