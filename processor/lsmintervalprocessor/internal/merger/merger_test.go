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

package merger

import (
	"slices"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
)

func TestMergeFinishLone(t *testing.T) {
	op := mustFatLSMOperand(t, fatLSMShape{resources: 4, dps: 2})
	out, _ := compactFinish(t, op)
	require.Equal(t, op, out)
}

func TestMergeFinishCombine(t *testing.T) {
	baseShape := fatLSMShape{resources: 4, dps: 2}
	sibShape := fatLSMShape{resources: 2, dps: 2}
	base := mustFatLSMOperand(t, baseShape)
	sib := mustFatLSMOperand(t, sibShape)
	out, _ := compactFinish(t, base, sib)
	require.NotEqual(t, base, out)

	v := peakValue()
	require.NoError(t, v.Unmarshal(out))
	md, _, err := v.Finalize()
	require.NoError(t, err)

	// peakCompactMetrics sets each expo datapoint count to 1000.
	// Overlapping identities add those counts.
	want := uint64((baseShape.resources*baseShape.dps + sibShape.resources*sibShape.dps) * 1000)
	require.Equal(t, want, expoHistogramCount(md))
}

func TestMergeFinishCopiesOperand(t *testing.T) {
	op := mustFatLSMOperand(t, fatLSMShape{resources: 2, dps: 1})
	orig := slices.Clone(op)

	vm, err := peakPebbleMerger().Merge(nil, op)
	require.NoError(t, err)
	op[0] ^= 0xff

	out, _, err := vm.Finish(true)
	require.NoError(t, err)
	require.Equal(t, orig, out)
}

func expoHistogramCount(md pmetric.Metrics) uint64 {
	var n uint64
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			for _, m := range sms.At(j).Metrics().All() {
				if m.Type() != pmetric.MetricTypeExponentialHistogram {
					continue
				}
				for _, dp := range m.ExponentialHistogram().DataPoints().All() {
					n += dp.Count()
				}
			}
		}
	}
	return n
}

// TestMergeOrder pins admission across merge directions: operands are folded
// oldest first at Finish whichever direction pebble delivers them, so at a
// resource limit of 1 the older resource keeps its slot and the newer one
// overflows.
func TestMergeOrder(t *testing.T) {
	limit := config.LimitConfig{
		MaxCardinality: 1,
		Overflow:       config.OverflowConfig{Attributes: []config.Attribute{{Key: "overflow", Value: true}}},
	}
	operand := func(name string) []byte {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("name", name)
		m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		m.Sum().DataPoints().AppendEmpty().SetIntValue(1)
		v := NewValue(limit, limit, limit, limit, defaultMaxBuckets)
		require.NoError(t, updateValueWithPMetrics(md, v))
		b, err := v.AppendBinary(nil)
		require.NoError(t, err)
		return b
	}
	older, newer := operand("old"), operand("new")

	for _, tc := range []struct {
		name  string
		first []byte
		run   func(vm pebble.ValueMerger)
	}{
		{name: "older_arrives_second", first: newer, run: func(vm pebble.ValueMerger) { require.NoError(t, vm.MergeOlder(older)) }},
		{name: "newer_arrives_second", first: older, run: func(vm pebble.ValueMerger) { require.NoError(t, vm.MergeNewer(newer)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vm, err := NewPebbleMerger(limit, limit, limit, limit, defaultMaxBuckets).Merge(nil, tc.first)
			require.NoError(t, err)
			tc.run(vm)
			out, _, err := vm.Finish(true)
			require.NoError(t, err)
			v := NewValue(limit, limit, limit, limit, defaultMaxBuckets)
			require.NoError(t, v.Unmarshal(out))
			md, stats, err := v.Finalize()
			require.NoError(t, err)
			require.Equal(t, uint64(1), stats.Resources)
			var names []string
			for _, rm := range md.ResourceMetrics().All() {
				if n, ok := rm.Resource().Attributes().Get("name"); ok {
					names = append(names, n.Str())
				}
			}
			require.Equal(t, []string{"old"}, names)
		})
	}
}
