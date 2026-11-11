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

// TestMergeOrder pins admission across merge directions. Newer is always
// folded into older, so without budgets the older operand keeps its slot
// whichever way pebble feeds them. With budgets it is "oldest visible operand
// wins": an overflow decided inside a group of operands is final, so fed
// newest first, as pebble does, B stays in overflow although p1 has room once
// the oldest operand is seen; fed oldest first, B is admitted.
func TestMergeOrder(t *testing.T) {
	operand := func(budget, path string) []byte {
		v := budgetTestValue(1)
		mergeMetrics(t, v, pathMetrics(path), budget)
		b, err := v.AppendBinary(nil)
		require.NoError(t, err)
		return b
	}
	older, newer := operand("", "old/s/m/d"), operand("", "new/s/m/d")
	p0A, p1A, p1B := operand("p0", "A/s/m/d"), operand("p1", "A/s/m/d"), operand("p1", "B/s/m/d")

	for _, tc := range []struct {
		name         string
		first        []byte
		run          func(vm pebble.ValueMerger)
		wantAdmitted map[string]int64
		wantOverflow uint64
	}{
		{
			name:         "older_arrives_second",
			first:        newer,
			run:          func(vm pebble.ValueMerger) { require.NoError(t, vm.MergeOlder(older)) },
			wantAdmitted: map[string]int64{"old/s/m/d": 1},
			wantOverflow: 1,
		},
		{
			name:         "newer_arrives_second",
			first:        older,
			run:          func(vm pebble.ValueMerger) { require.NoError(t, vm.MergeNewer(newer)) },
			wantAdmitted: map[string]int64{"old/s/m/d": 1},
			wantOverflow: 1,
		},
		{
			name:  "budgets_newest_first",
			first: p1B,
			run: func(vm pebble.ValueMerger) {
				require.NoError(t, vm.MergeOlder(p1A))
				require.NoError(t, vm.MergeOlder(p0A))
			},
			wantAdmitted: map[string]int64{"A/s/m/d": 2},
			wantOverflow: 1,
		},
		{
			name:  "budgets_oldest_first",
			first: p0A,
			run: func(vm pebble.ValueMerger) {
				require.NoError(t, vm.MergeNewer(p1A))
				require.NoError(t, vm.MergeNewer(p1B))
			},
			wantAdmitted: map[string]int64{"A/s/m/d": 2, "B/s/m/d": 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, s, m, d := budgetTestLimits(1)
			vm, err := NewPebbleMerger(r, s, m, d, defaultMaxBuckets).Merge(nil, tc.first)
			require.NoError(t, err)
			tc.run(vm)
			out, _, err := vm.Finish(true)
			require.NoError(t, err)
			v := budgetTestValue(1)
			require.NoError(t, v.Unmarshal(out))
			md, stats, err := v.Finalize()
			require.NoError(t, err)
			require.Equal(t, tc.wantAdmitted, admittedPaths(md))
			require.Equal(t, tc.wantOverflow, stats.Resources)
		})
	}
}
