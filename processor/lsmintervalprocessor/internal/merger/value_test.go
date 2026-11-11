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
	"encoding/binary"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	defaultMaxBuckets = 160
)

func TestMergeMetric(t *testing.T) {
	for _, tc := range []struct {
		name              string
		expectedOverflows OverflowStats
	}{
		{name: "empty"},
		{name: "single_metric"},
		{
			name:              "all_overflow",
			expectedOverflows: OverflowStats{Resources: 1, Scopes: 1, Metrics: 1, Datapoints: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := getTestValue(t)
			dir := filepath.Join("../../testdata/merger", tc.name)
			md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)
			mergeMetrics(t, v, md, "")

			// Do a binary<>merger#Value conversion to assert the binary marshaling logic
			vb, err := v.AppendBinary(nil)
			require.NoError(t, err)
			v = getTestValue(t)
			require.NoError(t, v.Unmarshal(vb))

			// Merging the value into an empty value, as the pebble merger does
			// with stored operands, must not change the result: overflow
			// buckets carried by the operand are neither counted against the
			// limits nor estimated as overflows.
			merged := getTestValue(t)
			require.NoError(t, merged.Merge(v))

			// Compare the final metric with the expected metric
			expected, err := golden.ReadMetrics(filepath.Join(dir, "output.yaml"))
			require.NoError(t, err)
			for name, v := range map[string]*Value{"unmarshaled": v, "merged": merged} {
				actual, overflowStats, err := v.Finalize()
				require.NoError(t, err, name)
				assert.NoError(t, pmetrictest.CompareMetrics(expected, actual), name)
				assert.Equal(t, tc.expectedOverflows, overflowStats, name)
			}
		})
	}
}

func getTestValue(t *testing.T) *Value {
	t.Helper()

	return NewValue(
		config.LimitConfig{
			MaxCardinality: 1,
			Overflow: config.OverflowConfig{
				Attributes: []config.Attribute{{Key: "resource_overflow", Value: true}},
			},
		},
		config.LimitConfig{
			MaxCardinality: 1,
			Overflow: config.OverflowConfig{
				Attributes: []config.Attribute{{Key: "scope_overflow", Value: true}},
			},
		},
		config.LimitConfig{
			MaxCardinality: 1,
			Overflow: config.OverflowConfig{
				Attributes: []config.Attribute{{Key: "metric_overflow", Value: true}},
			},
		},
		config.LimitConfig{
			MaxCardinality: 1,
			Overflow: config.OverflowConfig{
				Attributes: []config.Attribute{{Key: "dp_overflow", Value: true}},
			},
		},
		defaultMaxBuckets,
	)
}

func mergeMetrics(t *testing.T, v *Value, md pmetric.Metrics, budget string) {
	t.Helper()

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				require.NoError(t, v.MergeMetric(rm, sm, ms.At(k), budget))
			}
		}
	}
}

// we use the overflow test files but overflow behaviour will be governed
// by the limit configurations used in the tests/benchmarks.
var benchTestCases = []string{
	"sum_cumulative_overflow",
	"sum_delta_overflow",
	"histogram_cumulative_overflow",
	"histogram_delta_overflow",
	"exphistogram_cumulative_overflow",
	"exphistogram_delta_overflow",
}

func BenchmarkMerge(b *testing.B) {
	for _, tc := range []struct {
		name        string
		resLimit    config.LimitConfig
		scopeLimit  config.LimitConfig
		metricLimit config.LimitConfig
		dpLimit     config.LimitConfig
	}{
		{
			name:        "without_overflow",
			resLimit:    config.LimitConfig{MaxCardinality: math.MaxInt64},
			scopeLimit:  config.LimitConfig{MaxCardinality: math.MaxInt64},
			metricLimit: config.LimitConfig{MaxCardinality: math.MaxInt64},
			dpLimit:     config.LimitConfig{MaxCardinality: math.MaxInt64},
		},
		{
			name:        "with_overflow",
			resLimit:    config.LimitConfig{MaxCardinality: 1},
			scopeLimit:  config.LimitConfig{MaxCardinality: 1},
			metricLimit: config.LimitConfig{MaxCardinality: 1},
			dpLimit:     config.LimitConfig{MaxCardinality: 1},
		},
	} {
		benchmarkMergeWithTestdata(b, tc.name, tc.resLimit, tc.scopeLimit, tc.metricLimit, tc.dpLimit)
	}
}

// TODO rename this to BenchmarkAppendBinary once we have compared
// the old and AppendBinary implementations.
// TODO use subtests for with/without buffer reuse
func BenchmarkMarshal(b *testing.B) {
	benchmarkAppendBinary(b, func(b *testing.B, v *Value) {
		for i := 0; i < b.N; i++ {
			if _, err := v.AppendBinary(nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkAppendBinaryReuseBuffer(b *testing.B) {
	benchmarkAppendBinary(b, func(b *testing.B, v *Value) {
		var buf []byte
		var err error
		for i := 0; i < b.N; i++ {
			buf, err = v.AppendBinary(buf[:0])
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkAppendBinary(b *testing.B, f func(*testing.B, *Value)) {
	b.Helper()
	for _, tc := range benchTestCases {
		dir := filepath.Join("../../testdata", tc)
		md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
		require.NoError(b, err)
		md.MarkReadOnly()
		b.Run(tc, func(b *testing.B) {
			maxLimit := config.LimitConfig{MaxCardinality: math.MaxInt64}
			v := NewValue(maxLimit, maxLimit, maxLimit, maxLimit, defaultMaxBuckets)
			require.NoError(b, updateValueWithPMetrics(md, v))
			f(b, v)
		})
	}
}

func benchmarkMergeWithTestdata(
	b *testing.B,
	name string,
	resLimit, scopeLimit, metricLimit, dpLimit config.LimitConfig,
) {
	b.Helper()

	for _, tc := range benchTestCases {
		dir := filepath.Join("../../testdata", tc)
		md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
		require.NoError(b, err)
		md.MarkReadOnly()
		b.Run(name+"/"+tc, func(b *testing.B) {
			to := NewValue(resLimit, scopeLimit, metricLimit, dpLimit, defaultMaxBuckets)
			from := NewValue(resLimit, scopeLimit, metricLimit, dpLimit, defaultMaxBuckets)
			require.NoError(b, updateValueWithPMetrics(md, from))

			for i := 0; i < b.N; i++ {
				if err := to.Merge(from); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func updateValueWithPMetrics(m pmetric.Metrics, v *Value) error {
	rms := m.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				if err := v.MergeMetric(rm, sm, ms.At(k), ""); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// TestMergeBudgets asserts the admission rule with cardinality budgets: an
// entity already in the value is merged for free, a new entity takes one slot
// in the budget that delivered it, and a full budget sends it to overflow.
// Every op is built as its own value, round-tripped through binary and merged
// into the accumulated value, which mirrors the pebble merge operator.
func TestMergeBudgets(t *testing.T) {
	// entity is one datapoint of value 1 at path resource/scope/metric/datapoint,
	// charged to budget when new to the value.
	type entity struct {
		budget string
		path   string
	}
	for _, tc := range []struct {
		name         string
		limit        int64 // max cardinality at every level
		ops          [][]entity
		wantAdmitted map[string]int64
		wantOverflow OverflowStats
	}{
		{
			name:  "single_budget_overflows_at_limit",
			limit: 2,
			ops: [][]entity{
				{{"", "A/s/m/d"}}, {{"", "B/s/m/d"}}, {{"", "C/s/m/d"}},
			},
			wantAdmitted: map[string]int64{"A/s/m/d": 1, "B/s/m/d": 1},
			wantOverflow: OverflowStats{Resources: 1},
		},
		{
			name:  "capacity_per_budget",
			limit: 2,
			ops: [][]entity{
				{{"p0", "A/s/m/d"}}, {{"p0", "B/s/m/d"}}, {{"p0", "C/s/m/d"}},
				{{"p1", "D/s/m/d"}}, {{"p1", "E/s/m/d"}}, {{"p1", "F/s/m/d"}},
			},
			wantAdmitted: map[string]int64{"A/s/m/d": 1, "B/s/m/d": 1, "D/s/m/d": 1, "E/s/m/d": 1},
			wantOverflow: OverflowStats{Resources: 2},
		},
		{
			name:  "shared_entity_takes_one_slot",
			limit: 2,
			ops: [][]entity{
				{{"p0", "A/s/m/d"}}, {{"p0", "B/s/m/d"}},
				{{"p1", "A/s/m/d"}}, {{"p1", "C/s/m/d"}},
			},
			wantAdmitted: map[string]int64{"A/s/m/d": 2, "B/s/m/d": 1, "C/s/m/d": 1},
		},
		{
			name:  "slot_charged_to_first_deliverer",
			limit: 2,
			ops: [][]entity{
				{{"p0", "A/s/m/d"}},
				{{"p1", "A/s/m/d"}}, {{"p1", "B/s/m/d"}}, {{"p1", "C/s/m/d"}},
			},
			wantAdmitted: map[string]int64{"A/s/m/d": 2, "B/s/m/d": 1, "C/s/m/d": 1},
		},
		{
			name:  "stored_operand_keeps_attribution",
			limit: 2,
			ops: [][]entity{
				{{"p0", "C/s/m/d"}, {"p0", "D/s/m/d"}},
				// A was charged to p0 and B to p1 when this operand was built.
				// p0 is full in the accumulated value, p1 is empty.
				{{"p0", "A/s/m/d"}, {"p1", "B/s/m/d"}},
			},
			wantAdmitted: map[string]int64{"B/s/m/d": 1, "C/s/m/d": 1, "D/s/m/d": 1},
			wantOverflow: OverflowStats{Resources: 1},
		},
		{
			name:  "overflow_bucket_is_never_charged",
			limit: 2,
			ops: [][]entity{
				{{"p1", "A/s/m/d"}},
				// This operand holds A, B and an overflow bucket for C. On
				// merge A already exists, so p0 has one free slot that the
				// replayed overflow bucket must not take.
				{{"p0", "A/s/m/d"}, {"p0", "B/s/m/d"}, {"p0", "C/s/m/d"}},
				{{"p0", "D/s/m/d"}},
			},
			wantAdmitted: map[string]int64{"A/s/m/d": 2, "B/s/m/d": 1, "D/s/m/d": 1},
			wantOverflow: OverflowStats{Resources: 1},
		},
		{
			// Overflowed resources merge into one bucket whose scope tracker
			// charges scopes to the delivering budget like any resource.
			name:  "scope_overflow_inside_resource_overflow_bucket",
			limit: 1,
			ops: [][]entity{
				{{"p0", "A/s1/m1/d1"}},
				{{"p0", "B/s1/m1/d1"}}, // B overflows, s1 admitted under _other on p0
				{{"p0", "C/s2/m1/d1"}}, // C overflows, s2 overflows under _other, p0 full
				{{"p1", "D/s3/m1/d1"}},
				{{"p1", "E/s4/m1/d1"}}, // E overflows, s4 admitted under _other on p1
				{{"p1", "F/s5/m1/d1"}}, // F overflows, s5 overflows under _other, p1 full
			},
			wantAdmitted: map[string]int64{"A/s1/m1/d1": 1, "D/s3/m1/d1": 1},
			wantOverflow: OverflowStats{Resources: 4, Scopes: 2},
		},
		{
			name:  "budgets_apply_at_every_level",
			limit: 1,
			ops: [][]entity{
				{{"p0", "A/s1/m1/d1"}},
				{{"p1", "A/s1/m1/d2"}}, {{"p1", "A/s1/m1/d3"}},
				{{"p1", "A/s1/m2/d1"}}, {{"p1", "A/s1/m3/d1"}},
				{{"p1", "A/s2/m1/d1"}}, {{"p1", "A/s3/m1/d1"}},
				{{"p1", "B/s1/m1/d1"}}, {{"p1", "C/s1/m1/d1"}},
			},
			wantAdmitted: map[string]int64{
				"A/s1/m1/d1": 1, "A/s1/m1/d2": 1,
				"A/s1/m2/d1": 1,
				"A/s2/m1/d1": 1,
				"B/s1/m1/d1": 1,
			},
			wantOverflow: OverflowStats{Resources: 1, Scopes: 1, Metrics: 1, Datapoints: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := budgetTestValue(tc.limit)
			for _, op := range tc.ops {
				o := budgetTestValue(tc.limit)
				for _, e := range op {
					mergeMetrics(t, o, pathMetrics(e.path), e.budget)
				}
				require.NoError(t, v.Merge(roundtripValue(t, o, tc.limit)))
			}
			md, stats, err := roundtripValue(t, v, tc.limit).Finalize()
			require.NoError(t, err)
			assert.Equal(t, tc.wantAdmitted, admittedPaths(md))
			assert.Equal(t, tc.wantOverflow, stats)
		})
	}
}

// TestUnmarshalV1 reads a value written before budgets existed. Everything
// in it lands on the default budget and it is written back in the current
// format.
func TestUnmarshalV1(t *testing.T) {
	md := pathMetrics("A/s/m/d")
	pmb, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	require.NoError(t, err)
	// One tracker per level, each: type byte, count 1, sketch length 0.
	var trackers []byte
	for typ := byte(0); typ < 4; typ++ {
		trackers = append(trackers, typ, 1, 0, 0)
	}
	b := []byte{1}
	b = binary.BigEndian.AppendUint32(b, uint32(len(trackers)))
	b = append(append(b, trackers...), pmb...)

	v := budgetTestValue(2)
	require.NoError(t, v.Unmarshal(b))
	require.Equal(t, []string{""}, v.trackers.Budgets())

	// A holds one of the two default-budget slots, so one more resource
	// fits on "" and the next one overflows, while p0 has its own pool.
	mergeMetrics(t, v, pathMetrics("B/s/m/d"), "")
	mergeMetrics(t, v, pathMetrics("C/s/m/d"), "")
	mergeMetrics(t, v, pathMetrics("D/s/m/d"), "p0")
	out, err := v.AppendBinary(nil)
	require.NoError(t, err)
	assert.Equal(t, byte(2), out[0])
	md, stats, err := roundtripValue(t, v, 2).Finalize()
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{"A/s/m/d": 1, "B/s/m/d": 1, "D/s/m/d": 1}, admittedPaths(md))
	assert.Equal(t, OverflowStats{Resources: 1}, stats)
}

func budgetTestValue(limit int64) *Value {
	lc := func(key string) config.LimitConfig {
		return config.LimitConfig{
			MaxCardinality: limit,
			Overflow: config.OverflowConfig{
				Attributes: []config.Attribute{{Key: key, Value: true}},
			},
		}
	}
	return NewValue(
		lc("resource_overflow"), lc("scope_overflow"), lc("metric_overflow"), lc("dp_overflow"),
		defaultMaxBuckets,
	)
}

func roundtripValue(t *testing.T, v *Value, limit int64) *Value {
	t.Helper()
	b, err := v.AppendBinary(nil)
	require.NoError(t, err)
	out := budgetTestValue(limit)
	require.NoError(t, out.Unmarshal(b))
	return out
}

// pathMetrics builds one delta sum datapoint of value 1 at
// resource/scope/metric/datapoint, each segment naming its entity.
func pathMetrics(path string) pmetric.Metrics {
	parts := strings.Split(path, "/")
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("name", parts[0])
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(parts[1])
	m := sm.Metrics().AppendEmpty()
	m.SetName(parts[2])
	sum := m.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := sum.DataPoints().AppendEmpty()
	dp.Attributes().PutStr("name", parts[3])
	dp.SetIntValue(1)
	return md
}

// admittedPaths sums datapoint values per path, skipping overflow buckets and
// overflow metrics, which carry no entity name.
func admittedPaths(md pmetric.Metrics) map[string]int64 {
	out := map[string]int64{}
	for _, rm := range md.ResourceMetrics().All() {
		res, ok := rm.Resource().Attributes().Get("name")
		if !ok {
			continue
		}
		for _, sm := range rm.ScopeMetrics().All() {
			if sm.Scope().Name() == "" {
				continue
			}
			for _, m := range sm.Metrics().All() {
				for _, dp := range m.Sum().DataPoints().All() {
					d, ok := dp.Attributes().Get("name")
					if !ok {
						continue
					}
					out[res.Str()+"/"+sm.Scope().Name()+"/"+m.Name()+"/"+d.Str()] += dp.IntValue()
				}
			}
		}
	}
	return out
}
