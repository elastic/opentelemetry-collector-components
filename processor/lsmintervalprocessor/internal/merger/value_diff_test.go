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
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/xpdata/entity"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger/internal/v1ref"
)

// The tests in this file check that Value behaves exactly like the v1
// implementation it replaced, kept in v1ref: for random metrics, under
// small limits so that every level overflows, both must produce the same
// metrics and overflow estimates, whether metrics are merged directly or
// through encoded values merged the way the Pebble merger does.

const diffSeeds = 400

type diffLimits struct {
	res, scope, metric, dp config.LimitConfig
}

func randomLimits(r *rand.Rand) diffLimits {
	limit := func(name string) config.LimitConfig {
		return config.LimitConfig{
			MaxCardinality: int64(r.Intn(4)), // 0 disables the limit
			Overflow: config.OverflowConfig{
				Attributes: []config.Attribute{{Key: name + "_overflow", Value: true}},
			},
		}
	}
	return diffLimits{limit("resource"), limit("scope"), limit("metric"), limit("dp")}
}

func (l diffLimits) newValue() *Value {
	return NewValue(l.res, l.scope, l.metric, l.dp, 16)
}

func (l diffLimits) newRef() *v1ref.Value {
	return v1ref.NewValue(l.res, l.scope, l.metric, l.dp, 16)
}

// randomBatch generates metrics shaped like the processor's input, drawing
// identities from small pools so that batches overlap.
func randomBatch(r *rand.Rand, ts *pcommon.Timestamp) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for range 1 + r.Intn(3) {
		rm := md.ResourceMetrics().AppendEmpty()
		svc := r.Intn(5)
		rm.SetSchemaUrl(fmt.Sprintf("https://schema/%d", svc%2))
		rm.Resource().Attributes().PutStr("service.name", fmt.Sprint("svc-", svc))
		if svc%2 == 0 {
			rm.Resource().Attributes().PutInt("shard", int64(svc))
		}
		if svc%3 == 0 {
			ref := entity.ResourceEntityRefs(rm.Resource()).AppendEmpty()
			ref.SetType("service")
			ref.SetSchemaUrl("https://schema/entity")
			ref.IdKeys().Append("service.name")
			ref.DescriptionKeys().Append("shard")
		}
		for range 1 + r.Intn(2) {
			sm := rm.ScopeMetrics().AppendEmpty()
			sc := r.Intn(3)
			sm.Scope().SetName(fmt.Sprint("scope-", sc))
			sm.Scope().SetVersion("1.0")
			if sc == 2 {
				sm.Scope().Attributes().PutBool("custom", true)
			}
			for range 1 + r.Intn(4) {
				randomMetric(r, sm.Metrics().AppendEmpty(), ts)
			}
		}
	}
	return md
}

func randomMetric(r *rand.Rand, m pmetric.Metric, ts *pcommon.Timestamp) {
	next := func() pcommon.Timestamp {
		// Sometimes repeat a timestamp: ties decide cumulative merges.
		if r.Intn(4) != 0 {
			*ts += pcommon.Timestamp(1 + r.Intn(1000))
		}
		return *ts
	}
	attrs := func(dst pcommon.Map) {
		dst.PutStr("k", fmt.Sprint("v", r.Intn(6)))
		switch r.Intn(4) {
		case 0:
			dst.PutInt("n", int64(r.Intn(2)))
		case 1:
			s := dst.PutEmptySlice("list")
			s.AppendEmpty().SetStr("a")
			s.AppendEmpty().SetDouble(1.5)
		case 2:
			dst.PutEmptyBytes("raw").FromRaw([]byte{byte(r.Intn(2)), 7})
		}
	}
	dps := 1 + r.Intn(4)
	switch kind := r.Intn(7); kind {
	case 0, 1, 2:
		m.SetName([]string{"sum.delta.int", "sum.delta.double", "sum.cumulative"}[kind])
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		if kind == 2 {
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		}
		for range dps {
			dp := sum.DataPoints().AppendEmpty()
			attrs(dp.Attributes())
			dp.SetStartTimestamp(1)
			dp.SetTimestamp(next())
			if kind == 1 {
				dp.SetDoubleValue(r.Float64() * 10)
			} else {
				dp.SetIntValue(int64(r.Intn(100)))
			}
			if r.Intn(3) == 0 {
				e := dp.Exemplars().AppendEmpty()
				e.SetTimestamp(dp.Timestamp())
				e.SetIntValue(int64(r.Intn(9)))
				e.SetTraceID(pcommon.TraceID{1, byte(r.Intn(255))})
				e.SetSpanID(pcommon.SpanID{2, byte(r.Intn(255))})
				e.FilteredAttributes().PutStr("user", "u")
			}
		}
	case 3, 4:
		m.SetName([]string{"hist.delta", "hist.cumulative"}[kind-3])
		m.SetUnit("ms")
		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		if kind == 4 {
			h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		}
		for range dps {
			dp := h.DataPoints().AppendEmpty()
			attrs(dp.Attributes())
			dp.SetTimestamp(next())
			bounds := []float64{1, 5, 10}
			if r.Intn(5) == 0 {
				bounds = []float64{2, 4}
			}
			dp.ExplicitBounds().FromRaw(bounds)
			var count uint64
			for range len(bounds) + 1 {
				c := uint64(r.Intn(4))
				dp.BucketCounts().Append(c)
				count += c
			}
			dp.SetCount(count)
			if r.Intn(4) != 0 {
				dp.SetSum(float64(count) * 3)
			}
			if r.Intn(2) == 0 {
				dp.SetMin(r.Float64())
				dp.SetMax(5 + r.Float64())
			}
		}
	case 5:
		m.SetName("exphist.delta")
		e := m.SetEmptyExponentialHistogram()
		e.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for range dps {
			dp := e.DataPoints().AppendEmpty()
			attrs(dp.Attributes())
			dp.SetTimestamp(next())
			dp.SetScale(int32([]int{-1, 0, 2, 5}[r.Intn(4)]))
			dp.SetZeroCount(uint64(r.Intn(3)))
			count := dp.ZeroCount()
			for _, b := range []pmetric.ExponentialHistogramDataPointBuckets{dp.Positive(), dp.Negative()} {
				n := r.Intn(8)
				if n == 0 {
					// Empty buckets with an odd offset make expo.Collapse
					// panic in both implementations; SDKs send offset 0.
					continue
				}
				b.SetOffset(int32(r.Intn(20) - 10))
				for range n {
					c := uint64(r.Intn(5))
					b.BucketCounts().Append(c)
					count += c
				}
			}
			dp.SetCount(count)
			if r.Intn(3) != 0 {
				dp.SetSum(float64(count))
			}
		}
	case 6:
		m.SetName("summary")
		s := m.SetEmptySummary()
		for range dps {
			dp := s.DataPoints().AppendEmpty()
			attrs(dp.Attributes())
			dp.SetTimestamp(next())
			dp.SetCount(uint64(r.Intn(10)))
			dp.SetSum(r.Float64())
			q := dp.QuantileValues().AppendEmpty()
			q.SetQuantile(0.5)
			q.SetValue(r.Float64())
		}
	}
}

type metricMerger interface {
	MergeMetric(pmetric.ResourceMetrics, pmetric.ScopeMetrics, pmetric.Metric) error
}

// mergeAll merges a copy of md: merging may modify the input metrics.
func mergeAll(t *testing.T, v metricMerger, md pmetric.Metrics) {
	t.Helper()
	c := pmetric.NewMetrics()
	md.CopyTo(c)
	rms := c.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				require.NoError(t, v.MergeMetric(rms.At(i), sms.At(j), ms.At(k)))
			}
		}
	}
}

func assertSameFinal(t *testing.T, ref *v1ref.Value, v *Value, msg string) {
	t.Helper()
	wantMD, wantStats, err := ref.Finalize()
	require.NoError(t, err, msg)
	gotMD, gotStats, err := v.Finalize()
	require.NoError(t, err, msg)
	// v1 appends overflow metrics in map order, and a scope can have
	// several overflow metrics with the same name.
	sortOverflowMetrics(wantMD)
	sortOverflowMetrics(gotMD)
	assert.NoError(t, pmetrictest.CompareMetrics(wantMD, gotMD, pmetrictest.IgnoreMetricsOrder()), msg)
	assert.Equal(t, OverflowStats(wantStats), gotStats, msg)
}

func TestValueMatchesV1Reference(t *testing.T) {
	for seed := int64(0); seed < diffSeeds; seed++ {
		r := rand.New(rand.NewSource(seed))
		limits := randomLimits(r)
		var ts pcommon.Timestamp
		batches := make([]pmetric.Metrics, 1+r.Intn(10))
		for i := range batches {
			batches[i] = randomBatch(r, &ts)
		}
		msg := fmt.Sprintf("seed %d", seed)

		// Merging metrics directly, as the in-memory buffer does.
		ref, v := limits.newRef(), limits.newValue()
		for _, md := range batches {
			mergeAll(t, ref, md)
			mergeAll(t, v, md)
		}
		assertSameFinal(t, ref, v, msg+": direct")

		// The same, after encoding and decoding.
		ref, v = limits.newRef(), limits.newValue()
		for _, md := range batches {
			mergeAll(t, ref, md)
			mergeAll(t, v, md)
		}
		v = roundTrip(t, limits, v)
		refBytes, err := ref.AppendBinary(nil)
		require.NoError(t, err)
		ref = limits.newRef()
		require.NoError(t, ref.Unmarshal(refBytes))
		assertSameFinal(t, ref, v, msg+": round trip")

		// One encoded operand per batch, folded newest first like the
		// Pebble merger, re-encoding the result at random points like a
		// compaction would. v2 also gets some operands in the v1 format.
		var refOps, ops, v1Ops [][]byte
		for _, md := range batches {
			refOp, op := limits.newRef(), limits.newValue()
			mergeAll(t, refOp, md)
			mergeAll(t, op, md)
			b, err := refOp.AppendBinary(nil)
			require.NoError(t, err)
			refOps = append(refOps, b)
			v1Ops = append(v1Ops, b)
			b, err = op.appendBinaryV2(nil)
			require.NoError(t, err)
			ops = append(ops, b)
		}
		reencode := make([]bool, len(batches))
		useV1 := make([]bool, len(batches))
		for i := range reencode {
			reencode[i] = r.Intn(3) == 0
			useV1[i] = r.Intn(3) == 0
		}
		last := len(batches) - 1
		ref = limits.newRef()
		require.NoError(t, ref.Unmarshal(refOps[last]))
		v = limits.newValue()
		require.NoError(t, v.Unmarshal(pick(useV1[last], v1Ops[last], ops[last])))
		for i := last - 1; i >= 0; i-- {
			refOp := limits.newRef()
			require.NoError(t, refOp.Unmarshal(refOps[i]))
			require.NoError(t, ref.Merge(refOp))
			op := limits.newValue()
			require.NoError(t, op.Unmarshal(pick(useV1[i], v1Ops[i], ops[i])))
			require.NoError(t, v.Merge(op))
			if reencode[i] {
				b, err := ref.AppendBinary(nil)
				require.NoError(t, err)
				ref = limits.newRef()
				require.NoError(t, ref.Unmarshal(b))
				v = roundTrip(t, limits, v)
			}
		}
		assertSameFinal(t, ref, v, msg+": folded operands")
	}
}

// sortOverflowMetrics moves the overflow metrics of each scope to its end,
// ordered by name and value, so that outputs that differ only in the order
// of overflow metrics compare equal.
func sortOverflowMetrics(md pmetric.Metrics) {
	isOverflow := func(m pmetric.Metric) bool {
		return m.Name() == overflowMetricName || m.Name() == overflowDatapointMetricName
	}
	key := func(m pmetric.Metric) string {
		return fmt.Sprintf("%s/%020d", m.Name(), m.Sum().DataPoints().At(0).IntValue())
	}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			var overflows []pmetric.Metric
			for k := 0; k < ms.Len(); k++ {
				if isOverflow(ms.At(k)) {
					c := pmetric.NewMetric()
					ms.At(k).CopyTo(c)
					overflows = append(overflows, c)
				}
			}
			ms.RemoveIf(isOverflow)
			sort.Slice(overflows, func(a, b int) bool { return key(overflows[a]) < key(overflows[b]) })
			for _, m := range overflows {
				m.MoveTo(ms.AppendEmpty())
			}
		}
	}
}

func pick(cond bool, a, b []byte) []byte {
	if cond {
		return a
	}
	return b
}

func roundTrip(t *testing.T, limits diffLimits, v *Value) *Value {
	t.Helper()
	b, err := v.appendBinaryV2(nil)
	require.NoError(t, err)
	out := limits.newValue()
	require.NoError(t, out.Unmarshal(b))
	return out
}

// TestValueReadsV1Fixtures reads values written by the v1 implementation
// and checks that they finalize to what v1 produced.
func TestValueReadsV1Fixtures(t *testing.T) {
	dir := "../../testdata/merger/v1"
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var n int
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".bin")
		if !ok {
			continue
		}
		n++
		t.Run(name, func(t *testing.T) {
			b, err := os.ReadFile(filepath.Join(dir, e.Name()))
			require.NoError(t, err)
			require.Equal(t, versionV1, b[0])
			expected, err := golden.ReadMetrics(filepath.Join(dir, name+".output.yaml"))
			require.NoError(t, err)
			var expectedStats OverflowStats
			stats, err := os.ReadFile(filepath.Join(dir, name+".stats.json"))
			require.NoError(t, err)
			require.NoError(t, jsonUnmarshal(stats, &expectedStats))

			v := getTestValue(t)
			require.NoError(t, v.Unmarshal(b))
			// Re-encoding as v2 must not change the result either.
			v2b, err := v.appendBinaryV2(nil)
			require.NoError(t, err)
			v2 := getTestValue(t)
			require.NoError(t, v2.Unmarshal(v2b))
			for label, val := range map[string]*Value{"v1": v, "v1 as v2": v2} {
				actual, actualStats, err := val.Finalize()
				require.NoError(t, err, label)
				// golden.WriteMetrics normalized the timestamps of the
				// stored output; TestValueMatchesV1Reference compares them.
				assert.NoError(t, pmetrictest.CompareMetrics(expected, actual,
					pmetrictest.IgnoreMetricsOrder(),
					pmetrictest.IgnoreTimestamp(),
					pmetrictest.IgnoreStartTimestamp(),
				), label)
				assert.Equal(t, expectedStats, actualStats, label)
			}
		})
	}
	require.Positive(t, n)
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// TestValueCumulativeTie merges encoded operands holding the same cumulative
// stream at the same timestamp, which random metrics rarely produce: the
// canonical tie-breaker is used in both formats.
func TestValueCumulativeTie(t *testing.T) {
	batch := func(value int64) pmetric.Metrics {
		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("cumulative")
		sum := m.SetEmptySum()
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp := sum.DataPoints().AppendEmpty()
		dp.SetTimestamp(100)
		dp.SetIntValue(value)
		return md
	}
	var limits diffLimits
	encode := func(v interface{ AppendBinary([]byte) ([]byte, error) }) []byte {
		var b []byte
		var err error
		if current, ok := v.(*Value); ok {
			b, err = current.appendBinaryV2(nil)
		} else {
			b, err = v.AppendBinary(nil)
		}
		require.NoError(t, err)
		return b
	}
	newer, older := batch(1), batch(2)

	ref := limits.newRef()
	mergeAll(t, ref, newer)
	refOp := limits.newRef()
	mergeAll(t, refOp, older)
	refOlder := limits.newRef()
	require.NoError(t, refOlder.Unmarshal(encode(refOp)))
	refCur := limits.newRef()
	require.NoError(t, refCur.Unmarshal(encode(ref)))
	require.NoError(t, refCur.Merge(refOlder))

	v := limits.newValue()
	mergeAll(t, v, newer)
	op := limits.newValue()
	mergeAll(t, op, older)
	vOlder := limits.newValue()
	require.NoError(t, vOlder.Unmarshal(encode(op)))
	cur := limits.newValue()
	require.NoError(t, cur.Unmarshal(encode(v)))
	require.NoError(t, cur.Merge(vOlder))

	assertSameFinal(t, refCur, cur, "tie")
}

// TestValueKeepsResourceEntityRefs checks that resource entity references,
// which are not part of the resource identity, survive encoding and merging.
func TestValueKeepsResourceEntityRefs(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	ref := entity.ResourceEntityRefs(rm.Resource()).AppendEmpty()
	ref.SetType("service")
	ref.SetSchemaUrl("https://schema/entity")
	ref.IdKeys().Append("service.name")
	ref.DescriptionKeys().Append("service.version")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("requests")
	sum := m.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	sum.DataPoints().AppendEmpty().SetIntValue(1)

	var limits diffLimits
	v := limits.newValue()
	mergeAll(t, v, md)
	op := roundTrip(t, limits, v)
	merged := limits.newValue()
	require.NoError(t, merged.Merge(op))
	out, _, err := roundTrip(t, limits, merged).Finalize()
	require.NoError(t, err)

	refs := entity.ResourceEntityRefs(out.ResourceMetrics().At(0).Resource())
	require.Equal(t, 1, refs.Len())
	got := refs.At(0)
	assert.Equal(t, "service", got.Type())
	assert.Equal(t, "https://schema/entity", got.SchemaUrl())
	assert.Equal(t, []string{"service.name"}, got.IdKeys().AsRaw())
	assert.Equal(t, []string{"service.version"}, got.DescriptionKeys().AsRaw())
}

// The reader-first release must produce bytes readable by the previous
// implementation, including all overflow estimators, even after reading v2.
func TestValueReaderFirstWritesV1(t *testing.T) {
	for seed := int64(0); seed < diffSeeds; seed++ {
		r := rand.New(rand.NewSource(seed))
		limits := randomLimits(r)
		original := limits.newValue()
		var ts pcommon.Timestamp
		for i := 0; i < 5; i++ {
			mergeAll(t, original, randomBatch(r, &ts))
		}
		v2, err := original.appendBinaryV2(nil)
		require.NoError(t, err)
		restored := limits.newValue()
		require.NoError(t, restored.Unmarshal(v2))
		v1, err := restored.AppendBinary(nil)
		require.NoError(t, err)
		require.Equal(t, byte(1), v1[0])
		old := limits.newRef()
		require.NoError(t, old.Unmarshal(v1))
		assertSameFinal(t, old, original, fmt.Sprint("reader-first seed ", seed))
	}
}

func TestPebbleReaderFirstRewritesLoneV2(t *testing.T) {
	var limits diffLimits
	v := limits.newValue()
	r := rand.New(rand.NewSource(42))
	var ts pcommon.Timestamp
	mergeAll(t, v, randomBatch(r, &ts))
	encoded, err := v.appendBinaryV2(nil)
	require.NoError(t, err)
	pm := NewPebbleMerger(limits.res, limits.scope, limits.metric, limits.dp, 16)
	vm, err := pm.Merge(nil, encoded)
	require.NoError(t, err)
	output, closer, err := vm.Finish(true)
	require.NoError(t, err)
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.Equal(t, byte(1), output[0])
	old := limits.newRef()
	require.NoError(t, old.Unmarshal(output))
	assertSameFinal(t, old, v, "lone v2 operand")
}
