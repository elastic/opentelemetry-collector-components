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
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
)

type fatLSMShape struct {
	resources int
	dps       int
}

// BenchmarkCompactPeak measures lone MERGE and sibling combine on a fat
// operand. It does not measure ingest, export, or other merger testdata.
//
// base is one operand (expo histograms, explicit histograms, attributes).
// sibling overlaps the first 16 resources of base so a combine merges
// histogram streams instead of only appending new identities.
//
// Finish calls Merge/MergeOlder/Finish only. PebbleCompact writes the same
// bytes, flushes each operand to its own sstable, then Compacts that key.
func BenchmarkCompactPeak(b *testing.B) {
	base := mustFatLSMOperand(b, fatLSMShape{resources: 64, dps: 16})
	// Sibling overlaps the first 16 resources so MergeOlder combines
	// histogram streams instead of only appending new identities.
	sibling := mustFatLSMOperand(b, fatLSMShape{resources: 16, dps: 16})

	// One MERGE operand, no sibling. Lazy Finish returns the clone.
	b.Run(fmt.Sprintf("lone/Finish/%dKiB", len(base)/1024), func(b *testing.B) {
		benchFinish(b, base)
	})
	b.Run(fmt.Sprintf("lone/PebbleCompact/%dKiB", len(base)/1024), func(b *testing.B) {
		benchPebbleCompact(b, base)
	})
	// Sibling on the same key. Unmarshal still runs on both operands.
	b.Run(fmt.Sprintf("combine/Finish/%dKiB+%dKiB", len(base)/1024, len(sibling)/1024), func(b *testing.B) {
		benchFinish(b, base, sibling)
	})
	b.Run(fmt.Sprintf("combine/PebbleCompact/%dKiB+%dKiB", len(base)/1024, len(sibling)/1024), func(b *testing.B) {
		benchPebbleCompact(b, base, sibling)
	})
}

func benchFinish(b *testing.B, operand []byte, siblings ...[]byte) {
	b.Helper()
	total := len(operand)
	for _, s := range siblings {
		total += len(s)
	}
	b.ReportAllocs()
	b.SetBytes(int64(total))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, closer := compactFinish(b, operand, siblings...)
		if closer != nil {
			if err := closer.Close(); err != nil {
				b.Fatal(err)
			}
		}
		if len(out) == 0 {
			b.Fatal("finish wrote empty operand")
		}
	}
}

func benchPebbleCompact(b *testing.B, operand []byte, siblings ...[]byte) {
	b.Helper()
	db := openPeakDB(b)
	b.Cleanup(func() {
		require.NoError(b, db.Close())
	})
	total := len(operand)
	for _, s := range siblings {
		total += len(s)
	}
	b.ReportAllocs()
	b.SetBytes(int64(total))
	b.ResetTimer()
	for i := range b.N {
		key := binary.BigEndian.AppendUint64(nil, uint64(i))
		end := binary.BigEndian.AppendUint64(nil, uint64(i)+1)
		if err := db.Merge(key, operand, pebble.NoSync); err != nil {
			b.Fatal(err)
		}
		if err := db.Flush(); err != nil {
			b.Fatal(err)
		}
		for _, s := range siblings {
			if err := db.Merge(key, s, pebble.NoSync); err != nil {
				b.Fatal(err)
			}
			if err := db.Flush(); err != nil {
				b.Fatal(err)
			}
		}
		if err := db.Compact(key, end, false); err != nil {
			b.Fatal(err)
		}
	}
}

// noLimit is the processor default. MaxCardinality 0 disables overflow
// tracking, so the fat fixture keeps every identity. A limit of 1 would
// collapse the operand into overflow buckets and miss the compact-peak shape.
var noLimit config.LimitConfig

func peakValue() *Value {
	return NewValue(noLimit, noLimit, noLimit, noLimit, defaultMaxBuckets)
}

func peakPebbleMerger() *pebble.Merger {
	return NewPebbleMerger(noLimit, noLimit, noLimit, noLimit, defaultMaxBuckets)
}

func mustFatLSMOperand(tb testing.TB, shape fatLSMShape) []byte {
	tb.Helper()
	v := peakValue()
	require.NoError(tb, updateValueWithPMetrics(peakCompactMetrics(shape), v))
	operand, err := v.AppendBinary(nil)
	require.NoError(tb, err)
	require.NotEmpty(tb, operand)
	return operand
}

func compactFinish(tb testing.TB, operand []byte, siblings ...[]byte) ([]byte, io.Closer) {
	tb.Helper()
	vm, err := peakPebbleMerger().Merge(nil, operand)
	require.NoError(tb, err)
	for _, s := range siblings {
		require.NoError(tb, vm.MergeOlder(s))
	}
	out, closer, err := vm.Finish(true)
	require.NoError(tb, err)
	return out, closer
}

func openPeakDB(tb testing.TB) *pebble.DB {
	tb.Helper()
	opts := &pebble.Options{
		FS:                          vfs.NewMem(),
		DisableWAL:                  true,
		DisableAutomaticCompactions: true,
		MemTableSize:                32 << 20,
		Merger:                      peakPebbleMerger(),
	}
	db, err := pebble.Open("", opts)
	require.NoError(tb, err)
	return db
}

// peakCompactMetrics builds expo-histogram buckets, explicit histograms,
// and attributes.
func peakCompactMetrics(shape fatLSMShape) pmetric.Metrics {
	expoBuckets := make([]uint64, defaultMaxBuckets)
	for i := range expoBuckets {
		expoBuckets[i] = uint64(i + 1)
	}
	explicitBounds := []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000, 2000, 4000, 8000, 16000, 32000}
	explicitCounts := make([]uint64, len(explicitBounds)+1)
	for i := range explicitCounts {
		explicitCounts[i] = uint64(i + 1)
	}

	md := pmetric.NewMetrics()
	for r := 0; r < shape.resources; r++ {
		rm := md.ResourceMetrics().AppendEmpty()
		attrs := rm.Resource().Attributes()
		attrs.PutStr("service.name", fmt.Sprintf("svc-%d", r))
		attrs.PutStr("service.environment", "prod")
		attrs.PutStr("deployment.environment", "production")
		attrs.PutStr("telemetry.sdk.language", "go")

		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("github.com/elastic/apm-data")
		sm.Scope().SetVersion("1.0.0")

		expo := sm.Metrics().AppendEmpty()
		expo.SetName("transaction.duration")
		expo.SetUnit("us")
		eh := expo.SetEmptyExponentialHistogram()
		eh.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

		hist := sm.Metrics().AppendEmpty()
		hist.SetName("transaction.duration.histogram")
		hist.SetUnit("us")
		h := hist.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

		for d := 0; d < shape.dps; d++ {
			edp := eh.DataPoints().AppendEmpty()
			edp.Attributes().PutStr("transaction.name", fmt.Sprintf("GET /api/%d", d))
			edp.Attributes().PutStr("transaction.type", "request")
			edp.Attributes().PutStr("event.outcome", "success")
			edp.SetCount(1000)
			edp.SetSum(float64(1000 * (d + 1)))
			edp.SetScale(0)
			edp.SetZeroCount(1)
			edp.Positive().SetOffset(0)
			edp.Positive().BucketCounts().FromRaw(expoBuckets)
			edp.Negative().SetOffset(0)
			edp.Negative().BucketCounts().FromRaw(expoBuckets)

			hdp := h.DataPoints().AppendEmpty()
			hdp.Attributes().PutStr("transaction.name", fmt.Sprintf("GET /api/%d", d))
			hdp.Attributes().PutStr("transaction.type", "request")
			hdp.Attributes().PutStr("event.outcome", "success")
			hdp.SetCount(1000)
			hdp.SetSum(float64(1000 * (d + 1)))
			hdp.ExplicitBounds().FromRaw(explicitBounds)
			hdp.BucketCounts().FromRaw(explicitCounts)
		}
	}
	return md
}
