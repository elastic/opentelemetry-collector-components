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

package compressedotlpencodingextension

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressDecompress(t *testing.T) {
	zstdCompressor, err := NewZSTDCompressor()
	require.NoError(t, err)
	for _, tc := range []struct {
		name       string
		compressor compressor
	}{
		{
			name:       "none",
			compressor: NoneCompressor{},
		}, {
			name:       "lz4",
			compressor: LZ4Compressor{},
		}, {
			name:       "zstd",
			compressor: zstdCompressor,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := []byte("hello world hello world hello world hello world")
			buf := &bytes.Buffer{}
			err := tc.compressor.Compress(buf, b)
			require.NoError(t, err)
			decompressed, err := tc.compressor.Decompress(buf.Bytes())
			require.NoError(t, err)
			assert.Equal(t, b, decompressed)
		})
	}
}

func TestZSTDPoolCapSelection(t *testing.T) {
	smallDst := make([]byte, 0, zstdPooledBufDefaultCap)
	smallDstPtr := &smallDst
	hugeDst := make([]byte, 0, zstdPooledBufMaxCap+1)
	hugeDstPtr := &hugeDst

	t.Run("sanitize-get", func(t *testing.T) {
		got := zstdPoolSanitizeGet(smallDstPtr)
		require.Equal(t, cap(smallDst), cap(got))

		got = zstdPoolSanitizeGet(hugeDstPtr)
		require.LessOrEqual(t, cap(got), zstdPooledBufMaxCap)
		require.Equal(t, zstdPooledBufDefaultCap, cap(got))
	})

	t.Run("put-small-compressed", func(t *testing.T) {
		compressed := make([]byte, 0, zstdPooledBufDefaultCap)
		gotPtr := zstdPoolBufForPut(smallDstPtr, compressed)
		got := *gotPtr
		require.LessOrEqual(t, cap(got), zstdPooledBufMaxCap)
		require.Equal(t, 0, len(got))
	})

	t.Run("put-huge-compressed", func(t *testing.T) {
		compressed := make([]byte, 0, zstdPooledBufMaxCap+1)
		gotPtr := zstdPoolBufForPut(smallDstPtr, compressed)
		got := *gotPtr
		require.LessOrEqual(t, cap(got), zstdPooledBufMaxCap)
		require.Equal(t, 0, len(got))
		require.Equal(t, cap(smallDst), cap(got))
	})
}

// TestZSTDDecompressAllocsRegression ensures the DecodeAll-based decompress
// path stays within allocation bounds. DecodeAll with a frame content size
// (written by EncodeAll) allocates exactly once for the output buffer at
// small/medium sizes (~1 alloc without race detector, ~7-11 with it).
//
// Thresholds are set to tolerate race detector overhead while catching a
// real regression: reverting to the old streaming path (Reset+io.Copy) would
// produce 20-29 allocs/op.
func TestZSTDDecompressAllocsRegression(t *testing.T) {
	c, err := NewZSTDCompressor()
	require.NoError(t, err)
	for _, tc := range []struct {
		name      string
		size      int
		maxAllocs int
	}{
		{"1KB", 1 << 10, 15},
		{"64KB", 64 << 10, 15},
		{"1MB", 1 << 20, 15},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := generateCompressibleData(tc.size)
			var buf bytes.Buffer
			require.NoError(t, c.Compress(&buf, data))
			compressed := buf.Bytes()

			allocs := testing.AllocsPerRun(100, func() {
				_, _ = c.Decompress(compressed)
			})
			assert.LessOrEqual(t, allocs, float64(tc.maxAllocs),
				"alloc regression: got %.0f, want <= %d "+
					"(old streaming path was 20-29; if close, check for accidental revert)",
				allocs, tc.maxAllocs)
		})
	}
}

// TestZSTDCompressAllocsRegression ensures the EncodeAll-based compress path
// stays within allocation bounds (~3-7 allocs without race detector, ~12-18
// with it).
func TestZSTDCompressAllocsRegression(t *testing.T) {
	c, err := NewZSTDCompressor()
	require.NoError(t, err)
	for _, tc := range []struct {
		name      string
		size      int
		maxAllocs int
	}{
		{"1KB", 1 << 10, 20},
		{"64KB", 64 << 10, 20},
		{"1MB", 1 << 20, 25},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := generateCompressibleData(tc.size)

			allocs := testing.AllocsPerRun(100, func() {
				var out bytes.Buffer
				_ = c.Compress(&out, data)
			})
			assert.LessOrEqual(t, allocs, float64(tc.maxAllocs),
				"alloc regression: got %.0f, want <= %d", allocs, tc.maxAllocs)
		})
	}
}

// TestZSTDBackwardCompatibility verifies that data compressed with the old
// streaming encoder (Write+Close, no content size in frame header) is still
// decodable by the current DecodeAll-based Decompress path.
//
// This matters during rollout: Kafka may contain messages written by the old
// code. DecodeAll handles both frame formats — with content size (exact alloc)
// and without (falls back to 2×input heuristic).
func TestZSTDBackwardCompatibility(t *testing.T) {
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20} {
		t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
			data := generateCompressibleData(size)

			// Encode with streaming Write+Close (old code path).
			// This produces a frame WITHOUT content size in the header.
			w, err := zstd.NewWriter(nil)
			require.NoError(t, err)
			var streamBuf bytes.Buffer
			w.Reset(&streamBuf)
			_, err = w.Write(data)
			require.NoError(t, err)
			require.NoError(t, w.Close())

			// Decode with the current DecodeAll path.
			c, err := NewZSTDCompressor()
			require.NoError(t, err)
			decoded, err := c.Decompress(streamBuf.Bytes())
			require.NoError(t, err)
			assert.Equal(t, data, decoded)
		})
	}
}

// TestZSTDForwardCompatibility verifies that data compressed with the current
// EncodeAll path (content size in frame header) is decodable by a streaming
// decoder (Reset+Read). This ensures we don't break downstream consumers that
// may still use the streaming API.
func TestZSTDForwardCompatibility(t *testing.T) {
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20} {
		t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
			data := generateCompressibleData(size)

			// Encode with current EncodeAll path.
			c, err := NewZSTDCompressor()
			require.NoError(t, err)
			var compBuf bytes.Buffer
			require.NoError(t, c.Compress(&compBuf, data))

			// Decode with streaming reader (old code path).
			r, err := zstd.NewReader(bytes.NewReader(compBuf.Bytes()))
			require.NoError(t, err)
			var decoded bytes.Buffer
			_, err = io.Copy(&decoded, r)
			require.NoError(t, err)
			r.Close()
			assert.Equal(t, data, decoded.Bytes())
		})
	}
}

// BenchmarkCompress benchmarks the compress path for each compressor.
func BenchmarkCompress(b *testing.B) {
	zstdCompressor, err := NewZSTDCompressor()
	require.NoError(b, err)
	for _, tc := range []struct {
		name       string
		compressor compressor
	}{
		{"lz4", LZ4Compressor{}},
		{"zstd", zstdCompressor},
	} {
		for _, size := range []int{1 << 10, 64 << 10, 1 << 20, 2 << 20, 4 << 20} {
			b.Run(fmt.Sprintf("%s/%dB", tc.name, size), func(b *testing.B) {
				data := generateCompressibleData(size)
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						var out bytes.Buffer
						if err := tc.compressor.Compress(&out, data); err != nil {
							b.Error(err)
							return
						}
					}
				})
			})
		}
	}
}

// BenchmarkDecompress benchmarks the decompress path for each compressor.
// This is the hot path on the aggregation consumer side (every Kafka message).
func BenchmarkDecompress(b *testing.B) {
	zstdCompressor, err := NewZSTDCompressor()
	require.NoError(b, err)
	for _, tc := range []struct {
		name       string
		compressor compressor
	}{
		{"lz4", LZ4Compressor{}},
		{"zstd", zstdCompressor},
	} {
		for _, size := range []int{1 << 10, 64 << 10, 1 << 20, 2 << 20, 4 << 20} {
			b.Run(fmt.Sprintf("%s/%dB", tc.name, size), func(b *testing.B) {
				data := generateCompressibleData(size)
				var buf bytes.Buffer
				err := tc.compressor.Compress(&buf, data)
				require.NoError(b, err)
				compressed := make([]byte, buf.Len())
				copy(compressed, buf.Bytes())

				// Verify roundtrip before benchmarking.
				got, err := tc.compressor.Decompress(compressed)
				require.NoError(b, err)
				require.Equal(b, data, got)

				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				b.ResetTimer()
				for b.Loop() {
					_, _ = tc.compressor.Decompress(compressed)
				}
			})
		}
	}
}

func BenchmarkCompressZSTDIncompressible(b *testing.B) {
	c, err := NewZSTDCompressor()
	require.NoError(b, err)
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20, 2 << 20, 4 << 20} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			data := generateIncompressibleData(size)
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					var out bytes.Buffer
					if err := c.Compress(&out, data); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkCompressZSTDLongLivedMixed simulates a long-lived compress workload
// with mostly compressible payloads and periodic larger compressible outliers.
// This helps exercise pool behavior under realistic steady-state traffic plus
// tail events that can otherwise contaminate pooled backing arrays.
func BenchmarkCompressZSTDLongLivedMixed(b *testing.B) {
	c, err := NewZSTDCompressor()
	require.NoError(b, err)
	small := generateCompressibleData(64 << 10)
	medium := generateCompressibleData(1 << 20)
	outlier := generateCompressibleData(4 << 20)
	const (
		mediumEvery  = 32
		outlierEvery = 512
	)

	var totalInput int64
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		payload := small
		switch {
		case i%outlierEvery == 0:
			payload = outlier
		case i%mediumEvery == 0:
			payload = medium
		}
		totalInput += int64(len(payload))
		var out bytes.Buffer
		if err := c.Compress(&out, payload); err != nil {
			b.Error(err)
			return
		}
	}
	b.ReportMetric(float64(totalInput)/float64(b.N), "input_B/op")

	// Report end-of-benchmark heap after GC as a rough retained-memory signal.
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	b.ReportMetric(float64(ms.HeapAlloc), "heap_B")
}

func BenchmarkCompressZSTDLongLivedMixedParallel(b *testing.B) {
	c, err := NewZSTDCompressor()
	require.NoError(b, err)
	small := generateCompressibleData(64 << 10)
	medium := generateCompressibleData(1 << 20)
	outlier := generateCompressibleData(4 << 20)
	const (
		mediumEvery  = 32
		outlierEvery = 512
	)

	var seq atomic.Uint64
	var totalInput atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(seq.Add(1))
			payload := small
			switch {
			case i%outlierEvery == 0:
				payload = outlier
			case i%mediumEvery == 0:
				payload = medium
			}
			totalInput.Add(int64(len(payload)))
			var out bytes.Buffer
			if err := c.Compress(&out, payload); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.ReportMetric(float64(totalInput.Load())/float64(b.N), "input_B/op")

	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	b.ReportMetric(float64(ms.HeapAlloc), "heap_B")
}

// generateCompressibleData creates data with patterns similar to serialized
// OTLP payloads (repetitive strings, attribute keys, metric names).
func generateCompressibleData(size int) []byte {
	data := make([]byte, size)
	pattern := []byte("service.name=test-service resource.attribute.key=value metric.name=test_metric_")
	for i := range data {
		data[i] = pattern[i%len(pattern)]
	}
	return data
}

func generateIncompressibleData(size int) []byte {
	data := make([]byte, size)
	var x uint64 = 0x243f6a8885a308d3
	for i := 0; i < len(data); i++ {
		// xorshift64*
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		x *= 2685821657736338717
		data[i] = byte(x)
	}
	return data
}
