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

package cachebench

import (
	"context"
	"os"
	"testing"
	"time"
)

const benchTTL = 30 * time.Second

// redisAddr returns the Redis address to benchmark against, or "" to skip the
// Redis backend. Defaults to localhost:6379 so a locally running container is
// picked up automatically.
func redisAddr() string {
	if v, ok := os.LookupEnv("CACHEBENCH_REDIS_ADDR"); ok {
		return v
	}
	return "localhost:6379"
}

// backendFactory builds a fresh backend for a benchmark, with the given per-pod
// capacity. Returns nil when the backend is unavailable (e.g. Redis down).
type backendFactory struct {
	name string
	make func(tb testing.TB, capacity uint32) Cache
}

func backends() []backendFactory {
	return []backendFactory{
		{
			name: "lru",
			make: func(tb testing.TB, capacity uint32) Cache {
				c, err := NewLRUCache(capacity, benchTTL)
				if err != nil {
					tb.Fatalf("lru: %v", err)
				}
				return c
			},
		},
		{
			name: "pebble",
			make: func(tb testing.TB, capacity uint32) Cache {
				c, err := NewPebbleCache(PebbleOptions{
					HotCapacity: capacity,
					TTL:         benchTTL,
					Dir:         tb.TempDir(),
				})
				if err != nil {
					tb.Fatalf("pebble: %v", err)
				}
				return c
			},
		},
		{
			name: "redis",
			make: func(tb testing.TB, capacity uint32) Cache {
				addr := redisAddr()
				if addr == "" {
					tb.Skip("redis disabled")
				}
				c, err := NewRedisCache(context.Background(), RedisOptions{
					Addr:       addr,
					L1Capacity: capacity,
					TTL:        benchTTL,
					PoolSize:   32,
				})
				if err != nil {
					tb.Skipf("redis unavailable at %s: %v", addr, err)
				}
				_ = c.Flush(context.Background())
				return c
			},
		},
	}
}

// BenchmarkGetHit measures the hot path: every key is already cached, so this
// is the best case each backend can achieve (L1 hit for the tiered backends).
func BenchmarkGetHit(b *testing.B) {
	// The sharded LRU splits capacity across shards, so it cannot hold exactly
	// `capacity` distinct keys without evicting from hot shards. Give 4x
	// headroom so preloaded keys stay resident and this measures a true hit
	// path (L1 hit for the tiered backends) rather than accidental spillover.
	const hitKeys = 50_000
	const capacity = 4 * hitKeys
	ctx := context.Background()
	w := NewWorkload(hitKeys, 0, 1)
	for _, bf := range backends() {
		b.Run(bf.name, func(b *testing.B) {
			c := bf.make(b, capacity)
			defer c.Close()
			for i := 0; i < hitKeys; i++ {
				k := w.KeyAt(i)
				c.Add(ctx, k, makeEntry(k))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// A rare miss from shard imbalance is tolerated (not re-added) so
				// this stays a pure Get benchmark.
				_, _ = c.Get(ctx, w.KeyAt(i%hitKeys))
			}
		})
	}
}

// BenchmarkGetMissThenAdd measures the miss path: a lookup that misses followed
// by the insert the authenticator does after the ES round trip. This is the
// cost paid on every cold key, which dominates under one-key-per-source.
func BenchmarkGetMissThenAdd(b *testing.B) {
	const capacity = 50_000
	ctx := context.Background()
	for _, bf := range backends() {
		b.Run(bf.name, func(b *testing.B) {
			c := bf.make(b, capacity)
			defer c.Close()
			w := NewWorkload(b.N+1, 0, 2)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := w.KeyAt(i)
				if _, ok := c.Get(ctx, k); !ok {
					c.Add(ctx, k, makeEntry(k))
				}
			}
		})
	}
}

// BenchmarkParallelZipf measures throughput under concurrent load with a skewed
// access pattern over a working set larger than the per-pod capacity, which is
// the realistic ingest hot path.
func BenchmarkParallelZipf(b *testing.B) {
	const (
		capacity     = 65_536
		keyspaceSize = 500_000
	)
	ctx := context.Background()
	for _, bf := range backends() {
		b.Run(bf.name, func(b *testing.B) {
			c := bf.make(b, capacity)
			defer c.Close()
			w := NewWorkload(keyspaceSize, 1.1, 3)
			// Warm the cache so the steady state, not the cold start, is measured.
			for i := 0; i < capacity; i++ {
				k := w.Next()
				c.Add(ctx, k, makeEntry(k))
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				local := NewWorkload(keyspaceSize, 1.1, uint64(time.Now().UnixNano()))
				for pb.Next() {
					k := local.Next()
					if _, ok := c.Get(ctx, k); !ok {
						c.Add(ctx, k, makeEntry(k))
					}
				}
			})
		})
	}
}
