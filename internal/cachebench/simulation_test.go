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
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

// simTTL is long enough that nothing expires mid-run, so the simulation
// isolates capacity and sharing effects from time-based eviction.
const simTTL = 30 * time.Minute

// simParams describes a fleet workload.
type simParams struct {
	keyspaceSize int     // active sources (= unique keys, one per source)
	pods         int     // ingest replicas
	requests     int     // total auth requests across the fleet
	zipfS        float64 // access skew; 0 = uniform
	perPodCap    uint32  // per-pod cache capacity (L1 for tiered backends)
}

// simResult is what each backend produces for a fleet workload.
type simResult struct {
	backend   string
	esCalls   int           // misses that reached Elasticsearch (the cost to minimize)
	hitRatio  float64       // 1 - esCalls/requests
	elapsed   time.Duration // wall time to serve the stream (includes L2 network for redis)
	perPodMem int           // approx resident bytes per pod (L1/in-mem tier)
	sharedMem int           // approx resident bytes in the shared tier (redis L2), 0 otherwise
	diskNote  string        // qualitative note for the on-disk backend
}

// pod is one replica's view of the cache. Shared-nothing backends get one Cache
// each; the Redis backend shares a single logical L2 across all pods.
type podSet struct {
	caches   []Cache
	esCalls  int
	closeAll func()
}

// buildFleet constructs P pod caches for a backend. For lru and pebble each pod
// is independent (shared-nothing). For redis every pod is a separate client
// with its own L1 but the same flushed shared L2.
func buildFleet(tb testing.TB, backend string, p simParams) (*podSet, bool) {
	caches := make([]Cache, p.pods)
	switch backend {
	case "lru":
		for i := range caches {
			c, err := NewLRUCache(p.perPodCap, simTTL)
			if err != nil {
				tb.Fatalf("lru: %v", err)
			}
			caches[i] = c
		}
	case "pebble":
		for i := range caches {
			c, err := NewPebbleCache(PebbleOptions{
				HotCapacity: p.perPodCap,
				TTL:         simTTL,
				Dir:         tb.TempDir(),
			})
			if err != nil {
				tb.Fatalf("pebble: %v", err)
			}
			caches[i] = c
		}
	case "redis":
		addr := redisAddr()
		if addr == "" {
			return nil, false
		}
		for i := range caches {
			c, err := NewRedisCache(context.Background(), RedisOptions{
				Addr:       addr,
				L1Capacity: p.perPodCap,
				TTL:        simTTL,
				KeyPrefix:  "sim:",
				PoolSize:   16,
			})
			if err != nil {
				tb.Skipf("redis unavailable at %s: %v", addr, err)
			}
			if i == 0 {
				if err := c.Flush(context.Background()); err != nil {
					tb.Fatalf("redis flush: %v", err)
				}
			}
			caches[i] = c
		}
	}
	return &podSet{
		caches: caches,
		closeAll: func() {
			for _, c := range caches {
				_ = c.Close()
			}
		},
	}, true
}

// runFleet replays an identical request stream against a backend's fleet and
// counts the misses that reach Elasticsearch.
func runFleet(tb testing.TB, backend string, p simParams) (simResult, bool) {
	ps, ok := buildFleet(tb, backend, p)
	if !ok {
		return simResult{}, false
	}
	defer ps.closeAll()

	ctx := context.Background()
	// One shared stream, deterministic per params, so every backend is judged
	// on the exact same sequence of (pod, key) requests.
	stream := rand.New(rand.NewPCG(42, 99))
	w := NewWorkload(p.keyspaceSize, p.zipfS, 7)

	esCalls := 0
	start := time.Now()
	for i := 0; i < p.requests; i++ {
		pod := stream.IntN(p.pods)
		key := w.Next()
		c := ps.caches[pod]
		if _, hit := c.Get(ctx, key); !hit {
			esCalls++ // an ES _has_privileges round trip would happen here
			c.Add(ctx, key, makeEntry(key))
		}
	}
	elapsed := time.Since(start)

	res := simResult{
		backend:  backend,
		esCalls:  esCalls,
		hitRatio: 1 - float64(esCalls)/float64(p.requests),
		elapsed:  elapsed,
	}
	// Memory accounting is approximate and meant for relative comparison.
	residentPerPod := min(int(p.perPodCap), p.keyspaceSize)
	switch backend {
	case "lru":
		res.perPodMem = residentPerPod * approxEntrySize
	case "pebble":
		res.perPodMem = residentPerPod * approxEntrySize
		res.diskNote = fmt.Sprintf("+~%dMB on-disk per pod (full working set)",
			p.keyspaceSize*approxEntrySize/(1<<20))
	case "redis":
		res.perPodMem = residentPerPod * approxEntrySize
		res.sharedMem = p.keyspaceSize * approxEntrySize // one shared copy
	}
	return res, true
}

// TestFleetSimulation is the decision harness. It reports, per backend, the
// number of Elasticsearch calls (the quantity to minimize under
// one-key-per-source), the effective hit ratio, wall time, and memory model.
// Run with: go test -run TestFleetSimulation -v
func TestFleetSimulation(t *testing.T) {
	scenarios := []struct {
		name   string
		params simParams
	}{
		{
			name: "uniform_200k_keys_10_pods_cap8192",
			params: simParams{
				keyspaceSize: 200_000,
				pods:         10,
				requests:     2_000_000,
				zipfS:        0,
				perPodCap:    8192,
			},
		},
		{
			name: "zipf_200k_keys_10_pods_cap8192",
			params: simParams{
				keyspaceSize: 200_000,
				pods:         10,
				requests:     2_000_000,
				zipfS:        1.2,
				perPodCap:    8192,
			},
		},
		{
			name: "uniform_200k_keys_10_pods_cap65536",
			params: simParams{
				keyspaceSize: 200_000,
				pods:         10,
				requests:     2_000_000,
				zipfS:        0,
				perPodCap:    65536,
			},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Logf("params: keyspace=%d pods=%d requests=%d zipfS=%.2f perPodCap=%d",
				sc.params.keyspaceSize, sc.params.pods, sc.params.requests,
				sc.params.zipfS, sc.params.perPodCap)
			t.Logf("%-14s %12s %9s %11s %13s %s",
				"backend", "es_calls", "hit%", "wall", "mem/pod", "notes")
			for _, backend := range []string{"lru", "pebble", "redis"} {
				res, ok := runFleet(t, backend, sc.params)
				if !ok {
					t.Logf("%-14s %12s (skipped: backend unavailable)", backend, "-")
					continue
				}
				notes := res.diskNote
				if res.sharedMem > 0 {
					notes = fmt.Sprintf("+~%dMB shared L2 (one copy for fleet)",
						res.sharedMem/(1<<20))
				}
				t.Logf("%-14s %12d %8.2f%% %11s %10dKB %s",
					res.backend, res.esCalls, res.hitRatio*100,
					res.elapsed.Round(time.Millisecond), res.perPodMem/1024, notes)
			}
		})
	}
}
