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

package ratelimitprocessor

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/processor/processortest"
)

// newTestLocalRateLimiter creates a new localRateLimiter.
func newTestLocalRateLimiter(t *testing.T, cfg *Config) *localRateLimiter {
	if cfg == nil {
		cfg = createDefaultConfig().(*Config)
	}
	rl, err := newLocalRateLimiter(cfg, processortest.NewNopSettings(metadata.Type))
	require.NoError(t, err)
	t.Cleanup(func() {
		err := rl.Shutdown(context.Background())
		assert.NoError(t, err)
	})
	return rl
}

func TestLocalRateLimiter_StartStop(t *testing.T) {
	rateLimiter := newTestLocalRateLimiter(t, nil)

	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	err = rateLimiter.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestLocalRateLimiter_RateLimit(t *testing.T) {
	for _, behavior := range []ThrottleBehavior{ThrottleBehaviorError, ThrottleBehaviorDelay} {
		t.Run(string(behavior), func(t *testing.T) {
			burst := 2
			rateLimiter := newTestLocalRateLimiter(t, &Config{
				RateLimitSettings: RateLimitSettings{
					Rate: 10, Burst: burst, ThrottleBehavior: behavior,
				},
			})
			err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
			require.NoError(t, err)

			startTime := time.Now()

			for i := 0; i < burst; i++ {
				err = rateLimiter.RateLimit(context.Background(), 1) // should pass
				assert.NoError(t, err)
			}

			err = rateLimiter.RateLimit(context.Background(), 1) // should fail
			switch behavior {
			case ThrottleBehaviorError:
				assert.EqualError(t, err, "rpc error: code = ResourceExhausted desc = too many requests")
				// retry every 20ms to ensure that RateLimit will recover from error when bucket refills after 1 second
				assert.EventuallyWithT(t, func(collect *assert.CollectT) {
					assert.NoError(collect, rateLimiter.RateLimit(context.Background(), 1))
				}, 2*time.Second, 20*time.Millisecond)
			case ThrottleBehaviorDelay:
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, time.Now(), startTime.Add(100*time.Millisecond))
			}
		})
	}
}

func TestLocalRateLimiter_RateLimit_MetadataKeys(t *testing.T) {
	burst := 2
	rateLimiter := newTestLocalRateLimiter(t, &Config{
		RateLimitSettings: RateLimitSettings{
			Rate:             1,
			Burst:            burst,
			ThrottleBehavior: ThrottleBehaviorError,
		},
		MetadataKeys: []string{"metadata_key"},
	})
	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	clientContext1 := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"metadata_key": {"value1"},
		}),
	})
	clientContext2 := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"metadata_key": {"value2"},
		}),
	})

	// Each unique combination of metadata keys should get its own rate limit.
	for i := 0; i < burst; i++ {
		err = rateLimiter.RateLimit(clientContext1, 1) // should pass
		assert.NoError(t, err)
		err = rateLimiter.RateLimit(clientContext2, 1) // should pass
		assert.NoError(t, err)
	}
}

// TestLocalRateLimiter_HitsExceedsBurst_Delay verifies that a single batch
// larger than burst is handled by delay mode (split internally) instead of
// being rejected. With rate=100/s and burst=10, a request for 100 hits
// requires ~900ms (10 immediate + 90 future tokens at 100/s).
func TestLocalRateLimiter_HitsExceedsBurst_Delay(t *testing.T) {
	rl := newTestLocalRateLimiter(t, &Config{
		RateLimitSettings: RateLimitSettings{
			Rate: 100, Burst: 10, ThrottleBehavior: ThrottleBehaviorDelay,
			ThrottleInterval: 100 * time.Millisecond,
			RetryDelay:       100 * time.Millisecond,
		},
	})

	start := time.Now()
	err := rl.RateLimit(context.Background(), 100)
	require.NoError(t, err)
	elapsed := time.Since(start)

	// 100 hits at 100/s with burst=10: ~900ms expected. Allow generous
	// margins for CI timing jitter.
	assert.GreaterOrEqual(t, elapsed, 800*time.Millisecond,
		"expected at least ~900ms wait, got %v", elapsed)
	assert.Less(t, elapsed, 1500*time.Millisecond,
		"expected ~900ms wait, got %v (something is very slow)", elapsed)
}

// TestLocalRateLimiter_HitsExceedsBurst_Error verifies that error mode
// continues to reject hits > burst (no behavior change vs. before the
// chunked-reservation fix).
func TestLocalRateLimiter_HitsExceedsBurst_Error(t *testing.T) {
	rl := newTestLocalRateLimiter(t, &Config{
		RateLimitSettings: RateLimitSettings{
			Rate: 100, Burst: 10, ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: 100 * time.Millisecond,
			RetryDelay:       100 * time.Millisecond,
		},
	})
	err := rl.RateLimit(context.Background(), 100)
	assert.EqualError(t, err, "rpc error: code = ResourceExhausted desc = too many requests")
}

// TestLocalRateLimiter_FIFO_HitsExceedsBurst spawns multiple concurrent
// callers each with hits > burst, staggered by a small delay so their
// arrival order is deterministic. With the per-key reserveLock holding
// chunked ReserveN calls atomically, callers should complete in arrival
// order — not at roughly the same time as a non-atomic chunked WaitN
// loop would produce.
func TestLocalRateLimiter_FIFO_HitsExceedsBurst(t *testing.T) {
	rl := newTestLocalRateLimiter(t, &Config{
		RateLimitSettings: RateLimitSettings{
			Rate: 100, Burst: 10, ThrottleBehavior: ThrottleBehaviorDelay,
			ThrottleInterval: 100 * time.Millisecond,
			RetryDelay:       100 * time.Millisecond,
		},
	})

	const N = 4
	completions := make([]time.Time, N)

	var wg sync.WaitGroup
	wg.Add(N)

	// Stagger launches so callers arrive in known order. The stagger
	// (20ms) is much smaller than the per-batch wait (~200ms per
	// additional 20-hit caller), so arrival order survives scheduling
	// jitter.
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			err := rl.RateLimit(context.Background(), 20)
			require.NoError(t, err)
			completions[i] = time.Now()
		}(i)
		time.Sleep(20 * time.Millisecond)
	}
	wg.Wait()

	for i := 1; i < N; i++ {
		assert.True(t, completions[i].After(completions[i-1]),
			"goroutine %d completed at %v, before goroutine %d at %v (FIFO violated)",
			i, completions[i], i-1, completions[i-1])
	}

	// Successive completions should be ~200ms apart (20 hits / 100 per
	// second). Allow generous slack.
	for i := 1; i < N; i++ {
		gap := completions[i].Sub(completions[i-1])
		assert.GreaterOrEqual(t, gap, 100*time.Millisecond,
			"gap between completion %d and %d is %v, expected >= ~200ms",
			i-1, i, gap)
	}
}

// TestLocalRateLimiter_MixedBatchSizes verifies that small and large
// batches both make progress under concurrent load — small batches are
// not starved waiting for large ones, and total throughput is bounded
// by rate*time.
func TestLocalRateLimiter_MixedBatchSizes(t *testing.T) {
	rl := newTestLocalRateLimiter(t, &Config{
		RateLimitSettings: RateLimitSettings{
			Rate: 100, Burst: 10, ThrottleBehavior: ThrottleBehaviorDelay,
			ThrottleInterval: 100 * time.Millisecond,
			RetryDelay:       100 * time.Millisecond,
		},
	})

	// Total tokens: 4*5 + 4*20 = 100. At 100/s with burst=10, ~900ms.
	const smallHits, largeHits, eachKind = 5, 20, 4

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(eachKind * 2)
	for i := 0; i < eachKind; i++ {
		go func() {
			defer wg.Done()
			require.NoError(t, rl.RateLimit(context.Background(), smallHits))
		}()
		go func() {
			defer wg.Done()
			require.NoError(t, rl.RateLimit(context.Background(), largeHits))
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Throughput invariant: total elapsed >= (totalHits - burst) / rate.
	// The initial bucket of `burst` tokens absorbs the first burst hits
	// for free, so minimum wait is (totalHits - burst) / rate.
	const totalHits = eachKind * (smallHits + largeHits)
	minExpected := time.Duration(float64(totalHits-10)/100.0*float64(time.Second)) -
		50*time.Millisecond
	assert.GreaterOrEqual(t, elapsed, minExpected,
		"elapsed %v less than throughput floor %v", elapsed, minExpected)
}

// TestLocalRateLimiter_ContextCancellation_DuringDelay verifies that a
// caller whose context is cancelled while waiting returns ctx.Err() and
// returns its tokens via CancelAt — visible as a shorter wait for the
// next caller.
func TestLocalRateLimiter_ContextCancellation_DuringDelay(t *testing.T) {
	rl := newTestLocalRateLimiter(t, &Config{
		RateLimitSettings: RateLimitSettings{
			Rate: 10, Burst: 10, ThrottleBehavior: ThrottleBehaviorDelay,
			ThrottleInterval: 100 * time.Millisecond,
			RetryDelay:       100 * time.Millisecond,
		},
	})

	// Drain the bucket with one large request, cancel its context
	// mid-wait. The reservation must be cancelled so subsequent callers
	// don't inherit a phantom deficit.
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- rl.RateLimit(ctx, 30) // would take ~2s
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("RateLimit did not return after context cancellation")
	}

	// A small request after cancellation should succeed quickly because
	// the cancelled reservation released most of its tokens.
	start := time.Now()
	require.NoError(t, rl.RateLimit(context.Background(), 5))
	elapsed := time.Since(start)
	assert.Less(t, elapsed, 500*time.Millisecond,
		"post-cancellation request took %v; cancelled tokens may not have been released", elapsed)
}

// TestLocalRateLimiter_PerKeyIsolation_HitsExceedsBurst verifies that
// the per-key reserveLock does not block callers using different
// metadata keys: each key has its own keyState and its own lock.
func TestLocalRateLimiter_PerKeyIsolation_HitsExceedsBurst(t *testing.T) {
	rl := newTestLocalRateLimiter(t, &Config{
		RateLimitSettings: RateLimitSettings{
			Rate: 100, Burst: 10, ThrottleBehavior: ThrottleBehaviorDelay,
			ThrottleInterval: 100 * time.Millisecond,
			RetryDelay:       100 * time.Millisecond,
		},
		MetadataKeys: []string{"tenant"},
	})

	ctx1 := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{"tenant": {"a"}}),
	})
	ctx2 := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{"tenant": {"b"}}),
	})

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		require.NoError(t, rl.RateLimit(ctx1, 100))
	}()
	go func() {
		defer wg.Done()
		require.NoError(t, rl.RateLimit(ctx2, 100))
	}()
	wg.Wait()
	elapsed := time.Since(start)

	// Each tenant's bucket is independent. Both 100-hit batches should
	// finish in ~900ms in parallel — not ~1.8s as they would if they
	// serialized.
	assert.Less(t, elapsed, 1300*time.Millisecond,
		"parallel calls on different keys took %v; per-key isolation may be broken", elapsed)
	assert.GreaterOrEqual(t, elapsed, 800*time.Millisecond,
		"calls completed in %v; rate limit may not be enforced", elapsed)
}

func TestLocalRateLimiter_MultipleRequests_Delay(t *testing.T) {
	throttleInterval := 100 * time.Millisecond
	rl := newTestLocalRateLimiter(t, &Config{
		RateLimitSettings: RateLimitSettings{
			Rate:             1, // request per second
			Burst:            1, // capacity only for one
			ThrottleBehavior: ThrottleBehaviorDelay,
			ThrottleInterval: throttleInterval, // add 1 token after 100ms
		},
		MetadataKeys: []string{"metadata_key"},
	})

	// Simulate 4 requests hitting the rate limit simultaneously.
	// The first request passes, and the next ones hit it simultaneously.
	requests := 5
	endingTimes := make([]time.Time, requests)
	var wg sync.WaitGroup
	wg.Add(requests)

	for i := 0; i < requests; i++ {
		go func(i int) {
			defer wg.Done()
			err := rl.RateLimit(context.Background(), 1)
			require.NoError(t, err)
			endingTimes[i] = time.Now()
		}(i)
	}
	wg.Wait()

	// Make sure all ending times have a difference of at least 100ms, as tokens are
	// added at that rate. We need to sort them first.
	slices.SortFunc(endingTimes, func(a, b time.Time) int {
		if a.Before(b) {
			return -1
		}
		return 1
	})

	for i := 1; i < requests; i++ {
		diff := endingTimes[i].Sub(endingTimes[i-1]).Milliseconds()
		minExpected := throttleInterval - 5*time.Millisecond // allow small tolerance
		if diff < minExpected.Milliseconds() {
			t.Fatalf("difference is %dms, requests were sent before tokens were added", diff)
		}
	}
}
