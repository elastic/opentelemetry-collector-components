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
	"testing"
	"time"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"
	"github.com/gubernator-io/gubernator/v2"
	"github.com/gubernator-io/gubernator/v2/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/processor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// newTestGubernatorRateLimiter starts a local cluster with a gubernator
// daemon and returns a new gubernatorRateLimiter instance that relies
// on this daemon for rate limiting.
func newTestGubernatorRateLimiter(t *testing.T, cfg *Config, c chan<- gubernator.HitEvent) *gubernatorRateLimiter {
	rl := newGubernatorRateLimiterFrom(t, cfg,
		startGubernatorCluster(t, c),
	)
	t.Cleanup(func() {
		// Wait a bit after the test to shut down the daemon.
		time.Sleep(50 * time.Millisecond)
		err := rl.Shutdown(context.Background())
		require.NoError(t, err)
		cluster.Stop()
	})
	return rl
}

func newGubernatorRateLimiterFrom(t *testing.T, cfg *Config, daemon *gubernator.Daemon) *gubernatorRateLimiter {
	conn, err := grpc.NewClient(daemon.PeerInfo.GRPCAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	cl := gubernator.NewV1Client(conn)
	require.NotNil(t, cl)

	return &gubernatorRateLimiter{
		cfg: cfg,
		set: processor.Settings{
			ID:                component.NewIDWithName(metadata.Type, "abc123"),
			TelemetrySettings: componenttest.NewNopTelemetrySettings(),
			BuildInfo:         component.NewDefaultBuildInfo(),
		},
		behavior: gubernator.Behavior_BATCHING,

		daemon:     daemon,
		client:     cl,
		clientConn: conn,
	}
}

func startGubernatorCluster(t *testing.T, c chan<- gubernator.HitEvent) *gubernator.Daemon {
	var err error
	const local = "127.0.0.1:0"
	peers := []gubernator.PeerInfo{{GRPCAddress: local, HTTPAddress: local}}
	if c != nil {
		err = cluster.StartWith(peers, cluster.WithEventChannel(c))
	} else {
		err = cluster.StartWith(peers)
	}
	require.NoError(t, err)

	daemons := cluster.GetDaemons()
	require.Equal(t, 1, len(daemons))
	return daemons[0]
}

func TestGubernatorRateLimiter_RateLimit_Dynamic_Simple(t *testing.T) {
	const EWMAWindowPeriod = 500 * time.Millisecond
	const eventCount = 62
	eventChannel := make(chan gubernator.HitEvent, eventCount)
	rateLimiter := newTestGubernatorRateLimiter(t, &Config{
		Type: GubernatorRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Strategy:         StrategyRateLimitBytes,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: EWMAWindowPeriod,
			Rate:             5_000_000, // Always allow 5MB,
		},
		DynamicRateLimiting: DynamicRateLimiting{
			Enabled:            true,
			EWMAMultiplier:     1.5,
			EWMAWindow:         EWMAWindowPeriod,
			RecentWindowWeight: 0.7, // 70%.
		},
	}, eventChannel)

	waitUntilNextPeriod(EWMAWindowPeriod)

	// The first request should be allowed, as it is below the static threshold.
	err := rateLimiter.RateLimit(context.Background(), 2_000_000)
	assert.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit, maxDynamicLimit)
	assertDynamicAct(t, <-eventChannel, 2_000_000, maxDynamicLimit-2_000_000)
	assertRateLimitEvent(t, <-eventChannel, 2_000_000, 5_000_000, 3_000_000, gubernator.Status_UNDER_LIMIT)

	// The second request should also be allowed, as the EWMA is still low.
	err = rateLimiter.RateLimit(context.Background(), 3_000_000)
	assert.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit, maxDynamicLimit-2_000_000)
	assertDynamicAct(t, <-eventChannel, 3_000_000, maxDynamicLimit-5_000_000)
	assertRateLimitEvent(t, <-eventChannel, 3_000_000, 5_000_000, 120_000, gubernator.Status_UNDER_LIMIT)

	// After this request, the current rate should be 5MB/s.

	// Issue two more requests of 2MB each, and the current rate should not
	// be incremented afterwards.
	for i := 0; i < 2; i++ {
		err = rateLimiter.RateLimit(context.Background(), 2_000_000)
		assert.EqualError(t, err, "rpc error: code = ResourceExhausted desc = too many requests")
	}
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-5_000_000, maxDynamicLimit)
	assertRateLimitEvent(t, <-eventChannel, 2_000_000, 5_000_000, 180_000, gubernator.Status_OVER_LIMIT)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-5_000_000, maxDynamicLimit)
	assertRateLimitEvent(t, <-eventChannel, 2_000_000, 5_000_000, 210_000, gubernator.Status_OVER_LIMIT)

	// Wait for the next period so the current rate unique key is rotated
	// and becomes the previous.
	waitUntilNextPeriod(EWMAWindowPeriod)

	// Now request 10MB which should be accepted since the last period
	// rate was 5MB and the limits are to up to previous * 1.5.
	err = rateLimiter.RateLimit(context.Background(), 10_000_000)
	require.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-5_000_000, maxDynamicLimit)
	assertDynamicAct(t, <-eventChannel, 10_000_000, maxDynamicLimit-10_000_000)
	assertRateLimitEvent(t, <-eventChannel, 10_000_000, 15_000_000, 5_000_000, gubernator.Status_UNDER_LIMIT)

	// Wait another period so the key is rotated.
	waitUntilNextPeriod(EWMAWindowPeriod)

	// Now issue a 20MB request, which should be accepted since the last
	// period rate was 10MB and the limits are to up to previous * 1.5.
	err = rateLimiter.RateLimit(context.Background(), 20_000_000)
	require.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-10_000_000, maxDynamicLimit)
	assertDynamicAct(t, <-eventChannel, 20_000_000, maxDynamicLimit-20_000_000)
	assertRateLimitEvent(t, <-eventChannel, 20_000_000, 30_000_000, 10_000_000, gubernator.Status_UNDER_LIMIT)

	// Now issue two 5MB requests and wait until the next period, then issue
	// 2 3MB requests.
	err = rateLimiter.RateLimit(context.Background(), 5_000_000)
	require.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-20_000_000, maxDynamicLimit-10_000_000)
	assertDynamicAct(t, <-eventChannel, 5_000_000, maxDynamicLimit-25_000_000)
	assertRateLimitEvent(t, <-eventChannel, 5_000_000, 30_000_000, 5_360_000, gubernator.Status_UNDER_LIMIT)

	err = rateLimiter.RateLimit(context.Background(), 5_000_000)
	require.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-10_000_000, maxDynamicLimit-25_000_000)
	assertDynamicAct(t, <-eventChannel, 5_000_000, maxDynamicLimit-30_000_000)
	assertRateLimitEvent(t, <-eventChannel, 5_000_000, 30_000_000, 780_000, gubernator.Status_UNDER_LIMIT)

	waitUntilNextPeriod(EWMAWindowPeriod)

	err = rateLimiter.RateLimit(context.Background(), 3_000_000)
	require.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-30_000_000, maxDynamicLimit)
	assertDynamicAct(t, <-eventChannel, 3_000_000, maxDynamicLimit-3_000_000)
	assertRateLimitEvent(t, <-eventChannel, 3_000_000, 33_300_000, 30_300_000, gubernator.Status_UNDER_LIMIT)

	err = rateLimiter.RateLimit(context.Background(), 3_000_000)
	require.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-30_000_000, maxDynamicLimit-3_000_000)
	assertDynamicAct(t, <-eventChannel, 3_000_000, maxDynamicLimit-6_000_000)
	assertRateLimitEvent(t, <-eventChannel, 3_000_000, 39_600_000, 36_600_000, gubernator.Status_UNDER_LIMIT)

	waitUntilNextPeriod(EWMAWindowPeriod)

	err = rateLimiter.RateLimit(context.Background(), 3_000_000)
	require.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-6_000_000, maxDynamicLimit)
	assertDynamicAct(t, <-eventChannel, 3_000_000, maxDynamicLimit-3_000_000)
	assertRateLimitEvent(t, <-eventChannel, 3_000_000, 11_700_000, 8_700_000, gubernator.Status_UNDER_LIMIT)

	err = rateLimiter.RateLimit(context.Background(), 3_000_000)
	require.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-3_000_000, maxDynamicLimit-6_000_000)
	assertDynamicAct(t, <-eventChannel, 3_000_000, maxDynamicLimit-6_000_000)
	assertRateLimitEvent(t, <-eventChannel, 3_000_000, 18_000_000, 15_000_000, gubernator.Status_UNDER_LIMIT)

	waitUntilNextPeriod(EWMAWindowPeriod)

	err = rateLimiter.RateLimit(context.Background(), 1_000_000)
	require.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-6_000_000, maxDynamicLimit)
	assertDynamicAct(t, <-eventChannel, 1_000_000, maxDynamicLimit-1_000_000)
	assertRateLimitEvent(t, <-eventChannel, 1_000_000, 7_500_000, 6_500_000, gubernator.Status_UNDER_LIMIT)

	err = rateLimiter.RateLimit(context.Background(), 1_000_000)
	require.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-6_000_000, maxDynamicLimit-1_000_000)
	assertDynamicAct(t, <-eventChannel, 1_000_000, maxDynamicLimit-2_000_000)
	assertRateLimitEvent(t, <-eventChannel, 1_000_000, 9_600_000, 8_600_000, gubernator.Status_UNDER_LIMIT)

	waitUntilNextPeriod(EWMAWindowPeriod)

	err = rateLimiter.RateLimit(context.Background(), 1_000_000)
	require.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-2_000_000, maxDynamicLimit)
	assertDynamicAct(t, <-eventChannel, 1_000_000, maxDynamicLimit-1_000_000)
	assertRateLimitEvent(t, <-eventChannel, 1_000_000, 5_000_000, 4_000_000, gubernator.Status_UNDER_LIMIT)

	err = rateLimiter.RateLimit(context.Background(), 1_000_000)
	require.NoError(t, err)
	assertDynamicPeekPair(t, <-eventChannel, <-eventChannel, maxDynamicLimit-1_000_000, maxDynamicLimit-2_000_000)
	assertDynamicAct(t, <-eventChannel, 1_000_000, maxDynamicLimit-2_000_000)
	assertRateLimitEvent(t, <-eventChannel, 1_000_000, 6_000_000, 5_000_000, gubernator.Status_UNDER_LIMIT)

	assert.Empty(t, eventChannel)
}

func assertDynamicPeekPair(t *testing.T, event1, event2 gubernator.HitEvent, expectedRemaining1, expectedRemaining2 int64) {
	t.Helper()
	// The order of the peek events is not guaranteed, so we need to check both possibilities.
	if event1.Response.Remaining == expectedRemaining1 {
		assertDynamicPeek(t, event1, expectedRemaining1)
		assertDynamicPeek(t, event2, expectedRemaining2)
	} else {
		assertDynamicPeek(t, event1, expectedRemaining2)
		assertDynamicPeek(t, event2, expectedRemaining1)
	}
}

func assertDynamicPeek(t *testing.T, event gubernator.HitEvent, expectedRemaining int64) {
	t.Helper()
	assert.Equal(t, "dynamic", event.Request.Name, "request.name")
	assert.Contains(t, event.Request.UniqueKey, "default-", "request.unique_key")
	assert.Equal(t, int64(0), event.Request.Hits, "request.hits")
	assert.Equal(t, maxDynamicLimit, event.Request.Limit, "request.limit")
	assert.Equal(t, maxDynamicLimit, event.Request.Burst, "request.burst")
	assert.Equal(t, gubernator.Algorithm_TOKEN_BUCKET, event.Request.Algorithm, "request.algorithm")
	assert.Equal(t, gubernator.Behavior_BATCHING, event.Request.Behavior, "request.behavior")
	assert.NotZero(t, *event.Request.CreatedAt, "request.created_at")

	assert.Equal(t, maxDynamicLimit, event.Response.Limit, "response.limit")
	assert.Equal(t, expectedRemaining, event.Response.Remaining, "response.remaining")
	assert.GreaterOrEqual(t, event.Response.ResetTime, *event.Request.CreatedAt, "response.reset_time")
}

func assertDynamicAct(t *testing.T, event gubernator.HitEvent, expectedHits, expectedRemaining int64) {
	t.Helper()
	assert.Equal(t, "dynamic", event.Request.Name, "request.name")
	assert.Contains(t, event.Request.UniqueKey, "default-", "request.unique_key")
	assert.Equal(t, expectedHits, event.Request.Hits, "request.hits")
	assert.Equal(t, maxDynamicLimit, event.Request.Limit, "request.limit")
	assert.Equal(t, maxDynamicLimit, event.Request.Burst, "request.burst")
	assert.Equal(t, gubernator.Algorithm_TOKEN_BUCKET, event.Request.Algorithm, "request.algorithm")
	assert.Equal(t, gubernator.Behavior_BATCHING, event.Request.Behavior, "request.behavior")
	assert.NotZero(t, *event.Request.CreatedAt, "request.created_at")

	assert.Equal(t, maxDynamicLimit, event.Response.Limit, "response.limit")
	assert.Equal(t, expectedRemaining, event.Response.Remaining, "response.remaining")
	assert.GreaterOrEqual(t, event.Response.ResetTime, *event.Request.CreatedAt, "response.reset_time")
}

func assertRateLimitEvent(t *testing.T, event gubernator.HitEvent, expectedHits, expectedLimit, expectedRemaining int64, expectedStatus gubernator.Status) {
	t.Helper()
	assert.Equal(t, "bytes_per_sec", event.Request.Name, "request.name")
	assert.Equal(t, "default", event.Request.UniqueKey, "request.unique_key")
	assert.Equal(t, expectedHits, event.Request.Hits, "request.hits")
	assert.Equal(t, expectedLimit, event.Request.Limit, "request.limit")
	assert.Equal(t, expectedLimit, event.Request.Burst, "request.burst")
	assert.Equal(t, gubernator.Algorithm_LEAKY_BUCKET, event.Request.Algorithm, "request.algorithm")
	assert.Equal(t, gubernator.Behavior_BATCHING, event.Request.Behavior, "request.behavior")
	assert.NotZero(t, *event.Request.CreatedAt, "request.created_at")

	assert.Equal(t, expectedLimit, event.Response.Limit, "response.limit")
	// Leaky bucket is trickier to calculate the remaining since the rate is
	// not constant and keeps decreasing. So depending on the latency between
	// calls, the remaining might be slightly different. This is why we allow
	// a delta of 550_000.
	assert.InDelta(t, expectedRemaining, event.Response.Remaining, 550_000, "response.remaining")
	assert.Equal(t, expectedStatus, event.Response.Status, "response.status")
	assert.GreaterOrEqual(t, event.Response.ResetTime, *event.Request.CreatedAt, "response.reset_time")
}

func waitUntilNextPeriod(interval time.Duration) {
	// To ensure we are in the next interval, we wait for the current interval to
	// pass, and then we wait for the next interval to start.
	time.Sleep(time.Until(time.Now().Truncate(interval).Add(interval)))
	time.Sleep(10 * time.Millisecond)
}

func TestGubernatorRateLimiter_RateLimit(t *testing.T) {
	for _, behavior := range []ThrottleBehavior{ThrottleBehaviorError, ThrottleBehaviorDelay} {
		t.Run(string(behavior), func(t *testing.T) {
			rateLimiter := newTestGubernatorRateLimiter(t, &Config{
				Type: GubernatorRateLimiter,
				RateLimitSettings: RateLimitSettings{
					Strategy:         StrategyRateLimitRequests,
					Rate:             1,
					Burst:            2,
					ThrottleBehavior: behavior,
					ThrottleInterval: time.Second,
				},
			}, nil)

			err := rateLimiter.RateLimit(context.Background(), 1)
			assert.NoError(t, err)

			err = rateLimiter.RateLimit(context.Background(), 1)
			assert.NoError(t, err)

			err = rateLimiter.RateLimit(context.Background(), 1)
			switch behavior {
			case ThrottleBehaviorError:
				assert.EqualError(t, err, "rpc error: code = ResourceExhausted desc = too many requests")
			case ThrottleBehaviorDelay:
				assert.NoError(t, err)
			}
		})
	}
}

func TestGubernatorRateLimiter_RateLimit_MetadataKeys(t *testing.T) {
	rateLimiter := newTestGubernatorRateLimiter(t, &Config{
		Type: GubernatorRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Strategy:         StrategyRateLimitRequests,
			Rate:             1,
			Burst:            2,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: 1 * time.Second,
		},
		MetadataKeys: []string{"metadata_key"},
	}, nil)

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
	// If everything is working as expected, making 3 requests on this rate
	// limiter should work, as it will be using different unique keys.
	err := rateLimiter.RateLimit(clientContext1, 1)
	assert.NoError(t, err)

	err = rateLimiter.RateLimit(clientContext1, 1)
	assert.NoError(t, err)

	err = rateLimiter.RateLimit(clientContext2, 1)
	assert.NoError(t, err)
}

func TestGubernatorRateLimiter_Dynamic_Scenarios(t *testing.T) {
	const (
		EWMAWindowPeriod = 500 * time.Millisecond
		StaticRate       = 1000
		EventBufferSize  = 100
	)

	config := &Config{
		Type: GubernatorRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Strategy:         StrategyRateLimitRequests,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: time.Second,
			Rate:             StaticRate,
		},
		DynamicRateLimiting: DynamicRateLimiting{
			Enabled:            true,
			EWMAMultiplier:     1.5,
			EWMAWindow:         EWMAWindowPeriod,
			RecentWindowWeight: 0.75,
		},
	}

	t.Run("Scenario 1: Initial Traffic", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)
		waitUntilNextPeriod(EWMAWindowPeriod)

		// 1st window: 400 req/sec -> 200 hits
		require.NoError(t, rateLimiter.RateLimit(context.Background(), 200))
		assertRequestRateLimitEvent(t, findLastRequestRateLimitEvent(drainEvents(eventChannel), t), 200, StaticRate, StaticRate-200, gubernator.Status_UNDER_LIMIT)

		waitUntilNextPeriod(EWMAWindowPeriod)

		// 2nd window: 500 req/sec -> 250 hits
		require.NoError(t, rateLimiter.RateLimit(context.Background(), 250))
		// previous_rate is 400. dynamic_limit should be 1000.
		assertRequestRateLimitEvent(t, findLastRequestRateLimitEvent(drainEvents(eventChannel), t), 250, StaticRate, StaticRate-250, gubernator.Status_UNDER_LIMIT)
	})

	t.Run("Scenario 2: Ramping Up Traffic", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)

		// Seed previous window with 900 req/sec -> 450 hits
		waitUntilNextPeriod(EWMAWindowPeriod)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), 450))
		drainEvents(eventChannel)

		// Current window: 1200 req/sec -> 600 hits
		waitUntilNextPeriod(EWMAWindowPeriod)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), 600))
		// dynamic_limit = max(1000, min(1125, 900) * 1.5) = 1350
		assertRequestRateLimitEvent(t, findLastRequestRateLimitEvent(drainEvents(eventChannel), t), 600, 1350, 1350-600, gubernator.Status_UNDER_LIMIT)
	})

	t.Run("Scenario 3: Sustained High Traffic", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)

		// Seed previous window with 1500 req/sec -> 750 hits
		waitUntilNextPeriod(EWMAWindowPeriod)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), 750))
		drainEvents(eventChannel)

		// Current window: 1600 req/sec -> 800 hits
		waitUntilNextPeriod(EWMAWindowPeriod)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), 800))
		// dynamic_limit = max(1000, min(1575, 1500) * 1.5) = 2250
		assertRequestRateLimitEvent(t, findLastRequestRateLimitEvent(drainEvents(eventChannel), t), 800, 2250, 2250-800, gubernator.Status_UNDER_LIMIT)
	})

	t.Run("Scenario 4: Traffic Spike", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)

		// Seed previous window with 1000 req/sec -> 500 hits
		waitUntilNextPeriod(EWMAWindowPeriod)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), 500))
		drainEvents(eventChannel)

		// Current window: 3000 req/sec -> 1500 hits
		waitUntilNextPeriod(EWMAWindowPeriod)
		// dynamic_limit = max(1000, min(2500, 1000) * 1.5) = 1500
		// A request for 1501 should be throttled.
		require.Error(t, rateLimiter.RateLimit(context.Background(), 1501))
		assertRequestRateLimitEvent(t, findLastRequestRateLimitEvent(drainEvents(eventChannel), t), 1501, 1500, 1500, gubernator.Status_OVER_LIMIT)

		// A smaller request should be allowed, the bucket was not drained.
		require.NoError(t, rateLimiter.RateLimit(context.Background(), 750))
		assertRequestRateLimitEvent(t, findLastRequestRateLimitEvent(drainEvents(eventChannel), t), 750, 1500, 1500-750, gubernator.Status_UNDER_LIMIT)
	})

	t.Run("Scenario 5: Traffic Reduction", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)

		// Seed previous window with 2000 req/sec -> 1000 hits
		waitUntilNextPeriod(EWMAWindowPeriod)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), 1000))
		drainEvents(eventChannel)

		// Current window: 500 req/sec -> 250 hits
		waitUntilNextPeriod(EWMAWindowPeriod)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), 250))
		// dynamic_limit = max(1000, min(875, 2000) * 1.5) = 1312.5 -> 1313
		assertRequestRateLimitEvent(t, findLastRequestRateLimitEvent(drainEvents(eventChannel), t), 250, 1313, 1313-250, gubernator.Status_UNDER_LIMIT)
	})
}

// drainEvents drains events from the channel until it's been empty for a short while.
func drainEvents(c <-chan gubernator.HitEvent) []gubernator.HitEvent {
	events := make([]gubernator.HitEvent, 0)
	for {
		select {
		case e := <-c:
			events = append(events, e)
		case <-time.After(200 * time.Millisecond):
			return events
		}
	}
}

// findLastRequestRateLimitEvent finds the last 'requests_per_sec' event from a slice of events.
func findLastRequestRateLimitEvent(events []gubernator.HitEvent, t *testing.T) gubernator.HitEvent {
	t.Helper()
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Request.Name == "requests_per_sec" {
			return events[i]
		}
	}
	require.Fail(t, "did not find 'requests_per_sec' event")
	return gubernator.HitEvent{}
}

func assertRequestRateLimitEvent(t *testing.T, event gubernator.HitEvent, expectedHits, expectedLimit, expectedRemaining int64, expectedStatus gubernator.Status) {
	t.Helper()
	assert.Equal(t, "requests_per_sec", event.Request.Name, "request.name")
	assert.Equal(t, "default", event.Request.UniqueKey, "request.unique_key")
	assert.Equal(t, expectedHits, event.Request.Hits, "request.hits")
	assert.Equal(t, expectedLimit, event.Request.Limit, "request.limit")
	assert.Equal(t, expectedLimit, event.Request.Burst, "request.burst")
	assert.Equal(t, gubernator.Algorithm_LEAKY_BUCKET, event.Request.Algorithm, "request.algorithm")
	assert.Equal(t, gubernator.Behavior_BATCHING, event.Request.Behavior, "request.behavior")
	assert.NotZero(t, *event.Request.CreatedAt, "request.created_at")

	assert.Equal(t, expectedLimit, event.Response.Limit, "response.limit")
	assert.InDelta(t, expectedRemaining, event.Response.Remaining, 1, "response.remaining")
	assert.Equal(t, expectedStatus, event.Response.Status, "response.status")
	assert.GreaterOrEqual(t, event.Response.ResetTime, *event.Request.CreatedAt, "response.reset_time")
}