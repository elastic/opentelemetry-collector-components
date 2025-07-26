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
	const (
		windowPeriod = 200 * time.Millisecond
		staticRate   = 10 // Very low rate for easier testing
	)

	rateLimiter := newTestGubernatorRateLimiter(t, &Config{
		Type: GubernatorRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Strategy:         StrategyRateLimitRequests,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: time.Second,
			Rate:             staticRate,
		},
		DynamicRateLimiting: DynamicRateLimiting{
			Enabled:            true,
			EWMAMultiplier:     2.0, // Higher multiplier for clearer effect
			EWMAWindow:         windowPeriod,
			RecentWindowWeight: 0.7,
		},
	}, nil)

	t.Run("static_rate_baseline", func(t *testing.T) {
		waitUntilNextPeriod(windowPeriod)

		// Use up most of the static rate (8 out of 10 req/sec)
		err := rateLimiter.RateLimit(context.Background(), 8)
		assert.NoError(t, err, "Request within static rate should be allowed")

		// This should still fit within burst capacity
		err = rateLimiter.RateLimit(context.Background(), 2)
		assert.NoError(t, err, "Request at burst limit should be allowed")

		// This should exceed the limit
		err = rateLimiter.RateLimit(context.Background(), 1)
		assert.Error(t, err, "Request exceeding rate should be denied")
	})

	t.Run("dynamic_scaling_basic", func(t *testing.T) {
		// Establish baseline traffic in first window
		waitUntilNextPeriod(windowPeriod)
		err := rateLimiter.RateLimit(context.Background(), 6) // 6 req/sec baseline
		require.NoError(t, err)

		// Wait for window rotation
		waitUntilNextPeriod(windowPeriod)

		// Dynamic limit should now be 6 * 2.0 = 12 req/sec
		// We should be able to send 12 requests in a burst
		err = rateLimiter.RateLimit(context.Background(), 12)
		assert.NoError(t, err, "Request within dynamic limit should be allowed")
	})

	t.Run("static_minimum_enforced", func(t *testing.T) {
		// Establish very low baseline
		waitUntilNextPeriod(windowPeriod)
		err := rateLimiter.RateLimit(context.Background(), 1) // 1 req/sec (very low)
		require.NoError(t, err)

		waitUntilNextPeriod(windowPeriod)

		// Even though dynamic would be 1 * 2.0 = 2, static minimum of 10 should apply
		err = rateLimiter.RateLimit(context.Background(), 8)
		assert.NoError(t, err, "Request should be allowed due to static minimum")
	})
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
		EWMAWindowPeriod = 150 * time.Millisecond
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

		// 1st window: 400 req/sec -> 60 hits (400 * 150ms / 1000ms)
		reqsSec := int64(400)
		actual := reqsSec * int64(EWMAWindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		assertRequestRateLimitEvent(t, "default", findLastRequestRateLimitEvent(drainEvents(eventChannel), t), actual, StaticRate, StaticRate-actual, gubernator.Status_UNDER_LIMIT)

		waitUntilNextPeriod(EWMAWindowPeriod)

		// 2nd window: 500 req/sec -> 75 hits (500 * 150ms / 1000ms)
		reqsSec = 500
		actual = reqsSec * int64(EWMAWindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		// previous_rate is 400. dynamic_limit should be 1000.
		assertRequestRateLimitEvent(t, "default", findLastRequestRateLimitEvent(drainEvents(eventChannel), t), actual, StaticRate, StaticRate-actual, gubernator.Status_UNDER_LIMIT)
	})

	t.Run("Scenario 2: Ramping Up Traffic", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)

		// Seed previous window with 900 req/sec -> 135 hits (900 * 150ms / 1000ms)
		waitUntilNextPeriod(EWMAWindowPeriod)
		reqsSec := int64(900)
		actual := reqsSec * int64(EWMAWindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		drainEvents(eventChannel)

		// Current window: 1200 req/sec -> 180 hits (1200 * 150ms / 1000ms)
		waitUntilNextPeriod(EWMAWindowPeriod)
		reqsSec = 1200
		actual = reqsSec * int64(EWMAWindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		// dynamic_limit = max(1000, min(1125, 900) * 1.5) = 1350
		assertRequestRateLimitEvent(t, "default", findLastRequestRateLimitEvent(drainEvents(eventChannel), t), actual, 1350, 1350-actual, gubernator.Status_UNDER_LIMIT)
	})

	t.Run("Scenario 3: Sustained High Traffic", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)

		// Seed previous window with 1500 req/sec -> 225 hits (1500 * 150ms / 1000ms)
		waitUntilNextPeriod(EWMAWindowPeriod)
		reqsSec := int64(1500)
		actual := reqsSec * int64(EWMAWindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		drainEvents(eventChannel)

		// Current window: 1600 req/sec -> 240 hits (1600 * 150ms / 1000ms)
		waitUntilNextPeriod(EWMAWindowPeriod)
		reqsSec = 1600
		actual = reqsSec * int64(EWMAWindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		// dynamic_limit = max(1000, min(1575, 1500) * 1.5) = 2250
		assertRequestRateLimitEvent(t, "default", findLastRequestRateLimitEvent(drainEvents(eventChannel), t), actual, 2250, 2250-actual, gubernator.Status_UNDER_LIMIT)
	})

	t.Run("Scenario 4: Traffic Spike", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)

		// Seed previous window with 1000 req/sec -> 150 hits (1000 * 150ms / 1000ms)
		waitUntilNextPeriod(EWMAWindowPeriod)
		reqsSec := int64(1000)
		actual := reqsSec * int64(EWMAWindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		drainEvents(eventChannel)

		// Current window: spike to 10007 req/sec -> 1501 hits (10007 * 150ms / 1000ms ≈ 1501)
		waitUntilNextPeriod(EWMAWindowPeriod)
		// dynamic_limit = max(1000, min(2500, 1000) * 1.5) = 1500
		// A request for 1501 should be throttled.
		reqsSec = 10007 // This will result in 1501 hits
		actual = reqsSec * int64(EWMAWindowPeriod) / int64(time.Second)
		require.Error(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		assertRequestRateLimitEvent(t, "default", findLastRequestRateLimitEvent(drainEvents(eventChannel), t), actual, 1500, 1500, gubernator.Status_OVER_LIMIT)

		// A smaller request should be allowed, the bucket was not drained.
		reqsSec = 5000 // This will result in 750 hits (5000 * 150ms / 1000ms)
		actual = reqsSec * int64(EWMAWindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		assertRequestRateLimitEvent(t, "default", findLastRequestRateLimitEvent(drainEvents(eventChannel), t), actual, 1500, 1500-actual, gubernator.Status_UNDER_LIMIT)
	})

	t.Run("Scenario 5: Traffic Reduction", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)

		// Seed previous window with 2000 req/sec -> 300 hits (2000 * 150ms / 1000ms)
		waitUntilNextPeriod(EWMAWindowPeriod)
		reqsSec := int64(2000)
		actual := reqsSec * int64(EWMAWindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		drainEvents(eventChannel)

		// Current window: 500 req/sec -> 75 hits (500 * 150ms / 1000ms)
		waitUntilNextPeriod(EWMAWindowPeriod)
		reqsSec = 500
		actual = reqsSec * int64(EWMAWindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		// dynamic_limit = max(1000.000000, min(16042.108753, 2000.000000)*1.500000)
		assertRequestRateLimitEvent(t, "default", findLastRequestRateLimitEvent(drainEvents(eventChannel), t), actual, 3000, 3000-actual, gubernator.Status_UNDER_LIMIT)
	})
}

func TestGubernatorRateLimiter_OverrideDisablesDynamicLimit(t *testing.T) {
	const (
		EWMAWindowPeriod = 150 * time.Millisecond
		StaticRate       = 1000
		EventBufferSize  = 20
	)

	verify := func(evc <-chan gubernator.HitEvent, rate int64, uniqKey string,
		status gubernator.Status, remaining int64, inDelta bool,
	) {
		t.Helper()
		staticEvent := findLastRequestRateLimitEvent(drainEvents(evc), t)
		defer func() {
			if t.Failed() {
				t.Logf("Event: %s", staticEvent.Response.String())
			}
		}()
		assert.Equal(t, uniqKey, staticEvent.Request.UniqueKey)
		assert.Equal(t, int64(rate), staticEvent.Request.Limit, "%s should use override rate", uniqKey)
		assert.Equal(t, status, staticEvent.Response.Status, "%s Status mismatch", uniqKey)
		if inDelta {
			assert.GreaterOrEqual(t, staticEvent.Response.Remaining, remaining,
				"%s Remaining tokens should be at least the expected delta", uniqKey,
			)
		} else {
			assert.Equal(t, remaining, staticEvent.Response.Remaining,
				"%s Remaining tokens should reflect static limit", uniqKey,
			)
		}
	}

	t.Run("override_with_static_only_disables_dynamic", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		// OVERRIDES
		rate := 500 // Static override rate for the test
		throttleInterval := 100 * time.Millisecond
		rateLimiter := newTestGubernatorRateLimiter(t, &Config{
			Type: GubernatorRateLimiter,
			RateLimitSettings: RateLimitSettings{
				Strategy:         StrategyRateLimitRequests,
				ThrottleBehavior: ThrottleBehaviorError,
				ThrottleInterval: time.Second,
				Rate:             StaticRate,
				Burst:            0,
			},
			DynamicRateLimiting: DynamicRateLimiting{
				Enabled:            true,
				EWMAMultiplier:     2.0,
				EWMAWindow:         EWMAWindowPeriod,
				RecentWindowWeight: 0.75,
			},
			MetadataKeys: []string{"x-tenant-id"},
			Overrides: map[string]RateLimitOverrides{
				"x-tenant-id:static-tenant": {
					StaticOnly:       true,
					Rate:             ptr(rate), // Lower than global rate to make test clearer
					ThrottleInterval: ptr(throttleInterval),
				},
			},
		}, eventChannel)

		// Create context for static tenant
		staticTenantCtx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id": {"static-tenant"},
			}),
		})

		// Test the static tenant at various traffic levels
		waitUntilNextPeriod(throttleInterval)

		// Test 1: Should allow traffic within static limit (499 req/sec)
		require.NoError(t, rateLimiter.RateLimit(staticTenantCtx, 499))
		verify(eventChannel, int64(rate), "x-tenant-id:static-tenant",
			gubernator.Status_UNDER_LIMIT, int64(rate-499), false,
		)

		// Now exceed the static limit
		assert.EqualError(t, rateLimiter.RateLimit(staticTenantCtx, 1000),
			"rpc error: code = ResourceExhausted desc = too many requests",
		)
		// record the time when the static limit was depleted
		verify(eventChannel, int64(rate), "x-tenant-id:static-tenant",
			gubernator.Status_OVER_LIMIT, 0, true,
		)

		// Test 2: Verify traffic is processed normally within the static limit
		waitUntilNextPeriod(throttleInterval)

		// Since we've replenished the tokens by now, we should be able to send
		// up to rate - 1 requests (499 req/sec)
		drain := rate - 1
		require.NoError(t, rateLimiter.RateLimit(staticTenantCtx, drain))
		verify(eventChannel, int64(rate), "x-tenant-id:static-tenant",
			gubernator.Status_UNDER_LIMIT, int64(rate-drain), true,
		)
	})

	t.Run("override_without_static_only_uses_override_rate_as_baseline", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)

		rate := 100 // Override rate for the test
		throttleInterval := 100 * time.Millisecond

		rateLimiter := newTestGubernatorRateLimiter(t, &Config{
			Type: GubernatorRateLimiter,
			RateLimitSettings: RateLimitSettings{
				Strategy:         StrategyRateLimitRequests,
				ThrottleBehavior: ThrottleBehaviorError,
				ThrottleInterval: time.Second,
				Rate:             StaticRate,
			},
			DynamicRateLimiting: DynamicRateLimiting{
				Enabled:            true,
				EWMAMultiplier:     2.0,
				EWMAWindow:         EWMAWindowPeriod,
				RecentWindowWeight: 0.75,
			},
			MetadataKeys: []string{"x-tenant-id"},
			Overrides: map[string]RateLimitOverrides{
				"x-tenant-id:dynamic-tenant": {
					// StaticOnly is false (default), so dynamic scaling should work
					Rate:             ptr(rate), // Override rate but still allow dynamic scaling
					ThrottleInterval: ptr(throttleInterval),
				},
			},
		}, eventChannel)

		// Test that the override rate is used as the baseline (not the global rate)
		waitUntilNextPeriod(EWMAWindowPeriod)

		dynamicTenantCtx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id": {"dynamic-tenant"},
			}),
		})

		// Send traffic within the override rate to establish baseline
		require.NoError(t, rateLimiter.RateLimit(dynamicTenantCtx, int(rate)))

		verify(eventChannel, int64(rate), "x-tenant-id:dynamic-tenant",
			gubernator.Status_UNDER_LIMIT, 0, false,
		)

		// Test that the override rate is used as the baseline (not the global rate)
		waitUntilNextPeriod(EWMAWindowPeriod)

		// Send traffic within the override rate to establish baseline

		reqsSec := 300
		proRatedCurrent := float64(rate) / float64(EWMAWindowPeriod.Seconds())
		dynLimit := computeDynamicLimit(float64(reqsSec), proRatedCurrent, float64(rate), DynamicRateLimiting{
			EWMAMultiplier:     2.0,
			EWMAWindow:         EWMAWindowPeriod,
			RecentWindowWeight: 0.75,
		})
		t.Log("requests/sec:", reqsSec, "limit:", dynLimit, "proRatedCurrent:", proRatedCurrent)
		require.NoError(t, rateLimiter.RateLimit(dynamicTenantCtx, int(reqsSec)))

		dynamicLimit := int64(1333)
		verify(eventChannel, dynamicLimit, "x-tenant-id:dynamic-tenant",
			gubernator.Status_UNDER_LIMIT, dynamicLimit-int64(reqsSec), false,
		)
	})
}

// drainEvents drains events from the channel until it's been empty for a short while.
func drainEvents(c <-chan gubernator.HitEvent) []gubernator.HitEvent {
	events := make([]gubernator.HitEvent, 0)
	for {
		select {
		case e := <-c:
			events = append(events, e)
		case <-time.After(50 * time.Millisecond):
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

func assertRequestRateLimitEvent(t *testing.T, uniqueKey string,
	event gubernator.HitEvent,
	expectedHits, expectedLimit, expectedRemaining int64,
	expectedStatus gubernator.Status,
) {
	defer func() {
		if t.Failed() {
			t.Logf("Event: %s", event.Response.String())
		}
	}()
	t.Helper()
	assert.Equal(t, "requests_per_sec", event.Request.Name, "request.name")
	assert.Equal(t, uniqueKey, event.Request.UniqueKey, "request.unique_key")
	assert.Equal(t, expectedHits, event.Request.Hits, "request.hits")
	assert.Equal(t, expectedLimit, event.Request.Limit, "request.limit")
	assert.Equal(t, expectedLimit, event.Request.Burst, "request.burst")
	assert.Equal(t, gubernator.Algorithm_LEAKY_BUCKET, event.Request.Algorithm, "request.algorithm")
	assert.Equal(t, gubernator.Behavior_BATCHING, event.Request.Behavior, "request.behavior")
	assert.NotZero(t, *event.Request.CreatedAt, "request.created_at")

	assert.Equal(t, expectedLimit, event.Response.Limit, "response.limit")
	assert.InDelta(t, expectedRemaining, event.Response.Remaining, 1, "response.remaining", event.Response.Remaining)
	assert.Equal(t, gubernator.Status_name[int32(expectedStatus)], gubernator.Status_name[int32(event.Response.Status)])
	assert.GreaterOrEqual(t, event.Response.ResetTime, *event.Request.CreatedAt, "response.reset_time")
}
