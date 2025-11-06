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
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gubernator-io/gubernator/v2"
	"github.com/gubernator-io/gubernator/v2/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadatatest"
)

func newTestGubernatorRateLimiterMetrics(t *testing.T, cfg *Config) (
	*gubernatorRateLimiter, *componenttest.Telemetry,
) {
	rl := newTestGubernatorRateLimiter(t, cfg, nil)
	tt := componenttest.NewTelemetry()
	telSettings := tt.NewTelemetrySettings()
	tb, err := metadata.NewTelemetryBuilder(telSettings)
	require.NoError(t, err)
	rl.telemetryBuilder = tb
	rl.tracerProvider = telSettings.TracerProvider
	// NOTE(carsonip): It does not test whether rate limiter is instrumenting grpc client correctly in Start
	// because we overwrite client and clientConn directly here, instead of calling Start.
	// To test Start properly it will require refactoring the tests.
	conn, err := grpc.NewClient(
		fmt.Sprintf("static:///%s", rl.daemon.PeerInfo.GRPCAddress),
		grpc.WithResolvers(gubernator.NewStaticBuilder()),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(telSettings.TracerProvider))),
	)
	require.NoError(t, err)
	client := gubernator.NewV1Client(conn)
	rl.clientConn = conn
	rl.client = client
	t.Cleanup(func() {
		_ = conn.Close()
		_ = tt.Shutdown(t.Context())
	})
	return rl, tt
}

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
		rl.telemetryBuilder.Shutdown()
		require.NoError(t, err)
		cluster.Stop()
	})
	return rl
}

func newGubernatorRateLimiterFrom(t *testing.T, cfg *Config, daemon *gubernator.Daemon) *gubernatorRateLimiter {
	telSettings := componenttest.NewNopTelemetrySettings()
	telSettings.Logger = zaptest.NewLogger(t)
	tb, err := metadata.NewTelemetryBuilder(telSettings)
	require.NoError(t, err)

	// Copied from daemon.Client()
	conn, err := grpc.NewClient(
		fmt.Sprintf("static:///%s", daemon.PeerInfo.GRPCAddress),
		grpc.WithResolvers(gubernator.NewStaticBuilder()),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	client := gubernator.NewV1Client(conn)
	t.Cleanup(func() { // Close the gRPC connection and daemon on test cleanup.
		conn.Close()
		daemon.Close()
	})
	return &gubernatorRateLimiter{
		cfg:              cfg,
		logger:           telSettings.Logger,
		telemetryBuilder: tb,
		behavior:         gubernator.Behavior_BATCHING,

		daemonCfg:          daemon.Config(),
		daemon:             daemon,
		clientConn:         conn,
		client:             client,
		classResolver:      noopResolver{},
		windowConfigurator: defaultWindowConfigurator{multiplier: cfg.DefaultWindowMultiplier},
	}
}

func startGubernatorCluster(t *testing.T, c chan<- gubernator.HitEvent) *gubernator.Daemon {
	var err error
	peers := []gubernator.PeerInfo{{GRPCAddress: "127.0.0.1:9990", HTTPAddress: "127.0.0.1:9980"}}
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
			ThrottleInterval: windowPeriod,
			Rate:             staticRate,
		},
		DynamicRateLimiting: DynamicRateLimiting{
			Enabled:                 true,
			DefaultWindowMultiplier: 2.0, // Higher multiplier for clearer effect
			WindowDuration:          windowPeriod,
		},
	}, nil)

	t.Run("static_rate_baseline", func(t *testing.T) {
		waitUntilNextPeriod(windowPeriod)

		// Use up most of the static rate (8 out of 10 req/sec)
		err := rateLimiter.RateLimit(context.Background(), normalizeHits(8, windowPeriod))
		assert.NoError(t, err, "Request within static rate should be allowed")

		// This should still fit within burst capacity
		err = rateLimiter.RateLimit(context.Background(), normalizeHits(2, windowPeriod))
		assert.NoError(t, err, "Request at burst limit should be allowed")

		// This should exceed the limit
		err = rateLimiter.RateLimit(context.Background(), normalizeHits(8, windowPeriod))
		assert.Error(t, err, "Request exceeding rate should be denied")
		waitUntilNextPeriod(windowPeriod)
	})

	t.Run("dynamic_scaling_basic", func(t *testing.T) {
		// Establish baseline traffic in first window
		waitUntilNextPeriod(windowPeriod)
		time.Sleep(150 * time.Millisecond)                    // Ensure we are in the next period
		err := rateLimiter.RateLimit(context.Background(), 8) // 6 req/sec baseline
		require.NoError(t, err)

		// Wait for window rotation
		waitUntilNextPeriod(windowPeriod)

		time.Sleep(100 * time.Millisecond) // Ensure we are in the next period

		// Dynamic limit should now be 6 * 2.0 = 12 req/sec
		// We should be able to send 12 requests in a burst
		err = rateLimiter.RateLimit(context.Background(), 75)
		assert.NoError(t, err, "Request within dynamic limit should be allowed")
	})

	t.Run("static_minimum_enforced", func(t *testing.T) {
		// Establish very low baseline
		waitUntilNextPeriod(windowPeriod)
		err := rateLimiter.RateLimit(context.Background(), normalizeHits(1, windowPeriod)) // 1 req/sec (very low)
		require.NoError(t, err)

		waitUntilNextPeriod(windowPeriod)

		// Even though dynamic would be 1 * 2.0 = 2, static minimum of 10 should apply
		err = rateLimiter.RateLimit(context.Background(), normalizeHits(1, windowPeriod)) // 1 req/sec
		assert.NoError(t, err, "Request should be allowed due to static minimum")
	})
}

// Ideally, we'd use a mock clock to control time precisely, but since `synctest`
// is available only in Go 1.25+, we use a best-effort approach with sleeps until
// the go.mod is updated. https://go.dev/blog/testing-time.
func waitUntilNextPeriod(interval time.Duration) {
	// To ensure we are in the next interval, we wait for the current interval to
	// pass, and then we wait for the next interval to start.
	time.Sleep(time.Until(time.Now().Truncate(interval).Add(interval)))
	time.Sleep(10 * time.Millisecond)
}

func normalizeHits(hits int, windowPeriod time.Duration) int {
	return int(float64(hits) / windowPeriod.Seconds())
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
		WindowPeriod    = 150 * time.Millisecond
		StaticRate      = 1000
		EventBufferSize = 100
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
			Enabled:                 true,
			DefaultWindowMultiplier: 1.5,
			WindowDuration:          WindowPeriod,
		},
	}

	t.Run("Scenario 1: Initial Traffic", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)
		waitUntilNextPeriod(WindowPeriod)

		// 1st window: 400 req/sec -> 60 hits (400 * 150ms / 1000ms)
		reqsSec := int64(400)
		actual := reqsSec * int64(WindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		assertRequestRateLimitEvent(t, "default", lastReqLimitEvent(drainEvents(eventChannel), t),
			actual, StaticRate, StaticRate-actual, gubernator.Status_UNDER_LIMIT,
		)

		waitUntilNextPeriod(WindowPeriod)

		// 2nd window: 500 req/sec -> 75 hits (500 * 150ms / 1000ms)
		reqsSec = 500
		actual = reqsSec * int64(WindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		// previous_rate is 400. dynamic_limit = max(1000, 400 * 1.5) = max(1000, 600) = 1000
		assertRequestRateLimitEvent(t, "default", lastReqLimitEvent(drainEvents(eventChannel), t),
			actual, StaticRate, StaticRate-actual, gubernator.Status_UNDER_LIMIT,
		)
	})

	t.Run("Scenario 2: Ramping Up Traffic", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)

		// Seed previous window with 900 req/sec -> 135 hits (900 * 150ms / 1000ms)
		waitUntilNextPeriod(WindowPeriod)
		reqsSec := int64(900)
		actual := reqsSec * int64(WindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		drainEvents(eventChannel)

		// Current window: 1200 req/sec -> 180 hits (1200 * 150ms / 1000ms)
		waitUntilNextPeriod(WindowPeriod)
		reqsSec = 1200
		actual = reqsSec * int64(WindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		dynamicLimit := int64(math.Max(1000, 900*1.5))
		assertRequestRateLimitEvent(t, "default", lastReqLimitEvent(drainEvents(eventChannel), t),
			actual, dynamicLimit, dynamicLimit-actual, gubernator.Status_UNDER_LIMIT,
		)
	})

	t.Run("Scenario 3: Sustained High Traffic", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)

		// Seed previous window with 1500 req/sec -> 225 hits (1500 * 150ms / 1000ms)
		waitUntilNextPeriod(WindowPeriod)
		reqsSec := int64(1500)
		actual := reqsSec * int64(WindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		drainEvents(eventChannel)

		// Current window: 1600 req/sec -> 240 hits (1600 * 150ms / 1000ms)
		waitUntilNextPeriod(WindowPeriod)
		reqsSec = 1600
		actual = reqsSec * int64(WindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		dynamicLimit := int64(math.Max(1000, 1000*1.5)) // max allowed was 1000 previously
		assertRequestRateLimitEvent(t, "default", lastReqLimitEvent(drainEvents(eventChannel), t),
			actual, dynamicLimit, dynamicLimit-actual, gubernator.Status_UNDER_LIMIT,
		)
	})

	t.Run("Scenario 4: Traffic Spike", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)

		// Seed previous window with 1000 req/sec -> 150 hits (1000 * 150ms / 1000ms)
		waitUntilNextPeriod(WindowPeriod)
		reqsSec := int64(1000)
		actual := reqsSec * int64(WindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		drainEvents(eventChannel)

		// Current window: spike to 10007 req/sec -> 1501 hits (10007 * 150ms / 1000ms ≈ 1501)
		waitUntilNextPeriod(WindowPeriod)
		dynamicLimit := int64(math.Max(1000, 1000*1.5)) // max allowed was 1000 previously
		// A request for 1501 should be throttled.
		reqsSec = 10007 // This will result in 1501 hits
		actual = reqsSec * int64(WindowPeriod) / int64(time.Second)
		require.Error(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		assertRequestRateLimitEvent(t, "default", lastReqLimitEvent(drainEvents(eventChannel), t),
			actual, dynamicLimit, dynamicLimit, gubernator.Status_OVER_LIMIT,
		)

		// A smaller request should be allowed, the bucket was not drained.
		reqsSec = 5000 // This will result in 750 hits (5000 * 150ms / 1000ms)
		actual = reqsSec * int64(WindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		assertRequestRateLimitEvent(t, "default", lastReqLimitEvent(drainEvents(eventChannel), t),
			actual, dynamicLimit, dynamicLimit-actual, gubernator.Status_UNDER_LIMIT,
		)
	})

	t.Run("Scenario 5: Traffic Reduction", func(t *testing.T) {
		eventChannel := make(chan gubernator.HitEvent, EventBufferSize)
		rateLimiter := newTestGubernatorRateLimiter(t, config, eventChannel)

		// Seed previous window with 2000 req/sec -> 300 hits (2000 * 150ms / 1000ms)
		waitUntilNextPeriod(WindowPeriod)
		reqsSec := int64(2000)
		actual := reqsSec * int64(WindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		drainEvents(eventChannel)

		// Current window: 500 req/sec -> 75 hits (500 * 150ms / 1000ms)
		waitUntilNextPeriod(WindowPeriod)
		reqsSec = 500
		actual = reqsSec * int64(WindowPeriod) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(context.Background(), int(actual)))
		dynamicLimit := int64(max(1000, 1000*1.5)) // max allowed was 1000 previously
		assertRequestRateLimitEvent(t, "default", lastReqLimitEvent(drainEvents(eventChannel), t),
			actual, dynamicLimit, dynamicLimit-actual, gubernator.Status_UNDER_LIMIT,
		)
	})
}

func TestGubernatorRateLimiter_OverrideDisablesDynamicLimit(t *testing.T) {
	const (
		WindowPeriod    = 150 * time.Millisecond
		StaticRate      = 1000
		EventBufferSize = 20
	)

	verify := func(evc <-chan gubernator.HitEvent, rate int64, uniqKey string,
		status gubernator.Status, remaining int64, inDelta bool,
	) {
		t.Helper()
		staticEvent := lastReqLimitEvent(drainEvents(evc), t)
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

	t.Run("override_with_disable_dynamic_disables_dynamic", func(t *testing.T) {
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
				Enabled:                 true,
				DefaultWindowMultiplier: 2.0,
				WindowDuration:          WindowPeriod,
			},
			MetadataKeys: []string{"x-tenant-id"},
			Overrides: []RateLimitOverrides{
				{
					Matches: map[string][]string{
						"x-tenant-id": {"static-tenant"},
					},
					DisableDynamic:   true,
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

	t.Run("override_without_disable_dynamic_uses_override_rate_as_baseline", func(t *testing.T) {
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
				Enabled:                 true,
				DefaultWindowMultiplier: 2.0,
				WindowDuration:          WindowPeriod,
			},
			MetadataKeys: []string{"x-tenant-id"},
			Overrides: []RateLimitOverrides{
				{
					Matches: map[string][]string{
						"x-tenant-id": {"dynamic-tenant"},
					},
					// DisableDynamic is false (default), so dynamic scaling should work
					Rate:             ptr(rate), // Override rate but still allow dynamic scaling
					ThrottleInterval: ptr(throttleInterval),
				},
			},
		}, eventChannel)

		// Test that the override rate is used as the baseline (not the global rate)
		waitUntilNextPeriod(WindowPeriod)

		dynamicTenantCtx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id": {"dynamic-tenant"},
			}),
		})

		// Send traffic within the override rate to establish baseline
		require.NoError(t, rateLimiter.RateLimit(dynamicTenantCtx, int(rate)))

		verify(eventChannel, int64(rate), "x-tenant-id:dynamic-tenant",
			gubernator.Status_UNDER_LIMIT, 0, true, // Use delta for remaining since bucket behavior varies
		)

		// Test that the override rate is used as the baseline (not the global rate)
		waitUntilNextPeriod(WindowPeriod)

		// Send smaller traffic within the override rate
		reqsSec := 50 // Send well under the 100 limit to avoid bucket complications
		proRatedCurrent := float64(rate) / float64(WindowPeriod.Seconds())
		t.Log("requests/sec:", reqsSec, "proRatedCurrent:", proRatedCurrent)
		require.NoError(t, rateLimiter.RateLimit(dynamicTenantCtx, int(reqsSec)))

		dynamicRate := max(100, 100*2)
		verify(eventChannel, int64(dynamicRate), "x-tenant-id:dynamic-tenant",
			gubernator.Status_UNDER_LIMIT, int64(dynamicRate)-int64(reqsSec), false,
		)
	})
}

func TestGubernatorRateLimiter_ClassResolver(t *testing.T) {
	eventChannel := make(chan gubernator.HitEvent, 20)
	rateLimiter := newTestGubernatorRateLimiter(t, &Config{
		Type: GubernatorRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Strategy:         StrategyRateLimitRequests,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: time.Second,
			Rate:             500,
		},
		DynamicRateLimiting: DynamicRateLimiting{
			Enabled:                 true,
			DefaultWindowMultiplier: 2.0,
			WindowDuration:          150 * time.Millisecond,
		},
		MetadataKeys: []string{"x-tenant-id"},
		Classes: map[string]Class{
			"alpha": {
				Rate:  2000,
				Burst: 0,
			},
			"bravo": {
				Rate:  1000,
				Burst: 0,
			},
			"charlie": {
				Rate:  500,
				Burst: 0,
			},
		},
		DefaultClass: "charlie",
	}, eventChannel)

	// Set the classResolver manually
	rateLimiter.classResolver = &fakeResolver{
		mapping: map[string]string{
			"x-tenant-id:12345": "alpha",
			"x-tenant-id:67890": "bravo",
		},
	}
	t.Run("Known class 'alpha'", func(t *testing.T) {
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id": {"12345"},
			}),
		})

		actual := int64(500)
		require.NoError(t, rateLimiter.RateLimit(ctx, int(actual)))
		verify := func(evc <-chan gubernator.HitEvent) {
			t.Helper()
			event := lastReqLimitEvent(drainEvents(evc), t)
			assertRequestRateLimitEvent(t, "x-tenant-id:12345", event,
				actual, 2000, 2000-actual, gubernator.Status_UNDER_LIMIT,
			)
		}
		verify(eventChannel)
	})
	t.Run("Known class 'bravo'", func(t *testing.T) {
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id": {"67890"},
			}),
		})

		actual := int64(300)
		require.NoError(t, rateLimiter.RateLimit(ctx, int(actual)))
		verify := func(evc <-chan gubernator.HitEvent) {
			t.Helper()
			event := lastReqLimitEvent(drainEvents(evc), t)
			assertRequestRateLimitEvent(t, "x-tenant-id:67890", event,
				actual, 1000, 1000-actual, gubernator.Status_UNDER_LIMIT,
			)
		}
		verify(eventChannel)
	})
	t.Run("Unknown class -> default_class 'charlie'", func(t *testing.T) {
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id": {"unknown"},
			}),
		})

		actual := int64(400)
		require.NoError(t, rateLimiter.RateLimit(ctx, int(actual)))
		verify := func(evc <-chan gubernator.HitEvent) {
			t.Helper()
			event := lastReqLimitEvent(drainEvents(evc), t)
			assertRequestRateLimitEvent(t, "x-tenant-id:unknown", event,
				actual, 500, 500-actual, gubernator.Status_UNDER_LIMIT,
			)
		}
		verify(eventChannel)
	})
}

func TestGubernatorRateLimiter_LoadClassResolverExtension(t *testing.T) {
	// This test will validate that a class resolver extension can be loaded
	// and used by the rate limiter.
	// Since we cannot depend on an external extension in unit tests, we will
	// just validate that the extension is loaded without error.
	const extName = "fake_resolver"
	cfg := &Config{
		Type: GubernatorRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Strategy:         StrategyRateLimitRequests,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: time.Second,
			Rate:             1000,
		},
		DynamicRateLimiting: DynamicRateLimiting{
			Enabled:                 true,
			DefaultWindowMultiplier: 2.0,
			WindowDuration:          150 * time.Millisecond,
		},
		ClassResolver: component.MustNewID(extName),
	}

	rateLimiter := newTestGubernatorRateLimiter(t, cfg, nil)
	err := rateLimiter.Start(t.Context(), &fakeHost{
		component.MustNewID(extName): &fakeResolver{},
	})
	require.NoError(t, err)
	require.NoError(t, rateLimiter.Shutdown(t.Context()))

	// Set the resolver and validate it's used
	r, ok := rateLimiter.classResolver.(*fakeResolver)
	require.True(t, ok, "expected fakeResolver type")
	require.True(t, r.calledStart, "expected Start to be called")
	require.True(t, r.calledShutdown, "expected Shutdown to be called")
}

func TestGubernatorRateLimiter_WindowConfigurator(t *testing.T) {
	eventChannel := make(chan gubernator.HitEvent, 20)
	windowDuration := 100 * time.Millisecond
	staticRatePerSec := 500
	rateLimiter := newTestGubernatorRateLimiter(t, &Config{
		Type: GubernatorRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Strategy:         StrategyRateLimitRequests,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: time.Second,
			Rate:             staticRatePerSec,
		},
		DynamicRateLimiting: DynamicRateLimiting{
			Enabled:                 true,
			DefaultWindowMultiplier: 2.0,
			WindowDuration:          windowDuration,
		},
		MetadataKeys: []string{"x-tenant-id"},
	}, eventChannel)

	// Set the windowConfigurator manually
	rateLimiter.windowConfigurator = &fakeWindowConfigurator{
		mapping: map[string][]float64{
			"x-tenant-id:12345": {1.5},                 // more ingest data
			"x-tenant-id:67890": {-1, 2, 2, 0.5, 0.01}, // throttle ingest data
		},
		count: make(map[string]int),
	}
	t.Run("scale up ingestion", func(t *testing.T) {
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id": {"12345"},
			}),
		})

		waitUntilNextPeriod(windowDuration)
		// 1st non-zero window: establish baseline with static limit. Since the previous
		// hits at this point were `0` the limit will always be equal to static limit.
		actual := int64(staticRatePerSec) * int64(windowDuration) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(ctx, int(actual)))
		drainEvents(eventChannel)

		waitUntilNextPeriod(windowDuration)
		// 2nd window: ramp up by 1.5 factor as configured for the tenant
		actual = int64(100) * int64(windowDuration) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(ctx, int(actual)))
		expectedLimit := int64(float64(staticRatePerSec) * 1.5) // 150% of previous hits/s
		verify := func(evc <-chan gubernator.HitEvent) {
			t.Helper()
			event := lastReqLimitEvent(drainEvents(evc), t)
			assertRequestRateLimitEvent(t, "x-tenant-id:12345", event,
				actual, expectedLimit, expectedLimit-actual, gubernator.Status_UNDER_LIMIT,
			)
		}
		verify(eventChannel)
	})

	t.Run("scale down ingestion", func(t *testing.T) {
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id": {"67890"},
			}),
		})
		uniqueKey := "x-tenant-id:67890"

		waitUntilNextPeriod(windowDuration)
		// 1st non-zero window: establish baseline with static limit. Since the previous
		// hits at this point were `0` the limit will always be equal to static limit.
		actual := int64(staticRatePerSec) * int64(windowDuration) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(ctx, int(actual)))
		drainEvents(eventChannel)

		waitUntilNextPeriod(windowDuration)
		// 2nd window: scale up by 2 factor as configured for the tenant
		expectedLimit := int64(float64(staticRatePerSec) * 2) // 200% of previous hits/s
		// maximize the hits so that the rate is fully utilized
		actual = expectedLimit * int64(windowDuration) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(ctx, int(actual)))
		assertRequestRateLimitEvent(
			t, uniqueKey,
			lastReqLimitEvent(drainEvents(eventChannel), t),
			actual, expectedLimit, expectedLimit-actual, gubernator.Status_UNDER_LIMIT,
		)

		waitUntilNextPeriod(windowDuration)
		// 3nd window: scale up by 2 factor as configured for the tenant
		expectedLimit = expectedLimit * 2 // 200% of previous hits/s
		// maximize the hits so that the rate is fully utilized
		actual = expectedLimit * int64(windowDuration) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(ctx, int(actual)))
		assertRequestRateLimitEvent(
			t, uniqueKey,
			lastReqLimitEvent(drainEvents(eventChannel), t),
			actual, expectedLimit, expectedLimit-actual, gubernator.Status_UNDER_LIMIT,
		)

		waitUntilNextPeriod(windowDuration)
		// 4th window: scale down by 50% as configured for the tenant
		expectedLimit = int64(float64(expectedLimit) * 0.5) // 50% of previous hits/s
		// maximize the hits so that the rate is fully utilized
		actual = expectedLimit * int64(windowDuration) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(ctx, int(actual)))
		assertRequestRateLimitEvent(
			t, uniqueKey,
			lastReqLimitEvent(drainEvents(eventChannel), t),
			actual, expectedLimit, expectedLimit-actual, gubernator.Status_UNDER_LIMIT,
		)

		waitUntilNextPeriod(windowDuration)
		// 5th window: scale down by 1% as configured for the tenant and assert
		// that the rate goes below static limit
		expectedLimit = int64(float64(expectedLimit) * 0.01) // 1% of previous hits/s
		// maximize the hits so that the rate is fully utilized
		actual = expectedLimit * int64(windowDuration) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(ctx, int(actual)))
		assertRequestRateLimitEvent(
			t, uniqueKey,
			lastReqLimitEvent(drainEvents(eventChannel), t),
			actual, expectedLimit, expectedLimit-actual, gubernator.Status_UNDER_LIMIT,
		)

		// wait for a few periods to assert that the rate goes to be calculated
		// based on static rate and the multiplier as data is NOT sent in the previous
		// waiting window causing the previous rate to go to zero. As the window
		// multiplier is still suggesting lowering the rate, we apply the multiplier
		// to the static limit.
		waitUntilNextPeriod(windowDuration)
		waitUntilNextPeriod(windowDuration)
		expectedLimit = int64(float64(staticRatePerSec) * 0.01)
		actual = int64(5) * int64(windowDuration) / int64(time.Second)
		require.NoError(t, rateLimiter.RateLimit(ctx, int(actual)))
		assertRequestRateLimitEvent(
			t, uniqueKey,
			lastReqLimitEvent(drainEvents(eventChannel), t),
			actual, expectedLimit, expectedLimit-actual, gubernator.Status_UNDER_LIMIT,
		)
	})
	t.Run("unknown tenant, use default multiplier", func(t *testing.T) {
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id": {"unknown"},
			}),
		})

		actual := int64(400)
		expectedLimit := int64(staticRatePerSec)
		require.NoError(t, rateLimiter.RateLimit(ctx, int(actual)))
		verify := func(evc <-chan gubernator.HitEvent) {
			t.Helper()
			event := lastReqLimitEvent(drainEvents(evc), t)
			assertRequestRateLimitEvent(t, "x-tenant-id:unknown", event,
				actual, expectedLimit, expectedLimit-actual, gubernator.Status_UNDER_LIMIT,
			)
		}
		verify(eventChannel)
	})
}

func TestGubernatorRateLimiter_TelemetryCounters(t *testing.T) {
	const (
		WindowPeriod = 150 * time.Millisecond
		StaticRate   = 1000
	)
	baseCfg := &Config{
		Type: GubernatorRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Strategy:         StrategyRateLimitRequests,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: time.Second,
			Rate:             StaticRate,
			Burst:            0,
		},
		DynamicRateLimiting: DynamicRateLimiting{
			Enabled:                 true,
			DefaultWindowMultiplier: 1.5,
			WindowDuration:          WindowPeriod,
		},
		MetadataKeys: []string{"x-tenant-id"},
		Classes: map[string]Class{
			"alpha": {Rate: StaticRate, Burst: 0},
		},
		DefaultClass: "alpha",
	}

	newRL := func(t *testing.T) (*gubernatorRateLimiter, *componenttest.Telemetry) {
		rl, tt := newTestGubernatorRateLimiterMetrics(t, baseCfg)
		rl.classResolver = &fakeResolver{mapping: map[string]string{
			"x-tenant-id:12345": "alpha",
		}}
		return rl, tt
	}
	// Helper to seed previous window rate directly in Gubernator dynamic buckets
	seedPrevWindow := func(t *testing.T, rl *gubernatorRateLimiter, uniqueKey string, reqsPerSec int) {
		t.Helper()
		// Align with the start of a window and move near its end to record hits in the current window A
		waitUntilNextPeriod(WindowPeriod)
		time.Sleep(WindowPeriod - 10*time.Millisecond)
		now := time.Now()
		drc := newDynamicRateContext(uniqueKey, now, rl.cfg.DynamicRateLimiting)
		// Convert reqs/sec into hits during WindowPeriod
		hits := int64(float64(reqsPerSec) * WindowPeriod.Seconds())
		_, err := rl.client.GetRateLimits(context.Background(), &gubernator.GetRateLimitsReq{
			Requests: []*gubernator.RateLimitReq{
				rl.newDynamicRequest(drc.currentKey, hits, drc),
			},
		})
		require.NoError(t, err)
	}

	// Common context containing the tenant id to produce unique key "x-tenant-id:12345"
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"x-tenant-id": {"12345"},
		}),
	})
	const uniqueKey = "x-tenant-id:12345"

	t.Run("dynamic_escalation_increments", func(t *testing.T) {
		rl, tt := newRL(t)
		// Seed previous window with ~900 req/sec so dynamic becomes 1350 (> static 1000)
		seedPrevWindow(t, rl, uniqueKey, 900)
		// Move to next window and perform one rate-limited call to trigger dynamic calculation
		waitUntilNextPeriod(WindowPeriod)
		assert.NoError(t, rl.RateLimit(ctx, 1))

		// Expect one increment with attributes {class="alpha", source_kind="class"}
		metadatatest.AssertEqualRatelimitDynamicEscalations(t, tt, []metricdata.DataPoint[int64]{
			{
				Value: 1,
				Attributes: attribute.NewSet(
					attribute.String("class", "alpha"),
					attribute.String("source_kind", "class"),
					attribute.String("reason", "success"),
				),
			},
		}, metricdatatest.IgnoreTimestamp())

		spans := tt.SpanRecorder.Ended()
		assert.Greater(t, len(spans), 0)
	})

	t.Run("dynamic_escalation_skipped_increments", func(t *testing.T) {
		rl, tt := newRL(t)
		// Seed previous window with ~200 req/sec so dynamic becomes max(1000, 200*1.5)=1000 (skipped)
		seedPrevWindow(t, rl, uniqueKey, 200)
		waitUntilNextPeriod(WindowPeriod)
		assert.NoError(t, rl.RateLimit(ctx, 1))

		metadatatest.AssertEqualRatelimitDynamicEscalations(t, tt, []metricdata.DataPoint[int64]{
			{
				Value: 1,
				Attributes: attribute.NewSet(
					attribute.String("class", "alpha"),
					attribute.String("source_kind", "class"),
					attribute.String("reason", "skipped"),
				),
			},
		}, metricdatatest.IgnoreTimestamp())

		spans := tt.SpanRecorder.Ended()
		assert.Greater(t, len(spans), 0)
	})

	t.Run("gubernator_degraded_increments", func(t *testing.T) {
		rl, tt := newRL(t)
		rl.daemon.Close() // Close the daemon to force errors in dynamic calls
		assert.Error(t, rl.RateLimit(ctx, 1))

		metadatatest.AssertEqualRatelimitDynamicEscalations(t, tt, []metricdata.DataPoint[int64]{
			{
				Value: 1,
				Attributes: attribute.NewSet(
					attribute.String("class", "alpha"),
					attribute.String("source_kind", "class"),
					attribute.String("reason", "gubernator_error"),
				),
			},
		}, metricdatatest.IgnoreTimestamp())

		spans := tt.SpanRecorder.Ended()
		assert.Greater(t, len(spans), 0)
	})
}

func TestGubernatorRateLimiter_ResolverFailures(t *testing.T) {
	const (
		WindowPeriod = 150 * time.Millisecond
		StaticRate   = 1000
	)
	t.Run("failure_uses_default_class_and_counts", func(t *testing.T) {
		cfg := &Config{
			Type: GubernatorRateLimiter,
			RateLimitSettings: RateLimitSettings{
				Strategy:         StrategyRateLimitRequests,
				ThrottleBehavior: ThrottleBehaviorError,
				ThrottleInterval: time.Second,
				Rate:             StaticRate,
				Burst:            0,
			},
			DynamicRateLimiting: DynamicRateLimiting{
				Enabled:                 true,
				DefaultWindowMultiplier: 1.5,
				WindowDuration:          WindowPeriod,
			},
			MetadataKeys: []string{"x-tenant-id"},
			Classes: map[string]Class{
				"alpha": {Rate: StaticRate, Burst: 0},
			},
			DefaultClass: "alpha",
		}

		rl, tt := newTestGubernatorRateLimiterMetrics(t, cfg)
		// Simulate resolver error for this key
		rl.classResolver = &fakeResolver{errorKeys: map[string]error{
			"x-tenant-id:fail1": errors.New("boom"),
		}}

		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id": {"fail1"},
			}),
		})

		// One call triggers resolver failure and should fall back to default class
		assert.NoError(t, rl.RateLimit(ctx, 1))

		// 1) resolver.failures counter increments
		metadatatest.AssertEqualRatelimitResolverFailures(t, tt, []metricdata.DataPoint[int64]{
			{
				Value: 1,
				Attributes: attribute.NewSet(attribute.String(
					"unique_key", "x-tenant-id:fail1",
				)),
			},
		}, metricdatatest.IgnoreTimestamp())

		// 2) dynamic escalation skipped for baseline (no seeding) with class/default
		metadatatest.AssertEqualRatelimitDynamicEscalations(t, tt, []metricdata.DataPoint[int64]{
			{
				Value: 1,
				Attributes: attribute.NewSet(
					attribute.String("class", "alpha"),
					attribute.String("source_kind", "class"),
					attribute.String("reason", "skipped"),
				),
			},
		}, metricdatatest.IgnoreTimestamp())
	})

	t.Run("failure_falls_back_to_global_when_no_classes", func(t *testing.T) {
		cfg := &Config{
			Type: GubernatorRateLimiter,
			RateLimitSettings: RateLimitSettings{
				Strategy:         StrategyRateLimitRequests,
				ThrottleBehavior: ThrottleBehaviorError,
				ThrottleInterval: time.Second,
				Rate:             StaticRate,
				Burst:            0,
			},
			DynamicRateLimiting: DynamicRateLimiting{
				Enabled:                 true,
				DefaultWindowMultiplier: 1.5,
				WindowDuration:          WindowPeriod,
			},
			MetadataKeys: []string{"x-tenant-id"},
			// No classes and no default class
		}

		rl, tt := newTestGubernatorRateLimiterMetrics(t, cfg)
		rl.classResolver = &fakeResolver{errorKeys: map[string]error{
			"x-tenant-id:fail2": errors.New("boom"),
		}}

		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id": {"fail2"},
			}),
		})

		assert.NoError(t, rl.RateLimit(ctx, 1))

		// resolver.failures increments
		metadatatest.AssertEqualRatelimitResolverFailures(t, tt, []metricdata.DataPoint[int64]{
			{
				Value: 1,
				Attributes: attribute.NewSet(attribute.String(
					"unique_key", "x-tenant-id:fail2",
				)),
			},
		}, metricdatatest.IgnoreTimestamp())

		// dynamic skipped with source_kind=fallback and empty class
		metadatatest.AssertEqualRatelimitDynamicEscalations(t, tt, []metricdata.DataPoint[int64]{
			{
				Value: 1,
				Attributes: attribute.NewSet(
					attribute.String("class", ""),
					attribute.String("source_kind", "fallback"),
					attribute.String("reason", "skipped"),
				),
			},
		}, metricdatatest.IgnoreTimestamp())
	})

	t.Run("multiple_failures_accumulate", func(t *testing.T) {
		cfg := &Config{
			Type: GubernatorRateLimiter,
			RateLimitSettings: RateLimitSettings{
				Strategy:         StrategyRateLimitRequests,
				ThrottleBehavior: ThrottleBehaviorError,
				ThrottleInterval: time.Second,
				Rate:             StaticRate,
				Burst:            0,
			},
			DynamicRateLimiting: DynamicRateLimiting{
				Enabled:                 true,
				DefaultWindowMultiplier: 1.5,
				WindowDuration:          WindowPeriod,
			},
			MetadataKeys: []string{"x-tenant-id"},
			Classes: map[string]Class{
				"alpha": {Rate: StaticRate, Burst: 0},
			},
			DefaultClass: "alpha",
		}
		rl, tt := newTestGubernatorRateLimiterMetrics(t, cfg)
		rl.classResolver = &fakeResolver{errorKeys: map[string]error{
			"x-tenant-id:fail3": errors.New("boom"),
		}}
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id": {"fail3"},
			}),
		})

		for range 3 {
			assert.NoError(t, rl.RateLimit(ctx, 1))
		}
		metadatatest.AssertEqualRatelimitResolverFailures(t, tt, []metricdata.DataPoint[int64]{
			{
				Value: 3,
				Attributes: attribute.NewSet(attribute.String(
					"unique_key", "x-tenant-id:fail3",
				)),
			},
		}, metricdatatest.IgnoreTimestamp())
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

// lastReqLimitEvent finds the last 'requests_per_sec' event from a slice of events.
func lastReqLimitEvent(events []gubernator.HitEvent, t *testing.T) gubernator.HitEvent {
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
	assert.InDelta(t, expectedRemaining, event.Response.Remaining, 2, "response.remaining", event.Response.Remaining)
	assert.Equal(t, gubernator.Status_name[int32(expectedStatus)], gubernator.Status_name[int32(event.Response.Status)])
	assert.GreaterOrEqual(t, event.Response.ResetTime, *event.Request.CreatedAt, "response.reset_time")
}

func TestGubernatorRateLimiter_MultipleRequests_Delay(t *testing.T) {
	throttleInterval := 100 * time.Millisecond
	rl := newTestGubernatorRateLimiter(t, &Config{
		RateLimitSettings: RateLimitSettings{
			Strategy:         StrategyRateLimitRequests,
			Rate:             1, // request per second
			Burst:            1, // capacity only for one
			ThrottleBehavior: ThrottleBehaviorDelay,
			ThrottleInterval: throttleInterval, // add 1 token after 100ms
		},
		MetadataKeys: []string{"metadata_key"},
	}, nil)

	// Simulate 4 requests hitting the rate limit simultaneously.
	// The first request passes, and the next ones hit it simultaneously.
	requests := 5
	endingTimes := make([]time.Time, requests)
	var wg sync.WaitGroup
	wg.Add(requests)

	for i := range requests {
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

type fakeResolver struct {
	mapping    map[string]string
	errorKeys  map[string]error
	callCount  int
	calledKeys []string

	calledStart    bool
	calledShutdown bool
}

func (f *fakeResolver) ResolveClass(_ context.Context, key string) (string, error) {
	f.callCount++
	f.calledKeys = append(f.calledKeys, key)
	if err, hasError := f.errorKeys[key]; hasError {
		return "", err
	}
	if class, exists := f.mapping[key]; exists {
		return class, nil
	}
	return "", nil // Unknown class
}

func (f *fakeResolver) Reset() {
	f.callCount = 0
	f.calledKeys = nil
}

func (f *fakeResolver) Start(context.Context, component.Host) error {
	f.calledStart = true
	return nil
}

func (f *fakeResolver) Shutdown(context.Context) error {
	f.calledShutdown = true
	return nil
}

type fakeWindowConfigurator struct {
	// returns the multiplier based on the count as seen by the configurator
	// For example, if a key is configured with a slice of 2 multipliers then
	// the first call will return the first multiplier, the second will return
	// the next multipler and subsequent call will keep returning the last
	// multiplier in the slice.
	mapping map[string][]float64
	// keep track of number of calls
	count map[string]int
}

func (f *fakeWindowConfigurator) Multiplier(
	ctx context.Context,
	_ time.Duration,
	key string,
) float64 {
	if multipliers, ok := f.mapping[key]; ok {
		if f.count[key] < len(multipliers) {
			f.count[key]++
		}
		return multipliers[f.count[key]-1]
	}
	return -1 // force default multiplier
}

func TestGubernatorRateLimiter_UnavailableError(t *testing.T) {
	retryDelay := 2 * time.Second
	rateLimiter := newTestGubernatorRateLimiter(t, &Config{
		Type: GubernatorRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Strategy:         StrategyRateLimitRequests,
			Rate:             100,
			Burst:            10,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: time.Second,
			RetryDelay:       retryDelay,
		},
	}, nil)

	// Close the daemon to simulate gubernator being unavailable
	rateLimiter.daemon.Close()

	err := rateLimiter.RateLimit(context.Background(), 1)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok, "expected gRPC status error")
	assert.Equal(t, codes.Unavailable, st.Code(), "expected Unavailable status code")

	expectedMsg := fmt.Sprintf("service unavailable, try again in %v seconds", retryDelay.Seconds())
	assert.Contains(t, st.Message(), expectedMsg, "error message should contain retry delay information")

	details := st.Details()
	require.Len(t, details, 2, "expected 2 details (ErrorInfo and RetryInfo)")
}

type fakeHost map[component.ID]component.Component

func (f fakeHost) GetExtensions() map[component.ID]component.Component {
	return f
}
