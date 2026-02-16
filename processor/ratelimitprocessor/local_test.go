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
