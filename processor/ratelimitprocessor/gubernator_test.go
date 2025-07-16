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
	"sync"
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
func newTestGubernatorRateLimiter(t *testing.T, cfg *Config) *gubernatorRateLimiter {
	peers := []gubernator.PeerInfo{
		{GRPCAddress: "127.0.0.1:30100", HTTPAddress: "127.0.0.1:30101"},
	}
	err := cluster.StartWith(peers)
	require.NoError(t, err)

	daemons := cluster.GetDaemons()
	require.Equal(t, 1, len(daemons))

	conn, err := grpc.NewClient(
		daemons[0].PeerInfo.GRPCAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	cl := gubernator.NewV1Client(conn)
	require.NotNil(t, cl)

	rl := &gubernatorRateLimiter{
		cfg: cfg,
		set: processor.Settings{
			ID:                component.NewIDWithName(metadata.Type, "abc123"),
			TelemetrySettings: componenttest.NewNopTelemetrySettings(),
			BuildInfo:         component.NewDefaultBuildInfo(),
		},
		behavior: gubernator.Behavior_BATCHING,

		daemon:     daemons[0],
		client:     cl,
		clientConn: conn,

		currentRequests: make(map[string]int),
	}

	require.NoError(t, err)
	t.Cleanup(func() {
		cluster.Stop()
		err := rl.Shutdown(context.Background())
		assert.NoError(t, err)
	})
	return rl
}

func TestGubernatorRateLimiter_RateLimit(t *testing.T) {
	for _, behavior := range []ThrottleBehavior{ThrottleBehaviorError, ThrottleBehaviorDelay} {
		t.Run(string(behavior), func(t *testing.T) {
			rateLimiter := newTestGubernatorRateLimiter(t, &Config{
				RateLimitSettings: RateLimitSettings{
					Rate:             1,
					Burst:            2,
					ThrottleBehavior: behavior,
					ThrottleInterval: 1 * time.Second,
				},
			})

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
		RateLimitSettings: RateLimitSettings{
			Rate:             1,
			Burst:            2,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: 1 * time.Second,
		},
		MetadataKeys: []string{"metadata_key"},
	})

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

func TestGubernatorRateLimiter_MultipleRequests_Delay(t *testing.T) {
	rl := newTestGubernatorRateLimiter(t, &Config{
		RateLimitSettings: RateLimitSettings{
			Rate:             1, // request per second
			Burst:            1, // capacity only for one
			ThrottleBehavior: ThrottleBehaviorDelay,
			ThrottleInterval: 100 * time.Millisecond, // add 1 token after 100ms
		},
		MetadataKeys: []string{"metadata_key"},
	})

	// Simulate 2 requests hitting the rate limit simultaneously
	requests := 3
	var wg sync.WaitGroup
	wg.Add(requests)
	for i := 0; i < requests; i++ {
		go func() {
			defer wg.Done()
			err := rl.RateLimit(context.Background(), 1)
			require.NoError(t, err)
		}()
	}
	wg.Wait()
}
