// ELASTICSEARCH CONFIDENTIAL
// __________________
//
//  Copyright Elasticsearch B.V. All rights reserved.
//
// NOTICE:  All information contained herein is, and remains
// the property of Elasticsearch B.V. and its suppliers, if any.
// The intellectual and technical concepts contained herein
// are proprietary to Elasticsearch B.V. and its suppliers and
// may be covered by U.S. and Foreign Patents, patents in
// process, and are protected by trade secret or copyright
// law.  Dissemination of this information or reproduction of
// this material is strictly forbidden unless prior written
// permission is obtained from Elasticsearch B.V.

package ratelimitprocessor

import (
	"context"
	"testing"
	"time"

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
	require.Nil(t, cfg.Gubernator)

	rl, err := newLocalRateLimiter(cfg, processortest.NewNopSettings())
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
	burst := 2
	rateLimiter := newTestLocalRateLimiter(t, &Config{Rate: 1, Burst: burst})
	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	for i := 0; i < burst; i++ {
		err = rateLimiter.RateLimit(context.Background(), 1) // should pass
		assert.NoError(t, err)
	}

	err = rateLimiter.RateLimit(context.Background(), 1) // should fail
	assert.EqualError(t, err, "too many requests")

	// retry every 20ms to ensure that RateLimit will recover from error when bucket refills after 1 second
	assert.Eventually(t, func() bool {
		return rateLimiter.RateLimit(context.Background(), 1) == nil
	}, 2*time.Second, 20*time.Millisecond)
}

func TestLocalRateLimiter_RateLimit_MetadataKeys(t *testing.T) {
	burst := 2
	rateLimiter := newTestLocalRateLimiter(t, &Config{
		Rate:         1,
		Burst:        burst,
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
