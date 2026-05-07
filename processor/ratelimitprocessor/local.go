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

package ratelimitprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor"

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/processor"
	"golang.org/x/time/rate"
)

var _ RateLimiter = (*localRateLimiter)(nil)

// keyState holds the per-rate-limit-key state: the underlying token-bucket
// limiter and a mutex used to make a multi-chunk reservation atomic with
// respect to concurrent callers sharing the same key.
type keyState struct {
	limiter *rate.Limiter
	// reserveLock serializes reservation calculation across concurrent
	// callers for this key, so callers acquire reservations FIFO instead
	// of having their per-chunk ReserveN calls interleave. The lock is
	// held only during the (microsecond-scale) reservation loop and is
	// released before the caller waits, so concurrent callers queue on
	// arrival but wait in parallel.
	reserveLock sync.Mutex
}

type localRateLimiter struct {
	cfg *Config
	set processor.Settings
	// TODO use an LRU to keep a cap on the number of limiters.
	// When the LRU capacity is exceeded, reuse the evicted limiter.
	limiters sync.Map // map[string]*keyState
}

func newLocalRateLimiter(cfg *Config, set processor.Settings) (*localRateLimiter, error) {
	return &localRateLimiter{cfg: cfg, set: set}, nil
}

func (r *localRateLimiter) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (r *localRateLimiter) Shutdown(_ context.Context) error {
	r.limiters = sync.Map{}
	return nil
}

func (r *localRateLimiter) RateLimit(ctx context.Context, hits int) error {
	metadata := client.FromContext(ctx).Metadata
	// Each (shared) processor gets its own rate limiter,
	// so it's enough to use client metadata-based unique key.
	key := getUniqueKey(metadata, r.cfg.MetadataKeys)
	cfg := resolveRateLimit(r.cfg, metadata)

	v, _ := r.limiters.LoadOrStore(key, &keyState{
		limiter: rate.NewLimiter(rate.Limit(cfg.Rate), cfg.Burst),
	})
	state := v.(*keyState)
	limiter := state.limiter

	switch cfg.ThrottleBehavior {
	case ThrottleBehaviorError:
		if ok := limiter.AllowN(time.Now(), hits); !ok {
			return errorWithDetails(errTooManyRequests, cfg)
		}
	case ThrottleBehaviorDelay:
		reservations, delay, err := reserveAll(state, hits)
		if err != nil {
			return errorWithDetails(err, cfg)
		}
		if delay <= 0 {
			return nil
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			cancelReservations(reservations)
			return ctx.Err()
		case <-timer.C:
		}
	}

	return nil
}

// reserveAll books all chunks for one caller atomically under
// state.reserveLock and returns the cumulative wait duration plus the
// reservations (so the caller can cancel them on context cancellation).
//
// The lock is held only during the reservation loop, not while waiting,
// so concurrent callers queue FIFO on arrival but sleep in parallel.
//
// hits is split into chunks of at most limiter.Burst() because
// rate.Limiter rejects ReserveN calls with n > burst (the bucket can
// never hold more than burst tokens). All chunks are booked at the same
// timestamp, so the last reservation carries the largest deficit and
// therefore the cumulative delay for the whole batch.
func reserveAll(state *keyState, hits int) ([]*rate.Reservation, time.Duration, error) {
	// Empty batches (hits <= 0) consume nothing and have no delay.
	if hits <= 0 {
		return nil, 0, nil
	}
	state.reserveLock.Lock()
	defer state.reserveLock.Unlock()

	limiter := state.limiter
	burst := limiter.Burst()
	now := time.Now()
	reservations := make([]*rate.Reservation, 0, (hits+burst-1)/burst)

	remaining := hits
	for remaining > 0 {
		n := min(remaining, burst)
		lr := limiter.ReserveN(now, n)
		if !lr.OK() {
			// Defensive: should be unreachable because n <= burst and
			// ReserveN's only other failure mode is a wait larger than
			// math.MaxInt64 nanoseconds. Cancel anything we already
			// booked so we don't leak tokens.
			cancelReservations(reservations)
			return nil, 0, errTooManyRequests
		}
		reservations = append(reservations, lr)
		remaining -= n
	}
	return reservations, reservations[len(reservations)-1].Delay(), nil
}

// cancelReservations releases tokens reserved by reserveAll. Reservations
// must be cancelled in reverse order (newest first) because rate.Limiter
// only fully restores tokens for the most recently made reservation: each
// CancelAt rolls the limiter's lastEvent backwards by one reservation,
// making the next-newest reservation the new "most recent" and therefore
// fully restorable in turn.
func cancelReservations(reservations []*rate.Reservation) {
	now := time.Now()
	for i := len(reservations) - 1; i >= 0; i-- {
		reservations[i].CancelAt(now)
	}
}
