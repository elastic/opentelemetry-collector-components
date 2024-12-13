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
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/processor"
	"golang.org/x/time/rate"
)

var _ RateLimiter = (*localRateLimiter)(nil)

type localRateLimiter struct {
	cfg *Config
	set processor.Settings
	// TODO use an LRU to keep a cap on the number of limiters.
	// When the LRU capacity is exceeded, reuse the evicted limiter.
	limiters sync.Map
}

func newLocalRateLimiter(cfg *Config, set processor.Settings) (*localRateLimiter, error) {
	return &localRateLimiter{cfg: cfg, set: set}, nil
}

func (r *localRateLimiter) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (r *localRateLimiter) Shutdown(ctx context.Context) error {
	r.limiters = sync.Map{}
	return nil
}

func (r *localRateLimiter) RateLimit(ctx context.Context, hits int) error {
	// Each (shared) processor gets its own rate limiter,
	// so it's enough to use client metadata-based unique key.
	key := getUniqueKey(ctx, r.cfg.MetadataKeys)
	v, _ := r.limiters.LoadOrStore(key, rate.NewLimiter(rate.Limit(r.cfg.Rate), r.cfg.Burst))
	limiter := v.(*rate.Limiter)
	if ok := limiter.AllowN(time.Now(), hits); !ok {
		// TODO add configurable behaviour for returning an error vs. delaying processing.
		return errTooManyRequests
	}
	return nil
}
