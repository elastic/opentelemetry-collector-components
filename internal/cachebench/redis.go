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

package cachebench // import "github.com/elastic/opentelemetry-collector-components/internal/cachebench"

import (
	"context"
	"time"

	"github.com/elastic/go-freelru"
	"github.com/redis/go-redis/v9"
)

// RedisCache is Option 2: a small per-pod L1 LRU in front of a shared Redis L2.
// On an L1 miss it reads L2; on an L2 miss the caller does the ES
// round trip and calls Add, which writes both tiers. The shared L2 deduplicates
// the working set across the whole fleet, so ES sees roughly one miss per key
// instead of one per key per pod.
type RedisCache struct {
	l1     freelru.Cache[string, *Entry]
	rdb    *redis.Client
	ttl    time.Duration
	prefix string
}

// RedisOptions configures the L1 tier and the L2 connection.
type RedisOptions struct {
	// Addr is the Redis address, e.g. "localhost:6379".
	Addr string
	// L1Capacity is the number of entries kept in the per-pod memory tier.
	L1Capacity uint32
	// TTL is applied to both tiers; L2 uses native key expiry.
	TTL time.Duration
	// KeyPrefix namespaces keys, e.g. per tenant, to keep isolation auditable.
	KeyPrefix string
	// PoolSize bounds concurrent L2 connections per pod.
	PoolSize int
}

// NewRedisCache builds the L1 tier and connects to L2. It pings L2 to fail fast
// when Redis is unreachable.
func NewRedisCache(ctx context.Context, opts RedisOptions) (*RedisCache, error) {
	l1, err := freelru.NewSharded[string, *Entry](opts.L1Capacity, hashString)
	if err != nil {
		return nil, err
	}
	l1.SetLifetime(opts.TTL)

	rdb := redis.NewClient(&redis.Options{
		Addr:     opts.Addr,
		PoolSize: opts.PoolSize,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	return &RedisCache{l1: l1, rdb: rdb, ttl: opts.TTL, prefix: opts.KeyPrefix}, nil
}

func (*RedisCache) Name() string { return "redis-shared" }

func (r *RedisCache) redisKey(key string) string { return r.prefix + key }

func (r *RedisCache) Get(ctx context.Context, key string) (*Entry, bool) {
	if e, ok := r.l1.Get(key); ok {
		return e, true
	}
	b, err := r.rdb.Get(ctx, r.redisKey(key)).Bytes()
	if err != nil {
		return nil, false // redis.Nil (miss) or connection error
	}
	entry, decErr := decodeEntry(b)
	if decErr != nil {
		return nil, false
	}
	r.l1.Add(key, entry) // fill L1 from shared L2
	return entry, true
}

func (r *RedisCache) Add(ctx context.Context, key string, entry *Entry) {
	r.l1.Add(key, entry)
	_ = r.rdb.Set(ctx, r.redisKey(key), encodeEntry(entry), r.ttl).Err()
}

// Flush clears the shared store. Used between simulation runs so one backend's
// warm L2 does not leak into the next measurement.
func (r *RedisCache) Flush(ctx context.Context) error {
	return r.rdb.FlushDB(ctx).Err()
}

func (r *RedisCache) Close() error { return r.rdb.Close() }
