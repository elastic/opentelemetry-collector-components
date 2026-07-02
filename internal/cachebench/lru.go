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
	"hash/fnv"
	"time"

	"github.com/elastic/go-freelru"
)

// LRUCache is Option 0: the current design, a sharded in-memory LRU sized by
// capacity. This is exactly what apikeyauthextension uses today, just made
// configurable so the benchmark can vary capacity against the working set.
type LRUCache struct {
	cache freelru.Cache[string, *Entry]
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// NewLRUCache builds a sharded LRU with the given capacity and TTL.
func NewLRUCache(capacity uint32, ttl time.Duration) (*LRUCache, error) {
	c, err := freelru.NewSharded[string, *Entry](capacity, hashString)
	if err != nil {
		return nil, err
	}
	c.SetLifetime(ttl)
	return &LRUCache{cache: c}, nil
}

func (*LRUCache) Name() string { return "lru-inmem" }

func (l *LRUCache) Get(_ context.Context, key string) (*Entry, bool) {
	return l.cache.Get(key)
}

func (l *LRUCache) Add(_ context.Context, key string, entry *Entry) {
	l.cache.Add(key, entry)
}

func (*LRUCache) Close() error { return nil }
