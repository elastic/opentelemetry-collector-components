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

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/elastic/go-freelru"
)

// PebbleCache is Option 1: a per-pod two-tier cache. A small in-memory hot LRU
// fronts a large on-disk pebble store sized to the full working set. TTL is not
// native to pebble, so each value carries an absolute expiry and expired reads
// are treated as misses (lazy expiry).
type PebbleCache struct {
	hot freelru.Cache[string, *Entry]
	db  *pebble.DB
	ttl time.Duration
}

// PebbleOptions configures the two tiers.
type PebbleOptions struct {
	// HotCapacity is the number of entries kept in the in-memory tier.
	HotCapacity uint32
	// TTL is the logical lifetime of an entry.
	TTL time.Duration
	// Dir is the on-disk location. Empty uses an in-memory VFS, which keeps the
	// LSM code path (encode, compaction, block cache) while avoiding real disk,
	// useful for CI. Set a real dir to measure SSD behavior.
	Dir string
}

// NewPebbleCache opens the on-disk store and builds the hot tier.
func NewPebbleCache(opts PebbleOptions) (*PebbleCache, error) {
	hot, err := freelru.NewSharded[string, *Entry](opts.HotCapacity, hashString)
	if err != nil {
		return nil, err
	}
	hot.SetLifetime(opts.TTL)

	pebbleOpts := &pebble.Options{}
	if opts.Dir == "" {
		pebbleOpts.FS = vfs.NewMem()
	}
	db, err := pebble.Open(opts.Dir, pebbleOpts)
	if err != nil {
		return nil, err
	}
	return &PebbleCache{hot: hot, db: db, ttl: opts.TTL}, nil
}

func (*PebbleCache) Name() string { return "pebble-disk" }

func (p *PebbleCache) Get(_ context.Context, key string) (*Entry, bool) {
	if e, ok := p.hot.Get(key); ok {
		return e, true
	}
	b, closer, err := p.db.Get([]byte(key))
	if err != nil {
		return nil, false // pebble.ErrNotFound or read error
	}
	entry, expired, decErr := decodeDiskEntry(b, time.Now())
	_ = closer.Close()
	if decErr != nil || expired {
		if expired {
			_ = p.db.Delete([]byte(key), pebble.NoSync)
		}
		return nil, false
	}
	p.hot.Add(key, entry) // promote to hot tier
	return entry, true
}

func (p *PebbleCache) Add(_ context.Context, key string, entry *Entry) {
	p.hot.Add(key, entry)
	_ = p.db.Set([]byte(key), encodeDiskEntry(entry, time.Now().Add(p.ttl)), pebble.NoSync)
}

func (p *PebbleCache) Close() error { return p.db.Close() }
