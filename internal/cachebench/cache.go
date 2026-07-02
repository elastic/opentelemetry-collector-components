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

// Package cachebench compares three cache backends for the source-scoped
// API-key auth cache in the ingest collector, under the fixed constraint that
// every source ID carries its own unique API key (so the auth keyspace equals
// the number of active source keys and cannot be collapsed).
//
// The three backends map to Options 0, 1, and 2 of
// source-cache-scaling-options.md:
//
//	Option 0, LRUCache    : sharded in-memory freelru (per pod)
//	Option 1, PebbleCache : small in-memory hot tier + on-disk pebble (per pod)
//	Option 2, RedisCache  : small in-memory L1 + shared Redis L2 (fleet)
package cachebench // import "github.com/elastic/opentelemetry-collector-components/internal/cachebench"

import (
	"context"
	"encoding/binary"
	"errors"
	"time"
)

// Entry mirrors the fields the real apikeyauthextension caches per key: the
// PBKDF2-derived key (32 bytes), an authorization decision, and the resolved
// username. Kept deliberately small and fixed so per-entry memory is realistic.
type Entry struct {
	// DerivedKey is the PBKDF2 hash of the API key (32 bytes in the real code).
	DerivedKey [32]byte
	// Authorized reports whether the key passed the privilege check.
	Authorized bool
	// Username is the ES username resolved for the key.
	Username string
}

// approxEntrySize is a rough per-entry heap cost used only for reporting: the
// 32-byte key, a bool, and a short username string header plus backing bytes.
const approxEntrySize = 32 + 1 + 16 + 16

// Cache is the common surface the benchmark exercises. It is intentionally
// narrower than the production authenticator: it isolates the caching decision
// (hit, miss, insert) from the Elasticsearch round trip so the three backends
// can be compared apples to apples.
type Cache interface {
	// Name returns a short identifier used in benchmark and report output.
	Name() string
	// Get returns the cached entry for key, or ok=false on a miss.
	Get(ctx context.Context, key string) (*Entry, bool)
	// Add stores entry for key with the cache's configured TTL.
	Add(ctx context.Context, key string, entry *Entry)
	// Close releases any resources (disk, connections).
	Close() error
}

// encodeEntry serializes an Entry to bytes for the off-heap backends (pebble,
// redis). Layout: [32]byte key | 1 byte authorized | username bytes.
func encodeEntry(e *Entry) []byte {
	b := make([]byte, 32+1+len(e.Username))
	copy(b[:32], e.DerivedKey[:])
	if e.Authorized {
		b[32] = 1
	}
	copy(b[33:], e.Username)
	return b
}

// decodeEntry is the inverse of encodeEntry.
func decodeEntry(b []byte) (*Entry, error) {
	if len(b) < 33 {
		return nil, errors.New("cachebench: short entry")
	}
	e := &Entry{Authorized: b[32] == 1, Username: string(b[33:])}
	copy(e.DerivedKey[:], b[:32])
	return e, nil
}

// diskEntry wraps an encoded entry with an absolute expiry for backends that do
// not evict on TTL natively (pebble). Layout: 8 byte unix-nano expiry | entry.
func encodeDiskEntry(e *Entry, expiresAt time.Time) []byte {
	payload := encodeEntry(e)
	b := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint64(b[:8], uint64(expiresAt.UnixNano()))
	copy(b[8:], payload)
	return b
}

func decodeDiskEntry(b []byte, now time.Time) (*Entry, bool, error) {
	if len(b) < 8 {
		return nil, false, errors.New("cachebench: short disk entry")
	}
	expiresAt := time.Unix(0, int64(binary.LittleEndian.Uint64(b[:8])))
	if now.After(expiresAt) {
		return nil, true, nil // expired: treat as miss, signal caller may sweep
	}
	e, err := decodeEntry(b[8:])
	return e, false, err
}
