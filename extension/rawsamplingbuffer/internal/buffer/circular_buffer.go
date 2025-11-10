package buffer
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

package buffer

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrNotFound is returned when an entry is not found in the buffer.
	ErrNotFound = errors.New("entry not found")
	// ErrEntryTooLarge is returned when an entry exceeds the maximum size.
	ErrEntryTooLarge = errors.New("entry size exceeds maximum")
)

type entry struct {
	data      []byte
	timestamp time.Time
}

// CircularBuffer is a thread-safe circular buffer with LRU eviction and TTL support.
type CircularBuffer struct {
	mu           sync.RWMutex
	entries      map[string]*entry
	maxSize      int
	maxEntrySize int
	ttl          time.Duration
	accessOrder  []string // Track access order for LRU
}

// NewCircularBuffer creates a new circular buffer.
func NewCircularBuffer(maxSize, maxEntrySize int, ttl time.Duration) *CircularBuffer {
	return &CircularBuffer{
		entries:      make(map[string]*entry),
		maxSize:      maxSize,
		maxEntrySize: maxEntrySize,
		ttl:          ttl,
		accessOrder:  make([]string, 0, maxSize),
	}
}

// Store stores data in the buffer with the given ID.
func (b *CircularBuffer) Store(id string, data []byte) error {
	if len(data) > b.maxEntrySize {
		return ErrEntryTooLarge
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// If entry exists, update it and move to end of access order
	if _, exists := b.entries[id]; exists {
		b.entries[id] = &entry{
			data:      data,
			timestamp: time.Now(),
		}
		b.moveToEnd(id)
		return nil
	}

	// If buffer is full, evict least recently used
	if len(b.entries) >= b.maxSize {
		b.evictLRU()
	}

	// Add new entry
	b.entries[id] = &entry{
		data:      data,
		timestamp: time.Now(),
	}
	b.accessOrder = append(b.accessOrder, id)

	return nil
}

// Retrieve retrieves data from the buffer by ID.
func (b *CircularBuffer) Retrieve(id string) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	entry, exists := b.entries[id]
	if !exists {
		return nil, ErrNotFound
	}

	// Check if entry has expired
	if time.Since(entry.timestamp) > b.ttl {
		return nil, ErrNotFound
	}

	return entry.data, nil
}

// Delete removes an entry from the buffer.
func (b *CircularBuffer) Delete(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.entries[id]; !exists {
		return ErrNotFound
	}

	delete(b.entries, id)
	b.removeFromAccessOrder(id)

	return nil
}

// Size returns the current number of entries in the buffer.
func (b *CircularBuffer) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.entries)
}

// CleanExpired removes all expired entries from the buffer.
func (b *CircularBuffer) CleanExpired() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	removed := 0
	now := time.Now()

	for id, entry := range b.entries {
		if now.Sub(entry.timestamp) > b.ttl {
			delete(b.entries, id)
			b.removeFromAccessOrder(id)
			removed++
		}
	}

	return removed
}

// evictLRU removes the least recently used entry.
// Must be called with lock held.
func (b *CircularBuffer) evictLRU() {
	if len(b.accessOrder) == 0 {
		return
	}

	// Remove oldest entry
	oldestID := b.accessOrder[0]
	delete(b.entries, oldestID)
	b.accessOrder = b.accessOrder[1:]
}

// moveToEnd moves an ID to the end of the access order (most recently used).
// Must be called with lock held.
func (b *CircularBuffer) moveToEnd(id string) {
	b.removeFromAccessOrder(id)
	b.accessOrder = append(b.accessOrder, id)
}

// removeFromAccessOrder removes an ID from the access order list.
// Must be called with lock held.
func (b *CircularBuffer) removeFromAccessOrder(id string) {
	for i, accessID := range b.accessOrder {
		if accessID == id {
			b.accessOrder = append(b.accessOrder[:i], b.accessOrder[i+1:]...)
			return
		}
	}
}
