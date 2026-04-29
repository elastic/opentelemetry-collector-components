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

package cursor // import "github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/cursor"

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/extension/xextension/storage"
)

const cursorKey = "akamai_siem_cursor"

// Cursor holds the chain-based state for resuming event collection.
type Cursor struct {
	ChainFrom        int64     `json:"chain_from,omitempty"`
	ChainTo          int64     `json:"chain_to,omitempty"`
	CaughtUp         bool      `json:"caught_up,omitempty"`
	LastOffset       string    `json:"last_offset,omitempty"`
	OffsetObtainedAt time.Time `json:"offset_obtained_at,omitempty"`
}

// IsOffsetStale returns true if the stored offset has exceeded the given TTL.
func (c *Cursor) IsOffsetStale(ttl time.Duration) bool {
	if ttl == 0 || c.LastOffset == "" {
		return false
	}
	return !c.OffsetObtainedAt.IsZero() && time.Since(c.OffsetObtainedAt) > ttl
}

// ClearOffset resets offset fields for chain replay.
func (c *Cursor) ClearOffset() {
	c.LastOffset = ""
	c.OffsetObtainedAt = time.Time{}
}

// CursorStore persists cursor state using the OTel storage extension interface.
type CursorStore struct {
	client storage.Client
}

// NewCursorStore creates a cursor store backed by the given storage client.
func NewCursorStore(client storage.Client) *CursorStore {
	return &CursorStore{client: client}
}

// Load retrieves the persisted cursor. Returns a zero-value cursor if none exists.
func (s *CursorStore) Load(ctx context.Context) (Cursor, error) {
	data, err := s.client.Get(ctx, cursorKey)
	if err != nil {
		return Cursor{}, fmt.Errorf("failed to read cursor: %w", err)
	}
	if data == nil {
		return Cursor{}, nil
	}
	var c Cursor
	if err := json.Unmarshal(data, &c); err != nil {
		return Cursor{}, fmt.Errorf("failed to unmarshal cursor: %w", err)
	}
	return c, nil
}

// Save persists the cursor.
func (s *CursorStore) Save(ctx context.Context, c Cursor) error {
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal cursor: %w", err)
	}
	return s.client.Set(ctx, cursorKey, data)
}

// Close releases the storage client.
func (s *CursorStore) Close(ctx context.Context) error {
	return s.client.Close(ctx)
}
