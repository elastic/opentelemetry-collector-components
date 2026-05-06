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

package cursor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/extension/xextension/storage"
)

func TestCursor_IsOffsetStale(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name   string
		cursor Cursor
		ttl    time.Duration
		want   bool
	}{
		{
			name:   "no offset",
			cursor: Cursor{},
			ttl:    120 * time.Second,
			want:   false,
		},
		{
			name:   "ttl disabled",
			cursor: Cursor{LastOffset: "abc", OffsetObtainedAt: now.Add(-5 * time.Minute)},
			ttl:    0,
			want:   false,
		},
		{
			name:   "fresh offset",
			cursor: Cursor{LastOffset: "abc", OffsetObtainedAt: now.Add(-10 * time.Second)},
			ttl:    120 * time.Second,
			want:   false,
		},
		{
			name:   "stale offset",
			cursor: Cursor{LastOffset: "abc", OffsetObtainedAt: now.Add(-5 * time.Minute)},
			ttl:    120 * time.Second,
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.cursor.IsOffsetStale(tt.ttl))
		})
	}
}

func TestCursor_ClearOffset(t *testing.T) {
	c := Cursor{
		LastOffset:       "abc",
		OffsetObtainedAt: time.Now(),
		ChainFrom:        1000,
	}
	c.ClearOffset()
	assert.Empty(t, c.LastOffset)
	assert.True(t, c.OffsetObtainedAt.IsZero())
	assert.Equal(t, int64(1000), c.ChainFrom) // other fields untouched
}

func TestCursorStore_SaveAndLoad(t *testing.T) {
	store := NewCursorStore(newMemStorageClient())
	ctx := context.Background()

	// Load from empty store returns zero cursor.
	c, err := store.Load(ctx)
	require.NoError(t, err)
	assert.Equal(t, Cursor{}, c)

	// Save and reload.
	saved := Cursor{
		ChainFrom:        1000,
		ChainTo:          2000,
		CaughtUp:         true,
		LastOffset:       "test-offset",
		OffsetObtainedAt: time.Now().Truncate(time.Second), // JSON loses nanoseconds
	}
	require.NoError(t, store.Save(ctx, saved))

	loaded, err := store.Load(ctx)
	require.NoError(t, err)
	assert.Equal(t, saved.ChainFrom, loaded.ChainFrom)
	assert.Equal(t, saved.ChainTo, loaded.ChainTo)
	assert.Equal(t, saved.CaughtUp, loaded.CaughtUp)
	assert.Equal(t, saved.LastOffset, loaded.LastOffset)
}

func TestCursorStore_Overwrite(t *testing.T) {
	store := NewCursorStore(newMemStorageClient())
	ctx := context.Background()

	require.NoError(t, store.Save(ctx, Cursor{ChainFrom: 100}))
	require.NoError(t, store.Save(ctx, Cursor{ChainFrom: 200}))

	loaded, err := store.Load(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(200), loaded.ChainFrom)
}

func TestCursorStore_Close(t *testing.T) {
	store := NewCursorStore(newMemStorageClient())
	assert.NoError(t, store.Close(context.Background()))
}

func TestCursorStore_LoadCorruptData(t *testing.T) {
	client := newMemStorageClient()
	// Write invalid JSON directly to storage.
	require.NoError(t, client.Set(context.Background(), cursorKey, []byte("{invalid")))

	store := NewCursorStore(client)
	_, err := store.Load(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestCursorStore_NopClient(t *testing.T) {
	// NopClient returns nil on Get — should return zero cursor.
	store := NewCursorStore(storage.NewNopClient())
	ctx := context.Background()

	c, err := store.Load(ctx)
	require.NoError(t, err)
	assert.Equal(t, Cursor{}, c)

	// Save doesn't error (nop).
	require.NoError(t, store.Save(ctx, Cursor{ChainFrom: 100}))
}

// memStorageClient is a simple in-memory storage.Client for tests.
type memStorageClient struct {
	data sync.Map
}

func newMemStorageClient() *memStorageClient {
	return &memStorageClient{}
}

func (m *memStorageClient) Get(_ context.Context, key string) ([]byte, error) {
	v, ok := m.data.Load(key)
	if !ok {
		return nil, nil
	}
	return v.([]byte), nil
}

func (m *memStorageClient) Set(_ context.Context, key string, value []byte) error {
	m.data.Store(key, value)
	return nil
}

func (m *memStorageClient) Delete(_ context.Context, key string) error {
	m.data.Delete(key)
	return nil
}

func (m *memStorageClient) Batch(_ context.Context, ops ...*storage.Operation) error {
	for _, op := range ops {
		switch op.Type {
		case storage.Get:
			v, _ := m.data.Load(op.Key)
			if v != nil {
				op.Value = v.([]byte)
			}
		case storage.Set:
			m.data.Store(op.Key, op.Value)
		case storage.Delete:
			m.data.Delete(op.Key)
		}
	}
	return nil
}

func (m *memStorageClient) Close(_ context.Context) error {
	return nil
}
