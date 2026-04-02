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

package sharedcomponent

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
)

// mockComponent is a minimal component.Component for testing.
type mockComponent struct {
	started  atomic.Bool
	shutdown atomic.Bool
	startErr error
}

func (m *mockComponent) Start(_ context.Context, _ component.Host) error {
	m.started.Store(true)
	return m.startErr
}

func (m *mockComponent) Shutdown(_ context.Context) error {
	m.shutdown.Store(true)
	return nil
}

func TestSharedMap_LoadOrStore_New(t *testing.T) {
	m := NewSharedMap[string, *mockComponent]()
	mock := &mockComponent{}

	sc, err := m.LoadOrStore("key1", func() (*mockComponent, error) {
		return mock, nil
	})
	require.NoError(t, err)
	assert.Same(t, mock, sc.Unwrap())
}

func TestSharedMap_LoadOrStore_Existing(t *testing.T) {
	m := NewSharedMap[string, *mockComponent]()
	mock := &mockComponent{}

	sc1, err := m.LoadOrStore("key1", func() (*mockComponent, error) {
		return mock, nil
	})
	require.NoError(t, err)

	// Second call with same key returns the same component.
	sc2, err := m.LoadOrStore("key1", func() (*mockComponent, error) {
		return &mockComponent{}, nil // this factory should NOT be called
	})
	require.NoError(t, err)
	assert.Same(t, sc1.Unwrap(), sc2.Unwrap(), "should return same component for same key")
}

func TestSharedMap_LoadOrStore_DifferentKeys(t *testing.T) {
	m := NewSharedMap[string, *mockComponent]()

	sc1, err := m.LoadOrStore("key1", func() (*mockComponent, error) {
		return &mockComponent{}, nil
	})
	require.NoError(t, err)

	sc2, err := m.LoadOrStore("key2", func() (*mockComponent, error) {
		return &mockComponent{}, nil
	})
	require.NoError(t, err)

	assert.NotSame(t, sc1.Unwrap(), sc2.Unwrap(), "different keys should create different components")
}

func TestSharedMap_LoadOrStore_CreateError(t *testing.T) {
	m := NewSharedMap[string, *mockComponent]()

	_, err := m.LoadOrStore("key1", func() (*mockComponent, error) {
		return nil, fmt.Errorf("factory error")
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "factory error")
}

func TestSharedComponent_StartOnce(t *testing.T) {
	m := NewSharedMap[string, *mockComponent]()
	mock := &mockComponent{}

	sc1, err := m.LoadOrStore("key", func() (*mockComponent, error) {
		return mock, nil
	})
	require.NoError(t, err)
	sc2, err := m.LoadOrStore("key", func() (*mockComponent, error) {
		return mock, nil
	})
	require.NoError(t, err)

	host := componenttest.NewNopHost()

	// First Start actually starts the component.
	require.NoError(t, sc1.Start(context.Background(), host))
	assert.True(t, mock.started.Load())

	// Second Start is a no-op (sync.Once).
	require.NoError(t, sc2.Start(context.Background(), host))
}

func TestSharedComponent_StartError(t *testing.T) {
	m := NewSharedMap[string, *mockComponent]()
	mock := &mockComponent{startErr: fmt.Errorf("start failed")}

	sc, err := m.LoadOrStore("key", func() (*mockComponent, error) {
		return mock, nil
	})
	require.NoError(t, err)

	err = sc.Start(context.Background(), componenttest.NewNopHost())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "start failed")
}

func TestSharedComponent_ShutdownRefCounting(t *testing.T) {
	m := NewSharedMap[string, *mockComponent]()
	mock := &mockComponent{}

	// Two references.
	sc1, err := m.LoadOrStore("key", func() (*mockComponent, error) {
		return mock, nil
	})
	require.NoError(t, err)
	sc2, err := m.LoadOrStore("key", func() (*mockComponent, error) {
		return mock, nil
	})
	require.NoError(t, err)

	require.NoError(t, sc1.Start(context.Background(), componenttest.NewNopHost()))

	// First shutdown decrements ref count but does NOT shutdown.
	require.NoError(t, sc1.Shutdown(context.Background()))
	assert.False(t, mock.shutdown.Load(), "should not shutdown with remaining refs")

	// Second shutdown is the last ref — actually shuts down.
	require.NoError(t, sc2.Shutdown(context.Background()))
	assert.True(t, mock.shutdown.Load(), "should shutdown on last ref")
}

func TestSharedComponent_ShutdownRemovesFromMap(t *testing.T) {
	m := NewSharedMap[string, *mockComponent]()

	sc, err := m.LoadOrStore("key", func() (*mockComponent, error) {
		return &mockComponent{}, nil
	})
	require.NoError(t, err)
	require.NoError(t, sc.Start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, sc.Shutdown(context.Background()))

	// After shutdown, LoadOrStore should create a new component.
	mock2 := &mockComponent{}
	sc2, err := m.LoadOrStore("key", func() (*mockComponent, error) {
		return mock2, nil
	})
	require.NoError(t, err)
	assert.Same(t, mock2, sc2.Unwrap(), "should create new component after previous was removed")
}

func TestSharedComponent_Unwrap(t *testing.T) {
	m := NewSharedMap[string, *mockComponent]()
	mock := &mockComponent{}

	sc, err := m.LoadOrStore("key", func() (*mockComponent, error) {
		return mock, nil
	})
	require.NoError(t, err)
	assert.Same(t, mock, sc.Unwrap())
}
