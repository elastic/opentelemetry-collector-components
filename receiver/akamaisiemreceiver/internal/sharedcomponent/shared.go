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

// Adapted from go.opentelemetry.io/collector/internal/sharedcomponent.
// Provides LoadOrStore semantics for sharing a single component across
// multiple named receiver instances (e.g. akamai_siem/ecs + akamai_siem/otel).

package sharedcomponent // import "github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/sharedcomponent"

import (
	"context"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/component"
)

// NewSharedMap creates a new SharedMap for component sharing.
func NewSharedMap[K comparable, V component.Component]() *SharedMap[K, V] {
	return &SharedMap[K, V]{
		components: map[K]*SharedComponent[V]{},
	}
}

// SharedMap keeps reference of all created instances for a given shared key.
type SharedMap[K comparable, V component.Component] struct {
	lock       sync.Mutex
	components map[K]*SharedComponent[V]
}

// LoadOrStore returns the already created instance if it exists, otherwise
// creates a new instance and adds it to the map.
func (m *SharedMap[K, V]) LoadOrStore(key K, create func() (V, error)) (*SharedComponent[V], error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if c, ok := m.components[key]; ok {
		c.refCount.Add(1)
		return c, nil
	}
	comp, err := create()
	if err != nil {
		return nil, err
	}
	sc := &SharedComponent[V]{
		component: comp,
		removeFunc: func() {
			m.lock.Lock()
			defer m.lock.Unlock()
			delete(m.components, key)
		},
	}
	m.components[key] = sc
	sc.refCount.Add(1)
	return sc, nil
}

// SharedComponent wraps a component with ref-counted Start/Shutdown.
// Start runs only once (first caller). Shutdown runs only when the last
// reference calls it.
type SharedComponent[V component.Component] struct {
	component  V
	refCount   atomic.Int64
	startOnce  sync.Once
	startErr   error
	removeFunc func()
}

// Unwrap returns the underlying component.
func (c *SharedComponent[V]) Unwrap() V {
	return c.component
}

// Start starts the component on the first call only.
func (c *SharedComponent[V]) Start(ctx context.Context, host component.Host) error {
	c.startOnce.Do(func() {
		c.startErr = c.component.Start(ctx, host)
	})
	return c.startErr
}

// Shutdown shuts down the component when the last reference calls it.
func (c *SharedComponent[V]) Shutdown(ctx context.Context) error {
	if c.refCount.Add(-1) == 0 {
		err := c.component.Shutdown(ctx)
		c.removeFunc()
		return err
	}
	return nil
}
