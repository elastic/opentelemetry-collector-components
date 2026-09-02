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

package dynamicbatchprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/dynamicbatchprocessor"

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
)

// shard wraps a single exporterhelper-backed queue+batcher for one metadata key combination.
type shard[T any] struct {
	comp     component.Component
	consume  func(context.Context, T) error
	lastUsed atomic.Int64 // UnixNano; stamped at creation and on each consume
	refs     atomic.Int32 // in-progress consume calls; incremented under p.mu
}

func newShard[T any](comp component.Component, consume func(context.Context, T) error) *shard[T] {
	return &shard[T]{comp: comp, consume: consume}
}

func (s *shard[T]) start(ctx context.Context, host component.Host) error {
	return s.comp.Start(ctx, host)
}

func (s *shard[T]) shutdown(ctx context.Context) error {
	return s.comp.Shutdown(ctx)
}

// prepareForConsumption stamps lastUsed and acquires a ref. Must be called under p.mu
// so it cannot race with GC eviction (which also runs under p.mu before calling waitRefs).
func (s *shard[T]) prepareForConsumption() {
	s.lastUsed.Store(time.Now().UnixNano())
	s.refs.Add(1)
}

func (s *shard[T]) completeConsumption() {
	s.refs.Add(-1)
}

// waitRefs spins until all in-progress consume calls complete.
// refs can only decrease after the shard is removed from the map, so this terminates.
func (s *shard[T]) waitRefs() {
	for s.refs.Load() > 0 {
		runtime.Gosched()
	}
}

func (s *shard[T]) isIdle(cutoff int64) bool {
	return s.lastUsed.Load() < cutoff
}
