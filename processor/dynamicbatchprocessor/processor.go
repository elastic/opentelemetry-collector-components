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
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// dynamicProcessor routes incoming signal data to per-metadata-key shards.
// T is the signal type (ptrace.Traces, pmetric.Metrics, plog.Logs).
type dynamicProcessor[T any] struct {
	cfg      *Config
	newShard func(host component.Host) (*shard[T], error)

	mu     sync.Mutex
	host   component.Host
	shards map[string]*shard[T] // nil after Shutdown

	stopGC   chan struct{}
	stopOnce sync.Once
	gcDone   sync.WaitGroup
}

func newDynamicProcessor[T any](cfg *Config, newShard func(host component.Host) (*shard[T], error)) *dynamicProcessor[T] {
	return &dynamicProcessor[T]{
		cfg:      cfg,
		newShard: newShard,
		shards:   make(map[string]*shard[T]),
		stopGC:   make(chan struct{}),
	}
}

func (p *dynamicProcessor[T]) Start(_ context.Context, host component.Host) error {
	p.mu.Lock()
	p.host = host
	p.mu.Unlock()

	p.gcDone.Add(1)
	go p.gcLoop()
	return nil
}

func (p *dynamicProcessor[T]) Shutdown(ctx context.Context) error {
	p.stopOnce.Do(func() {
		close(p.stopGC)
		p.gcDone.Wait()
	})

	p.mu.Lock()
	shards := make([]*shard[T], 0, len(p.shards))
	for _, s := range p.shards {
		shards = append(shards, s)
	}
	p.shards = nil // signals getOrCreate that the processor is shut down
	p.mu.Unlock()

	var errs []error
	for _, s := range shards {
		s.waitRefs()
		if err := s.shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (p *dynamicProcessor[T]) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *dynamicProcessor[T]) consumeData(ctx context.Context, data T) error {
	key := buildMetadataKey(client.FromContext(ctx), p.cfg.MetadataKeys)
	s, err := p.getOrCreate(key)
	if err != nil {
		return err
	}
	defer s.completeConsumption()
	return s.consume(ctx, data)
}

// getOrCreate finds or creates a shard and increments its refs under the map lock.
// Holding the lock while incrementing refs ensures GC eviction (which also runs under
// the lock) cannot race with ref acquisition — no retry loop or draining flag needed.
func (p *dynamicProcessor[T]) getOrCreate(key string) (*shard[T], error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.shards == nil {
		return nil, errors.New("dynamicbatchprocessor: processor is shut down")
	}
	if s, ok := p.shards[key]; ok {
		s.prepareForConsumption()
		return s, nil
	}
	if p.cfg.MetadataCardinalityLimit != 0 && len(p.shards) >= p.cfg.MetadataCardinalityLimit {
		return nil, fmt.Errorf("dynamicbatchprocessor: too many metadata combinations (limit %d)", p.cfg.MetadataCardinalityLimit)
	}
	s, err := p.newShard(p.host)
	if err != nil {
		return nil, err
	}
	if err = s.start(context.Background(), p.host); err != nil {
		return nil, err
	}
	s.prepareForConsumption()
	p.shards[key] = s
	return s, nil
}

// buildMetadataKey produces a stable string key from the given metadata keys.
// Absent keys produce an empty value segment; order follows cfg.MetadataKeys.
// Uses a stack-backed buffer to avoid heap allocations for the common case.
func buildMetadataKey(info client.Info, keys []string) string {
	var backing [256]byte
	buf := backing[:0]
	for i, k := range keys {
		if i > 0 {
			buf = append(buf, '&')
		}
		buf = append(buf, k...)
		buf = append(buf, '=')
		for j, v := range info.Metadata.Get(k) {
			if j > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, v...)
		}
	}
	return string(buf)
}

func (p *dynamicProcessor[T]) gcLoop() {
	defer p.gcDone.Done()
	ticker := time.NewTicker(p.cfg.IdleTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.evictIdle()
		case <-p.stopGC:
			return
		}
	}
}

func (p *dynamicProcessor[T]) evictIdle() {
	cutoff := time.Now().Add(-p.cfg.IdleTimeout).UnixNano()

	p.mu.Lock()
	var evicted []*shard[T]
	for key, s := range p.shards {
		if s.isIdle(cutoff) {
			delete(p.shards, key)
			evicted = append(evicted, s)
		}
	}
	p.mu.Unlock()

	for _, s := range evicted {
		_ = s.shutdown(context.Background())
	}
}


func (p *dynamicProcessor[T]) shardCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.shards) // len(nil) == 0 after Shutdown
}

// Signal-specific wrappers — Go generics cannot directly satisfy processor.Traces etc.

type tracesProcessor struct {
	*dynamicProcessor[ptrace.Traces]
}

func (tp *tracesProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return tp.consumeData(ctx, td)
}

type metricsProcessor struct {
	*dynamicProcessor[pmetric.Metrics]
}

func (mp *metricsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return mp.consumeData(ctx, md)
}

type logsProcessor struct{ *dynamicProcessor[plog.Logs] }

func (lp *logsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return lp.consumeData(ctx, ld)
}
