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

package dynamicroutingconnector // import "github.com/elastic/opentelemetry-collector-components/connector/dynamicroutingconnector"

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap"
)

var _ component.Component = (*router[any])(nil)

// consumerProvider is a function with a type parameter C (expected to be one
// of consumer.Traces, consumer.Metrics, or Consumer.Logs). returns a
// consumer for the given component ID(s).
type consumerProvider[C any] func(...pipeline.ID) (C, error)

type router[C any] struct {
	evaluationInterval time.Duration
	primaryMetadataKey string
	sortedMetadataKeys []string
	defaultConsumer    C
	thresholds         []int
	consumers          []C

	// TODO(lahsivjar): Fix the chaos of Start<>Shutdown orchestration
	stop    chan struct{}
	stopped chan struct{}

	logger *zap.Logger

	dmu      sync.RWMutex
	decision map[string]*hyperloglog.Sketch

	mu sync.Mutex
	m  map[string]*hyperloglog.Sketch
}

func newRouter[C any](
	cfg *Config,
	settings component.TelemetrySettings,
	provider consumerProvider[C],
) (*router[C], error) {
	sortedMetadataKeys := slices.Clone(cfg.MetadataKeys)
	slices.Sort(sortedMetadataKeys)

	consumers := make([]C, 0, len(cfg.Pipelines))
	for i, p := range cfg.Pipelines {
		c, err := provider(p...)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer from provided pipelines at idx %d: %w", i, err)
		}
		consumers = append(consumers, c)
	}

	var (
		err             error
		defaultConsumer C
	)
	if len(cfg.DefaultPipelines) > 0 {
		defaultConsumer, err = provider(cfg.DefaultPipelines...)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer from default pipelines: %w", err)
		}
	}
	return &router[C]{
		evaluationInterval: cfg.EvaluationInterval,
		primaryMetadataKey: cfg.PrimaryMetadataKey,
		sortedMetadataKeys: sortedMetadataKeys,
		defaultConsumer:    defaultConsumer,
		thresholds:         cfg.Thresholds,
		consumers:          consumers,
		stop:               make(chan struct{}),
		decision:           make(map[string]*hyperloglog.Sketch),
		m:                  make(map[string]*hyperloglog.Sketch),
	}, nil
}

func (r *router[C]) Start(ctx context.Context, _ component.Host) error {
	// TODO(lahsivjar): Protect by lock to handle multiple calls to start
	if r.stopped == nil {
		r.stopped = make(chan struct{})
	}

	select {
	case <-r.stop:
		// Already signaled to be stopped
		return nil
	case <-r.stopped:
		// Already stopped
		return nil
	default:
	}

	go func() {
		defer close(r.stopped)
		// Use timers to ensure that decions are always atleast
		// evaluation interval apart.
		timer := time.NewTimer(r.evaluationInterval)
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stop:
				// stop has been signaled
				return
			case <-timer.C:
			}

			r.updateDecisions()
			timer.Reset(r.evaluationInterval)
		}
	}()
	return nil
}

func (r *router[C]) Shutdown(ctx context.Context) error {
	select {
	case <-r.stop:
		// Shutdown has already been called once
		return nil
	default:
	}

	close(r.stop)
	if r.stopped != nil {
		// Start has ran before
		select {
		case <-ctx.Done():
			// shutdown context done
			return fmt.Errorf("failed to shutdown due to context timeout while waiting for router to stop: %w", ctx.Err())
		case <-r.stopped:
			// wait for evaluation goroutine to stop
		}
	}
	return nil
}

func (r *router[C]) Process(ctx context.Context) C {
	return r.getNextConsumer(r.estimateCardinality(ctx))
}

func (r *router[C]) estimateCardinality(ctx context.Context) string {
	clientMeta := client.FromContext(ctx).Metadata
	var pk string
	switch pks := clientMeta.Get(r.primaryMetadataKey); len(pks) {
	case 0:
		return ""
	case 1:
		pk = pks[0]
	default:
		pk = strings.Join(pks, ":")
	}

	var hash xxhash.Digest
	for _, k := range r.sortedMetadataKeys {
		vs := clientMeta.Get(k)
		if len(vs) == 0 {
			continue
		}
		hash.WriteString(k)
		for _, v := range vs {
			hash.WriteString(":")
			hash.WriteString(v)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	hll, ok := r.m[pk]
	if !ok || hll == nil {
		hll = hyperloglog.New()
		r.m[pk] = hll
	}
	hll.InsertHash(hash.Sum64())
	return pk
}

func (r *router[C]) getNextConsumer(pk string) C {
	if pk == "" {
		return r.defaultConsumer
	}

	r.dmu.RLock()
	defer r.dmu.RUnlock()

	hll, ok := r.decision[pk]
	if !ok {
		return r.defaultConsumer
	}
	return r.consumers[sort.SearchInts(r.thresholds, int(hll.Estimate()))]
}

// updateDecisions updates the current cache to be used for decisions, discarding
// the previous decision map.
func (r *router[C]) updateDecisions() {
	r.dmu.Lock()
	defer r.dmu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.decision = r.m
	// TODO(lahsivjar): Map allocation can be optimized by reusing
	// the older decision map.
	r.m = make(map[string]*hyperloglog.Sketch)
}
