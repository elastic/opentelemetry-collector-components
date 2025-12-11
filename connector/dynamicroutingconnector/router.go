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
	evaluationInterval  time.Duration
	primaryMetadataKeys []string
	sortedMetadataKeys  []string
	defaultConsumer     C
	consumers           []ct[C]

	logger *zap.Logger

	mu      sync.Mutex
	m       map[string]*hyperloglog.Sketch
	stop    chan struct{}
	stopped chan struct{}

	dmu      sync.RWMutex
	decision map[string]ct[C]
}

type ct[C any] struct {
	consumer C
	maxCount float64
}

func newRouter[C any](
	cfg *Config,
	settings component.TelemetrySettings,
	provider consumerProvider[C],
) (*router[C], error) {
	sortedMetadataKeys := slices.Clone(cfg.MetadataKeys)
	slices.Sort(sortedMetadataKeys)
	consumers := make([]ct[C], 0, len(cfg.DynamicPipelines))
	for i, p := range cfg.DynamicPipelines {
		c, err := provider(p.Pipelines...)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer from provided pipelines at idx %d: %w", i, err)
		}
		consumers = append(consumers, ct[C]{consumer: c, maxCount: p.MaxCount})
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
		evaluationInterval:  cfg.EvaluationInterval,
		primaryMetadataKeys: cfg.PrimaryMetadataKeys,
		sortedMetadataKeys:  sortedMetadataKeys,
		defaultConsumer:     defaultConsumer,
		consumers:           consumers,
		logger:              settings.Logger,
		stop:                make(chan struct{}),
		decision:            make(map[string]ct[C]),
		m:                   make(map[string]*hyperloglog.Sketch),
	}, nil
}

func (r *router[C]) Start(ctx context.Context, _ component.Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()

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
	r.mu.Lock()
	select {
	case <-r.stop:
		// Shutdown has already been called once
		r.mu.Unlock()
		return nil
	default:
	}
	close(r.stop)
	r.mu.Unlock()

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
	var pkb strings.Builder
	for _, k := range r.primaryMetadataKeys {
		vs := clientMeta.Get(k)
		if len(vs) == 0 {
			continue
		}
		for _, mk := range vs {
			if _, err := pkb.WriteString(mk); err != nil {
				r.logger.Error(
					"unexpected failure on concatenating primary metadata keys",
					zap.Error(err),
				)
			}
			if err := pkb.WriteByte(':'); err != nil {
				r.logger.Error(
					"unexpected failure on concatenating primary metadata keys",
					zap.Error(err),
				)
			}
		}
		if err := pkb.WriteByte(';'); err != nil {
			r.logger.Error(
				"unexpected failure on concatenating primary metadata keys",
				zap.Error(err),
			)
		}
	}

	var hash xxhash.Digest
	for _, k := range r.sortedMetadataKeys {
		vs := clientMeta.Get(k)
		if len(vs) == 0 {
			continue
		}
		if _, err := hash.WriteString(k); err != nil {
			// xxhash writes are not expected to return an error, we are logging
			// errors for such unexpected cases and continuing nonetheless.
			r.logger.Error(
				"unexpected failure on creating hash key from client metadata",
				zap.Error(err),
			)
		}
		for _, v := range vs {
			// xxhash writes are not expected to return an error, we are logging
			// errors for such unexpected cases and continuing nonetheless.
			if _, err := hash.WriteString(":"); err != nil {
				r.logger.Error(
					"unexpected failure on creating hash key from client metadata",
					zap.Error(err),
				)
			}
			if _, err := hash.WriteString(v); err != nil {
				r.logger.Error(
					"unexpected failure on creating hash key from client metadata",
					zap.Error(err),
				)
			}
		}
	}

	pk := pkb.String()

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
		r.logger.Debug(
			"returning default consumer due to empty primary key",
			zap.String("primary_key", pk),
		)
		return r.defaultConsumer
	}

	r.dmu.RLock()
	defer r.dmu.RUnlock()

	next, ok := r.decision[pk]
	if !ok {
		r.logger.Debug(
			"returning default consumer due to missing decision",
			zap.String("primary_key", pk),
		)
		return r.defaultConsumer
	}
	r.logger.Debug(
		"returning non-default consumer",
		zap.String("primary_key", pk),
		zap.Float64("max_count", next.maxCount),
	)
	return next.consumer
}

// updateDecisions updates the current cache to be used for decisions, discarding
// the previous decision map.
func (r *router[C]) updateDecisions() {
	r.mu.Lock()
	oldM := r.m
	r.m = make(map[string]*hyperloglog.Sketch)
	r.mu.Unlock()

	newDecision := make(map[string]ct[C], len(oldM))
	for k, hll := range oldM {
		estimate := hll.Estimate()
		for _, c := range r.consumers {
			if float64(estimate) <= c.maxCount {
				newDecision[k] = c
				break
			}
		}
	}

	r.dmu.Lock()
	defer r.dmu.Unlock()
	r.decision = newDecision
}
