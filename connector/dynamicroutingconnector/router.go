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
	"math"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/cespare/xxhash/v2"
	"github.com/jellydator/ttlcache/v3"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/connector/dynamicroutingconnector/internal/metadata"
)

var _ component.Component = (*router[any])(nil)

const defaultCardinalityBucket = "default"
const maxPkCapacity = 256

// defaultRoutedOpt is a cached MeasurementOption for the default routing path
// (empty partition key, default bucket) to avoid per-call allocations.
var defaultRoutedOpt = metric.WithAttributeSet(attribute.NewSet(
	attribute.String("cardinality_bucket", defaultCardinalityBucket),
	attribute.String("partition_key", ""),
))

// consumerProvider is a function with a type parameter C (expected to be one
// of consumer.Traces, consumer.Metrics, or Consumer.Logs). returns a
// consumer for the given component ID(s).
type consumerProvider[C any] func(...pipeline.ID) (C, error)

type router[C any] struct {
	recordingInterval  time.Duration
	ttl                time.Duration
	partitionKeys      []string
	sortedMetadataKeys []string
	defaultConsumer    C
	consumers          []consumerThreshold[C]

	logger           *zap.Logger
	telemetryBuilder *metadata.TelemetryBuilder

	mu       sync.Mutex
	m        map[string]*hyperloglog.Sketch
	stop     chan struct{}
	stopped  chan struct{}
	decision *ttlcache.Cache[string, routingDecision[C]]
}

// consumerThreshold is a config-time structure mapping a consumer to its
// cardinality threshold and bucket label.
type consumerThreshold[C any] struct {
	consumer          C
	maxCount          float64
	cardinalityBucket string
}

// routingDecision is a cached per-partition-key entry holding the resolved
// consumer and a pre-built metric option.
type routingDecision[C any] struct {
	consumer C
	maxCount float64
	// routedOpt is a pre-built metric.MeasurementOption containing the
	// partition key and cardinality bucket attributes for this decision.
	// It is constructed once when the decision is cached (in updateDecisions)
	// and reused on every subsequent call, avoiding repeated attribute
	// allocations on the hot path.
	routedOpt metric.MeasurementOption
}

func newRouter[C any](
	cfg *Config,
	settings component.TelemetrySettings,
	provider consumerProvider[C],
) (*router[C], error) {
	sortedMetadataKeys := slices.Clone(cfg.RoutingKeys.MeasureBy)
	slices.Sort(sortedMetadataKeys)
	decisionCache := ttlcache.New(
		ttlcache.WithTTL[string, routingDecision[C]](cfg.TTL),
		ttlcache.WithDisableTouchOnHit[string, routingDecision[C]](),
	)
	consumers := make([]consumerThreshold[C], 0, len(cfg.RoutingPipelines))
	var prevMax float64
	for i, p := range cfg.RoutingPipelines {
		c, err := provider(p.Pipelines...)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer from provided pipelines at idx %d: %w", i, err)
		}
		consumers = append(consumers, consumerThreshold[C]{
			consumer:          c,
			maxCount:          p.MaxCardinality,
			cardinalityBucket: formatCardinalityBucket(prevMax, p.MaxCardinality),
		})
		prevMax = p.MaxCardinality
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

	telemetryBuilder, err := metadata.NewTelemetryBuilder(settings)
	if err != nil {
		return nil, fmt.Errorf("failed to create telemetry builder: %w", err)
	}
	return &router[C]{
		recordingInterval:  cfg.RecordingInterval,
		ttl:                cfg.TTL,
		partitionKeys:      cfg.RoutingKeys.PartitionBy,
		sortedMetadataKeys: sortedMetadataKeys,
		defaultConsumer:    defaultConsumer,
		consumers:          consumers,
		logger:             settings.Logger,
		telemetryBuilder:   telemetryBuilder,
		stop:               make(chan struct{}),
		decision:           decisionCache,
		m:                  make(map[string]*hyperloglog.Sketch),
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
		// Use timers to ensure that decision updates are always at least
		// recording interval apart.
		timer := time.NewTimer(r.recordingInterval)
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
			timer.Reset(r.recordingInterval)
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
	if r.telemetryBuilder != nil {
		r.telemetryBuilder.Shutdown()
	}
	return nil
}

func (r *router[C]) Process(ctx context.Context) C {
	pk := r.partitionKey(ctx)
	return r.getNextConsumerAndMaybeRecord(ctx, pk)
}

func (r *router[C]) partitionKey(ctx context.Context) string {
	clientMeta := client.FromContext(ctx).Metadata
	pkb := make([]byte, 0, maxPkCapacity)
	for _, k := range r.partitionKeys {
		vs := clientMeta.Get(k)
		if len(vs) == 0 {
			continue
		}
		for _, v := range vs {
			pkb = append(pkb, v...)
			pkb = append(pkb, ':')
		}
		pkb = append(pkb, ';')
	}
	return string(pkb)
}

func (r *router[C]) hashSum(ctx context.Context) uint64 {
	clientMeta := client.FromContext(ctx).Metadata
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
				zap.String("metadata_key", k),
			)
		}
		for _, v := range vs {
			// xxhash writes are not expected to return an error, we are logging
			// errors for such unexpected cases and continuing nonetheless.
			if _, err := hash.WriteString(":"); err != nil {
				r.logger.Error(
					"unexpected failure on creating hash key from client metadata",
					zap.Error(err),
					zap.String("metadata_key", k),
					zap.String("value", v),
				)
			}
			if _, err := hash.WriteString(v); err != nil {
				r.logger.Error(
					"unexpected failure on creating hash key from client metadata",
					zap.Error(err),
					zap.String("metadata_key", k),
					zap.String("value", v),
				)
			}
		}
	}
	return hash.Sum64()
}

func (r *router[C]) recordCardinality(pk string, hashSum uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	hll, ok := r.m[pk]
	if !ok || hll == nil {
		hll = hyperloglog.New()
		r.m[pk] = hll
	}
	hll.InsertHash(hashSum)
}

func (r *router[C]) getNextConsumerAndMaybeRecord(ctx context.Context, pk string) C {
	if pk == "" {
		if ce := r.logger.Check(zap.DebugLevel, "returning default consumer due to empty primary key"); ce != nil {
			ce.Write(zap.String("primary_key", pk))
		}
		r.telemetryBuilder.DynamicroutingRouted.Add(ctx, 1, defaultRoutedOpt)
		return r.defaultConsumer
	}

	item := r.decision.Get(pk)
	if item == nil || time.Until(item.ExpiresAt()) <= r.recordingInterval {
		r.recordCardinality(pk, r.hashSum(ctx))
	}

	if item == nil {
		if ce := r.logger.Check(zap.DebugLevel, "returning default consumer due to missing decision"); ce != nil {
			ce.Write(zap.String("primary_key", pk))
		}
		// Cold path: no cached decision yet, build the option on the fly.
		r.telemetryBuilder.DynamicroutingRouted.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
			attribute.String("cardinality_bucket", defaultCardinalityBucket),
			attribute.String("partition_key", pk),
		)))
		return r.defaultConsumer
	}
	next := item.Value()
	if ce := r.logger.Check(zap.DebugLevel, "returning non-default consumer"); ce != nil {
		ce.Write(zap.String("primary_key", pk), zap.Float64("max_count", next.maxCount))
	}
	r.telemetryBuilder.DynamicroutingRouted.Add(ctx, 1, next.routedOpt)
	return next.consumer
}

// updateDecisions refreshes decision cache entries from the most recently
// recorded HLL window.
func (r *router[C]) updateDecisions() {
	r.mu.Lock()
	oldM := r.m
	r.m = make(map[string]*hyperloglog.Sketch)
	r.mu.Unlock()

	newDecision := make(map[string]routingDecision[C], len(oldM))
	for k, hll := range oldM {
		estimate := hll.Estimate()
		for _, c := range r.consumers {
			if float64(estimate) <= c.maxCount {
				newDecision[k] = routingDecision[C]{
					consumer: c.consumer,
					maxCount: c.maxCount,
					routedOpt: metric.WithAttributeSet(attribute.NewSet(
						attribute.String("cardinality_bucket", c.cardinalityBucket),
						attribute.String("partition_key", k),
					)),
				}
				break
			}
		}
	}
	r.decision.DeleteExpired()
	for k, c := range newDecision {
		r.decision.Set(k, c, r.ttl)
	}
}


func formatCardinalityBucket(prevMax float64, max float64) string {
	return formatCardinality(prevMax) + "_" + formatCardinality(max)
}

func formatCardinality(value float64) string {
	switch {
	case math.IsInf(value, 1):
		return "inf"
	case math.IsInf(value, -1):
		return "-inf"
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}
