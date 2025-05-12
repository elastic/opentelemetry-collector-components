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

package ratelimitprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor"

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/gubernator"
	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/telemetry"
)

var _ RateLimiter = (*gubernatorRateLimiter)(nil)

var limitPercentDims = []float64{0.95, 0.90, 0.85, 0.75, 0.50, 0.25}

type gubernatorRateLimiter struct {
	cfg      *Config
	set      processor.Settings
	behavior gubernator.Behavior

	conn             *grpc.ClientConn
	client           gubernator.V1Client
	telemetryBuilder *metadata.TelemetryBuilder

	activeRequests int64 // Tracking concurrent RateLimit calls
}

func newGubernatorRateLimiter(cfg *Config, set processor.Settings) (*gubernatorRateLimiter, error) {
	var behavior int32

	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	for _, b := range cfg.Gubernator.Behavior {
		value, ok := gubernator.Behavior_value[strings.ToUpper(string(b))]
		if !ok {
			return nil, fmt.Errorf("invalid behavior %q", b)
		}
		behavior |= value
	}
	return &gubernatorRateLimiter{
		cfg:              cfg,
		set:              set,
		behavior:         gubernator.Behavior(behavior),
		telemetryBuilder: telemetryBuilder,
	}, nil
}

func (r *gubernatorRateLimiter) Start(ctx context.Context, host component.Host) error {
	if r.cfg.Gubernator.Auth != nil && r.cfg.Gubernator.Auth.AuthenticatorID.String() == "" {
		// if we do not set this explicitly to nil, then it will fail when creating the connection with:
		// `failed to resolve authenticator "": authenticator not found`
		r.cfg.Gubernator.Auth = nil
	}
	conn, err := r.cfg.Gubernator.ToClientConn(ctx, host, r.set.TelemetrySettings)
	if err != nil {
		return fmt.Errorf("failed to connect to gubernator: %w", err)
	}
	r.conn = conn
	r.client = gubernator.NewV1Client(r.conn)
	return nil
}

func (r *gubernatorRateLimiter) Shutdown(ctx context.Context) error {
	if r.conn != nil {
		err := r.conn.Close()
		r.conn = nil
		r.client = nil
		return err
	}
	return nil
}

func (r *gubernatorRateLimiter) RateLimit(ctx context.Context, hits int) error {
	startTime := time.Now()
	atomic.AddInt64(&r.activeRequests, 1)
	defer func() {
		duration := time.Since(startTime).Seconds()
		r.requestDurationTelemetry(ctx, duration, []attribute.KeyValue{
			telemetry.WithProcessorID(r.set.ID.String()),
		})

		r.requestConcurrentTelemetry(ctx, atomic.LoadInt64(&r.activeRequests), []attribute.KeyValue{
			telemetry.WithProcessorID(r.set.ID.String()),
		})
		atomic.AddInt64(&r.activeRequests, -1)
	}()

	uniqueKey := getUniqueKey(ctx, r.cfg.MetadataKeys)

	createdAt := time.Now().UnixMilli()
	getRateLimitsResp, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{{
			Name:      r.set.ID.String(),
			UniqueKey: uniqueKey,
			Hits:      int64(hits),
			Behavior:  r.behavior,
			Algorithm: gubernator.Algorithm_LEAKY_BUCKET,
			Limit:     int64(r.cfg.Rate), // rate is per second
			Burst:     int64(r.cfg.Burst),
			Duration:  1000, // duration is in milliseconds, i.e. 1s
			CreatedAt: &createdAt,
		}},
	})
	if err != nil {
		r.set.Logger.Error("error executing gubernator rate limit request", zap.Error(err))
		r.requestTelemetry(ctx, []attribute.KeyValue{
			telemetry.WithProcessorID(r.set.ID.String()),
			telemetry.WithReason(telemetry.RequestErr),
			telemetry.WithDecision("accepted"),
		})
		return errRateLimitInternalError
	}

	// Inside the gRPC response, we should have a single-item list of responses.
	responses := getRateLimitsResp.GetResponses()
	if n := len(responses); n != 1 {
		r.requestTelemetry(ctx, []attribute.KeyValue{
			telemetry.WithProcessorID(r.set.ID.String()),
			telemetry.WithReason(telemetry.RequestErr),
			telemetry.WithDecision("accepted"),
		})
		return fmt.Errorf("expected 1 response from gubernator, got %d", n)
	}
	resp := responses[0]
	if resp.GetError() != "" {
		r.set.Logger.Error("failed to get response from gubernator", zap.Error(errors.New(resp.GetError())))
		r.requestTelemetry(ctx, []attribute.KeyValue{
			telemetry.WithProcessorID(r.set.ID.String()),
			telemetry.WithReason(telemetry.LimitError),
			telemetry.WithDecision("accepted"),
		})
		return errRateLimitInternalError
	}

	var limitPercentUsed float64
	if resp.Limit == 0 {
		limitPercentUsed = 1.0
	} else {
		limitPercentUsed = 1.0 - min(1, float64(resp.Remaining)/float64(resp.Limit))
	}

	limitBucket := getLimitThresholdBucket(limitPercentUsed)
	if resp.GetStatus() == gubernator.Status_OVER_LIMIT || resp.Remaining == 0 {
		// Same logic as local
		switch r.cfg.ThrottleBehavior {
		case ThrottleBehaviorError:
			r.set.Logger.Error(
				"request is over the limits defined by the rate limiter",
				zap.Error(errTooManyRequests),
				zap.String("processor_id", r.set.ID.String()),
				zap.Strings("metadata_keys", r.cfg.MetadataKeys),
			)
			r.requestTelemetry(ctx, []attribute.KeyValue{
				telemetry.WithProcessorID(r.set.ID.String()),
				telemetry.WithReason(telemetry.StatusOverLimit),
				telemetry.WithDecision("throttled"),
				telemetry.WithLimitThreshold(limitBucket),
			})
			return errTooManyRequests
		case ThrottleBehaviorDelay:
			delay := time.Duration(resp.GetResetTime()-createdAt) * time.Millisecond
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
			r.requestTelemetry(ctx, []attribute.KeyValue{
				telemetry.WithProcessorID(r.set.ID.String()),
				telemetry.WithReason(telemetry.StatusOverLimit),
				telemetry.WithDecision("throttled"),
				telemetry.WithLimitThreshold(limitBucket),
			})
			return errTooManyRequests
		}
	}

	r.requestTelemetry(ctx, []attribute.KeyValue{
		telemetry.WithProcessorID(r.set.ID.String()),
		telemetry.WithReason(telemetry.StatusUnderLimit),
		telemetry.WithDecision("accepted"),
		telemetry.WithLimitThreshold(limitBucket),
	})
	return nil
}

func (r *gubernatorRateLimiter) requestTelemetry(ctx context.Context, baseAttrs []attribute.KeyValue) {
	attrs := attrsFromMetadata(ctx, r.cfg.MetadataKeys, baseAttrs)
	r.telemetryBuilder.RatelimitRequests.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attrs...)))
}

func (r *gubernatorRateLimiter) requestDurationTelemetry(ctx context.Context, duration float64, baseAttrs []attribute.KeyValue) {
	attrs := attrsFromMetadata(ctx, r.cfg.MetadataKeys, baseAttrs)
	r.telemetryBuilder.RatelimitRequestDuration.Record(ctx, duration, metric.WithAttributeSet(attribute.NewSet(attrs...)))
}

func (r *gubernatorRateLimiter) requestConcurrentTelemetry(ctx context.Context, activeRequests int64, baseAttrs []attribute.KeyValue) {
	attrs := attrsFromMetadata(ctx, r.cfg.MetadataKeys, baseAttrs)
	r.telemetryBuilder.RatelimitConcurrentRequests.Record(ctx, activeRequests, metric.WithAttributeSet(attribute.NewSet(attrs...)))
}

func getLimitThresholdBucket(percentUsed float64) float64 {
	for _, pct := range limitPercentDims {
		if percentUsed >= pct {
			return pct
		}
	}
	return 0
}
