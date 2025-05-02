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
	"go.opentelemetry.io/otel/attribute"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/gubernator"
)

var _ RateLimiter = (*gubernatorRateLimiter)(nil)

var limitPercentDims = []float64{0.95, 0.90, 0.85, 0.75, 0.50, 0.25}

type gubernatorRateLimiter struct {
	cfg      *Config
	set      processor.Settings
	behavior gubernator.Behavior

	conn   *grpc.ClientConn
	client gubernator.V1Client

	telemetry *ratelimitProcessorTelemetry
}

func newGubernatorRateLimiter(cfg *Config, set processor.Settings) (*gubernatorRateLimiter, error) {
	var behavior int32
	for _, b := range cfg.Gubernator.Behavior {
		value, ok := gubernator.Behavior_value[strings.ToUpper(string(b))]
		if !ok {
			return nil, fmt.Errorf("invalid behavior %q", b)
		}
		behavior |= value
	}

	rpt, err := newRatelimitProcessorTelemetry(set)
	if err != nil {
		return nil, fmt.Errorf("error creating rate limit processor telemetry: %w", err)
	}

	return &gubernatorRateLimiter{
		cfg:       cfg,
		set:       set,
		behavior:  gubernator.Behavior(behavior),
		telemetry: rpt,
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
	uniqueKey := getUniqueKey(ctx, r.cfg.MetadataKeys)
	createdAt := time.Now().UnixMilli()

	rateAttr := attribute.Int("max_bytes_per_sec", r.cfg.Rate)
	burstAttr := attribute.Int("burst", r.cfg.Burst)
	processorIDAttr := attribute.String("processor_id", r.set.ID.String())
	metadataKeysAttr := attribute.StringSlice("metadata_keys", r.cfg.MetadataKeys)

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
		r.telemetry.record(
			processorIDAttr,
			rateAttr,
			burstAttr,
			metadataKeysAttr,
			attribute.String("ratelimit_decision", "accepted"),
			attribute.String("reason", "request_error"))
		return errRateLimitInternalError
	}

	// Inside the gRPC response, we should have a single-item list of responses.
	responses := getRateLimitsResp.GetResponses()
	if n := len(responses); n != 1 {
		r.telemetry.record(
			processorIDAttr,
			rateAttr,
			burstAttr,
			metadataKeysAttr,
			attribute.String("ratelimit_decision", "accepted"),
			attribute.String("reason", "request_error"))
		return fmt.Errorf("expected 1 response from gubernator, got %d", n)
	}

	resp := responses[0]
	if resp.GetError() != "" {
		r.set.Logger.Error("failed to get response from gubernator", zap.Error(errors.New(resp.GetError())))
		r.telemetry.record(
			processorIDAttr,
			rateAttr,
			burstAttr,
			metadataKeysAttr,
			attribute.String("ratelimit_decision", "accepted"),
			attribute.String("reason", "limit_error"))
		return errRateLimitInternalError
	}

	var limitPercentUsed float64
	if resp.Limit == 0 {
		limitPercentUsed = 1.0
	} else {
		limitPercentUsed = 1.0 - min(1, float64(resp.Remaining)/float64(resp.Limit))
	}

	limitBucket := getLimitThresholdBucket(limitPercentUsed)

	if resp.GetStatus() != gubernator.Status_UNDER_LIMIT {
		// Same logic as local
		switch r.cfg.ThrottleBehavior {
		case ThrottleBehaviorError:
			r.set.Logger.Error(
				"request is over the limits defined by the rate limiter",
				zap.Error(errTooManyRequests),
				zap.String("processor_id", r.set.ID.String()),
				zap.Strings("metadata_keys", r.cfg.MetadataKeys),
			)
			r.telemetry.record(
				processorIDAttr,
				rateAttr,
				burstAttr,
				metadataKeysAttr,
				attribute.String("ratelimit_decision", "throttled"),
				attribute.Float64("limit_threshold", limitBucket),
				attribute.String("throttle_behavior", "error"))
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
			r.telemetry.record(
				processorIDAttr,
				rateAttr,
				burstAttr,
				metadataKeysAttr,
				attribute.String("ratelimit_decision", "throttled"),
				attribute.Float64("limit_threshold", limitBucket),
				attribute.String("throttle_behavior", "delay"))
		}
	}

	r.telemetry.record(
		processorIDAttr,
		rateAttr,
		burstAttr,
		metadataKeysAttr,
		attribute.String("ratelimit_decision", "accepted"),
		attribute.Float64("limit_threshold", limitBucket),
		attribute.String("reason", "under_limit"))
	return nil
}

func getLimitThresholdBucket(percentUsed float64) float64 {
	for _, pct := range limitPercentDims {
		if percentUsed >= pct {
			return pct
		}
	}
	return 0
}
