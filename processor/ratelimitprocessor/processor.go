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
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/elastic/opentelemetry-collector-components/internal/sharedcomponent"
	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/telemetry"
)

type rateLimiterProcessor struct {
	component.Component
	rl               RateLimiter
	metadataKeys     []string
	telemetryBuilder *metadata.TelemetryBuilder
	tracerProvider   trace.TracerProvider
	logger           *zap.Logger
	inflight         *int64
	strategy         Strategy
}

type LogsRateLimiterProcessor struct {
	rateLimiterProcessor
	count func(logs plog.Logs) int
	next  func(ctx context.Context, logs plog.Logs) error
}

type MetricsRateLimiterProcessor struct {
	rateLimiterProcessor
	count func(metrics pmetric.Metrics) int
	next  func(ctx context.Context, metrics pmetric.Metrics) error
}

type TracesRateLimiterProcessor struct {
	rateLimiterProcessor
	count func(traces ptrace.Traces) int
	next  func(ctx context.Context, traces ptrace.Traces) error
}

type ProfilesRateLimiterProcessor struct {
	rateLimiterProcessor
	count func(profiles pprofile.Profiles) int
	next  func(ctx context.Context, profiles pprofile.Profiles) error
}

func NewLogsRateLimiterProcessor(
	rateLimiter *sharedcomponent.Component[rateLimiterComponent],
	logger *zap.Logger,
	telemetryBuilder *metadata.TelemetryBuilder,
	tracerProvider trace.TracerProvider,
	strategy Strategy,
	next func(ctx context.Context, logs plog.Logs) error,
	inflight *int64,
	metadataKeys []string,
) (*LogsRateLimiterProcessor, error) {
	return &LogsRateLimiterProcessor{
		rateLimiterProcessor: rateLimiterProcessor{
			Component:        rateLimiter,
			rl:               rateLimiter.Unwrap(),
			telemetryBuilder: telemetryBuilder,
			tracerProvider:   tracerProvider,
			logger:           logger,
			inflight:         inflight,
			metadataKeys:     metadataKeys,
			strategy:         strategy,
		},
		count: getLogsCountFunc(strategy),
		next:  next,
	}, nil
}

func NewMetricsRateLimiterProcessor(
	rateLimiter *sharedcomponent.Component[rateLimiterComponent],
	logger *zap.Logger,
	telemetryBuilder *metadata.TelemetryBuilder,
	tracerProvider trace.TracerProvider,
	strategy Strategy,
	next func(ctx context.Context, metrics pmetric.Metrics) error,
	inflight *int64, // used to calculate concurrent requests
	metadataKeys []string,
) (*MetricsRateLimiterProcessor, error) {
	return &MetricsRateLimiterProcessor{
		rateLimiterProcessor: rateLimiterProcessor{
			Component:        rateLimiter,
			rl:               rateLimiter.Unwrap(),
			telemetryBuilder: telemetryBuilder,
			tracerProvider:   tracerProvider,
			logger:           logger,
			inflight:         inflight,
			metadataKeys:     metadataKeys,
			strategy:         strategy,
		},
		count: getMetricsCountFunc(strategy),
		next:  next,
	}, nil
}

func NewTracesRateLimiterProcessor(
	rateLimiter *sharedcomponent.Component[rateLimiterComponent],
	logger *zap.Logger,
	telemetryBuilder *metadata.TelemetryBuilder,
	tracerProvider trace.TracerProvider,
	strategy Strategy,
	next func(ctx context.Context, traces ptrace.Traces) error,
	inflight *int64,
	metadataKeys []string,
) (*TracesRateLimiterProcessor, error) {
	return &TracesRateLimiterProcessor{
		rateLimiterProcessor: rateLimiterProcessor{
			Component:        rateLimiter,
			rl:               rateLimiter.Unwrap(),
			telemetryBuilder: telemetryBuilder,
			tracerProvider:   tracerProvider,
			logger:           logger,
			inflight:         inflight,
			metadataKeys:     metadataKeys,
			strategy:         strategy,
		},
		count: getTracesCountFunc(strategy),
		next:  next,
	}, nil
}

func NewProfilesRateLimiterProcessor(
	rateLimiter *sharedcomponent.Component[rateLimiterComponent],
	logger *zap.Logger,
	telemetryBuilder *metadata.TelemetryBuilder,
	tracerProvider trace.TracerProvider,
	strategy Strategy,
	next func(ctx context.Context, profiles pprofile.Profiles) error,
	inflight *int64,
	metadataKeys []string,
) (*ProfilesRateLimiterProcessor, error) {
	return &ProfilesRateLimiterProcessor{
		rateLimiterProcessor: rateLimiterProcessor{
			Component:        rateLimiter,
			rl:               rateLimiter.Unwrap(),
			telemetryBuilder: telemetryBuilder,
			tracerProvider:   tracerProvider,
			logger:           logger,
			inflight:         inflight,
			metadataKeys:     metadataKeys,
			strategy:         strategy,
		},
		count: getProfilesCountFunc(strategy),
		next:  next,
	}, nil
}

func (r *LogsRateLimiterProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (r *MetricsRateLimiterProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (r *TracesRateLimiterProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (r *ProfilesRateLimiterProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func getTelemetryAttrs(attrsCommon []attribute.KeyValue, err error) (attrs []attribute.KeyValue) {
	switch {
	case err == nil:
		attrs = append(attrsCommon,
			telemetry.WithReason(telemetry.StatusUnderLimit),
			telemetry.WithDecision("accepted"),
		)
	case status.Code(err) == codes.ResourceExhausted:
		attrs = append(attrsCommon,
			telemetry.WithDecision("throttled"),
		)
	default:
		attrs = append(attrsCommon,
			telemetry.WithReason(telemetry.RequestErr),
			telemetry.WithDecision("accepted"),
		)
	}

	return attrs
}

func rateLimit(ctx context.Context,
	hits int,
	rateLimit func(ctx context.Context, n int) error,
	metadataKeys []string,
	tb *metadata.TelemetryBuilder,
	logger *zap.Logger,
	inflight *int64,
) error {
	current := atomic.AddInt64(inflight, 1)
	attrsCommon := getAttrsFromContext(ctx, metadataKeys)
	attrsSet := attribute.NewSet(attrsCommon...)
	tb.RatelimitConcurrentRequests.Record(ctx, current,
		metric.WithAttributeSet(attrsSet),
	)

	defer func(start time.Time) {
		atomic.AddInt64(inflight, -1)
		tb.RatelimitRequestDuration.Record(ctx, time.Since(start).Seconds(),
			metric.WithAttributeSet(attrsSet),
		)
	}(time.Now())

	err := rateLimit(ctx, hits)
	attrRequests := getTelemetryAttrs(attrsCommon, err)
	attrRequestsSet := attribute.NewSet(attrRequests...)
	tb.RatelimitRequestSize.Record(ctx, int64(hits),
		metric.WithAttributeSet(attrRequestsSet),
	)
	if err != nil {
		// enhance error logging with metadata keys
		fields := make([]zap.Field, 0, len(attrsCommon)+1)
		fields = append(fields, zap.Int("hits", hits))
		for _, kv := range attrsCommon {
			switch kv.Value.Type() {
			case attribute.STRINGSLICE:
				fields = append(fields, zap.Strings(string(kv.Key), kv.Value.AsStringSlice()))
			default:
				fields = append(fields, zap.String(string(kv.Key), kv.Value.AsString()))
			}
		}
		logger.Error("request is over the limits defined by the rate limiter", append(fields, zap.Error(err))...)
	}

	tb.RatelimitRequests.Add(ctx, 1, metric.WithAttributeSet(attrRequestsSet))
	return err
}

func (r *LogsRateLimiterProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	if err := rateLimit(
		ctx,
		r.count(ld),
		r.rl.RateLimit,
		r.metadataKeys,
		r.telemetryBuilder,
		r.logger,
		r.inflight,
	); err != nil {
		return err
	}
	return r.next(ctx, ld)
}

func (r *MetricsRateLimiterProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if err := rateLimit(
		ctx,
		r.count(md),
		r.rl.RateLimit,
		r.metadataKeys,
		r.telemetryBuilder,
		r.logger,
		r.inflight,
	); err != nil {
		return err
	}
	return r.next(ctx, md)
}

func (r *TracesRateLimiterProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	if err := rateLimit(
		ctx,
		r.count(td),
		r.rl.RateLimit,
		r.metadataKeys,
		r.telemetryBuilder,
		r.logger,
		r.inflight,
	); err != nil {
		return err
	}
	return r.next(ctx, td)
}

func (r *ProfilesRateLimiterProcessor) ConsumeProfiles(ctx context.Context, pd pprofile.Profiles) error {
	if err := rateLimit(
		ctx,
		r.count(pd),
		r.rl.RateLimit,
		r.metadataKeys,
		r.telemetryBuilder,
		r.logger,
		r.inflight,
	); err != nil {
		return err
	}
	return r.next(ctx, pd)
}

func getLogsCountFunc(strategy Strategy) func(ld plog.Logs) int {
	switch strategy {
	case StrategyRateLimitRequests:
		return func(plog.Logs) int {
			return 1
		}
	case StrategyRateLimitRecords:
		return func(ld plog.Logs) int {
			return ld.LogRecordCount()
		}
	case StrategyRateLimitBytes:
		return func(ld plog.Logs) int {
			pm := plog.ProtoMarshaler{}
			return pm.LogsSize(ld)
		}
	}
	return nil // cannot happen, prevented by config.Validate()
}

func getMetricsCountFunc(strategy Strategy) func(md pmetric.Metrics) int {
	switch strategy {
	case StrategyRateLimitRequests:
		return func(pmetric.Metrics) int {
			return 1
		}
	case StrategyRateLimitRecords:
		return func(md pmetric.Metrics) int {
			return md.DataPointCount()
		}
	case StrategyRateLimitBytes:
		return func(md pmetric.Metrics) int {
			pm := pmetric.ProtoMarshaler{}
			return pm.MetricsSize(md)
		}
	}
	return nil // cannot happen, prevented by config.Validate()
}

func getTracesCountFunc(strategy Strategy) func(td ptrace.Traces) int {
	switch strategy {
	case StrategyRateLimitRequests:
		return func(ptrace.Traces) int {
			return 1
		}
	case StrategyRateLimitRecords:
		return func(td ptrace.Traces) int {
			return td.SpanCount()
		}
	case StrategyRateLimitBytes:
		return func(td ptrace.Traces) int {
			pm := ptrace.ProtoMarshaler{}
			return pm.TracesSize(td)
		}
	}
	return nil // cannot happen, prevented by config.Validate()
}

func getProfilesCountFunc(strategy Strategy) func(pd pprofile.Profiles) int {
	switch strategy {
	case StrategyRateLimitRequests:
		return func(pprofile.Profiles) int {
			return 1
		}
	case StrategyRateLimitRecords:
		return func(pd pprofile.Profiles) int {
			return pd.SampleCount()
		}
	case StrategyRateLimitBytes:
		return func(pd pprofile.Profiles) int {
			pm := pprofile.ProtoMarshaler{}
			return pm.ProfilesSize(pd)
		}
	}
	return nil // cannot happen, prevented by config.Validate()
}
