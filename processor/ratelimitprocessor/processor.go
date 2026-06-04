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
	strategy         Strategy
	statusReporter   *throttleStatusReporter
}

func (r *rateLimiterProcessor) Start(ctx context.Context, host component.Host) error {
	r.statusReporter.setHost(host)
	return r.Component.Start(ctx, host)
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
	metadataKeys []string,
) (*LogsRateLimiterProcessor, error) {
	return &LogsRateLimiterProcessor{
		rateLimiterProcessor: rateLimiterProcessor{
			Component:        rateLimiter,
			rl:               rateLimiter.Unwrap(),
			telemetryBuilder: telemetryBuilder,
			tracerProvider:   tracerProvider,
			logger:           logger,
			metadataKeys:     metadataKeys,
			strategy:         strategy,
			statusReporter:   newThrottleStatusReporter(),
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
	metadataKeys []string,
) (*MetricsRateLimiterProcessor, error) {
	return &MetricsRateLimiterProcessor{
		rateLimiterProcessor: rateLimiterProcessor{
			Component:        rateLimiter,
			rl:               rateLimiter.Unwrap(),
			telemetryBuilder: telemetryBuilder,
			tracerProvider:   tracerProvider,
			logger:           logger,
			metadataKeys:     metadataKeys,
			strategy:         strategy,
			statusReporter:   newThrottleStatusReporter(),
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
	metadataKeys []string,
) (*TracesRateLimiterProcessor, error) {
	return &TracesRateLimiterProcessor{
		rateLimiterProcessor: rateLimiterProcessor{
			Component:        rateLimiter,
			rl:               rateLimiter.Unwrap(),
			telemetryBuilder: telemetryBuilder,
			tracerProvider:   tracerProvider,
			logger:           logger,
			metadataKeys:     metadataKeys,
			strategy:         strategy,
			statusReporter:   newThrottleStatusReporter(),
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
	metadataKeys []string,
) (*ProfilesRateLimiterProcessor, error) {
	return &ProfilesRateLimiterProcessor{
		rateLimiterProcessor: rateLimiterProcessor{
			Component:        rateLimiter,
			rl:               rateLimiter.Unwrap(),
			telemetryBuilder: telemetryBuilder,
			tracerProvider:   tracerProvider,
			logger:           logger,
			metadataKeys:     metadataKeys,
			strategy:         strategy,
			statusReporter:   newThrottleStatusReporter(),
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

func getTelemetryAttrs(attrsCommon []attribute.KeyValue, result RateLimitResult, err error) []attribute.KeyValue {
	switch result.Decision {
	case DecisionDelayed, DecisionThrottled, DecisionCancelled:
		return append(attrsCommon,
			telemetry.WithDecision(string(result.Decision)),
			telemetry.WithReason(telemetry.StatusOverLimit),
		)
	default: // DecisionAccepted
		if err != nil {
			return append(attrsCommon,
				telemetry.WithDecision(string(DecisionAccepted)),
				telemetry.WithReason(telemetry.RequestErr),
			)
		}
		return append(attrsCommon,
			telemetry.WithDecision(string(DecisionAccepted)),
			telemetry.WithReason(telemetry.StatusUnderLimit),
		)
	}
}

func withRateLimit[T any](ctx context.Context,
	hits int,
	rateLimit func(ctx context.Context, n int) (RateLimitResult, error),
	metadataKeys []string,
	tb *metadata.TelemetryBuilder,
	logger *zap.Logger,
	sr *throttleStatusReporter,
	next func(ctx context.Context, data T) error,
	data T,
) error {
	attrsCommon := getAttrsFromContext(ctx, metadataKeys)
	attrsSet := attribute.NewSet(attrsCommon...)
	tb.RatelimitConcurrentRequests.Add(ctx, 1, metric.WithAttributeSet(attrsSet))
	defer tb.RatelimitConcurrentRequests.Add(ctx, -1, metric.WithAttributeSet(attrsSet))

	start := time.Now()
	result, err := rateLimit(ctx, hits)
	tb.RatelimitRequestDuration.Record(ctx,
		time.Since(start).Seconds(),
		metric.WithAttributeSet(attrsSet),
	)

	attrRequests := getTelemetryAttrs(attrsCommon, result, err)
	attrRequestsSet := attribute.NewSet(attrRequests...)
	tb.RatelimitRequestSize.Record(ctx, int64(hits), metric.WithAttributeSet(attrRequestsSet))
	tb.RatelimitRequests.Add(ctx, 1, metric.WithAttributeSet(attrRequestsSet))
	if result.Decision == DecisionDelayed {
		tb.RatelimitDelayDuration.Record(ctx, result.Delay.Seconds(), metric.WithAttributeSet(attrRequestsSet))
	}
	tokenAttrs := attribute.NewSet(append(attrsCommon, telemetry.WithLimitThreshold(result.ConfigRate))...)
	tb.RatelimitTokensAfter.Record(ctx, result.TokensAfter, metric.WithAttributeSet(tokenAttrs))
	tb.RatelimitTokensBefore.Record(ctx, result.TokensBefore, metric.WithAttributeSet(tokenAttrs))

	var isThrottledVal int64
	if sr.observe(result.TokensBefore < 0) {
		isThrottledVal = 1
	}
	tb.RatelimitIsThrottled.Record(ctx, isThrottledVal, metric.WithAttributeSet(tokenAttrs))
	if err != nil {
		if result.Decision != DecisionCancelled {
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
			logger.Error(
				"request is over the limits defined by the rate limiter",
				append(fields, zap.Error(err))...,
			)
		}
		return err
	}
	return next(ctx, data)
}

func (r *LogsRateLimiterProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return withRateLimit(
		ctx,
		r.count(ld),
		r.rl.RateLimit,
		r.metadataKeys,
		r.telemetryBuilder,
		r.logger,
		r.statusReporter,
		r.next, ld,
	)
}

func (r *MetricsRateLimiterProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return withRateLimit(
		ctx,
		r.count(md),
		r.rl.RateLimit,
		r.metadataKeys,
		r.telemetryBuilder,
		r.logger,
		r.statusReporter,
		r.next, md,
	)
}

func (r *TracesRateLimiterProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return withRateLimit(
		ctx,
		r.count(td),
		r.rl.RateLimit,
		r.metadataKeys,
		r.telemetryBuilder,
		r.logger,
		r.statusReporter,
		r.next, td,
	)
}

func (r *ProfilesRateLimiterProcessor) ConsumeProfiles(ctx context.Context, pd pprofile.Profiles) error {
	return withRateLimit(
		ctx,
		r.count(pd),
		r.rl.RateLimit,
		r.metadataKeys,
		r.telemetryBuilder,
		r.logger,
		r.statusReporter,
		r.next, pd,
	)
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
