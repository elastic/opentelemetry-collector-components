// ELASTICSEARCH CONFIDENTIAL
// __________________
//
//  Copyright Elasticsearch B.V. All rights reserved.
//
// NOTICE:  All information contained herein is, and remains
// the property of Elasticsearch B.V. and its suppliers, if any.
// The intellectual and technical concepts contained herein
// are proprietary to Elasticsearch B.V. and its suppliers and
// may be covered by U.S. and Foreign Patents, patents in
// process, and are protected by trade secret or copyright
// law.  Dissemination of this information or reproduction of
// this material is strictly forbidden unless prior written
// permission is obtained from Elasticsearch B.V.

package ratelimitprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/sharedcomponent"
)

type rateLimiterProcessor struct {
	component.Component
	rl RateLimiter
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
	strategy Strategy,
	next func(ctx context.Context, logs plog.Logs) error,
) *LogsRateLimiterProcessor {
	return &LogsRateLimiterProcessor{
		rateLimiterProcessor: rateLimiterProcessor{
			Component: rateLimiter,
			rl:        rateLimiter.Unwrap(),
		},
		count: getLogsCountFunc(strategy),
		next:  next,
	}
}

func NewMetricsRateLimiterProcessor(
	rateLimiter *sharedcomponent.Component[rateLimiterComponent],
	strategy Strategy,
	next func(ctx context.Context, metrics pmetric.Metrics) error,
) *MetricsRateLimiterProcessor {
	return &MetricsRateLimiterProcessor{
		rateLimiterProcessor: rateLimiterProcessor{
			Component: rateLimiter,
			rl:        rateLimiter.Unwrap(),
		},
		count: getMetricsCountFunc(strategy),
		next:  next,
	}
}

func NewTracesRateLimiterProcessor(
	rateLimiter *sharedcomponent.Component[rateLimiterComponent],
	strategy Strategy,
	next func(ctx context.Context, traces ptrace.Traces) error,
) *TracesRateLimiterProcessor {
	return &TracesRateLimiterProcessor{
		rateLimiterProcessor: rateLimiterProcessor{
			Component: rateLimiter,
			rl:        rateLimiter.Unwrap(),
		},
		count: getTracesCountFunc(strategy),
		next:  next,
	}
}

func NewProfilesRateLimiterProcessor(
	rateLimiter *sharedcomponent.Component[rateLimiterComponent],
	strategy Strategy,
	next func(ctx context.Context, profiles pprofile.Profiles) error,
) *ProfilesRateLimiterProcessor {
	return &ProfilesRateLimiterProcessor{
		rateLimiterProcessor: rateLimiterProcessor{
			Component: rateLimiter,
			rl:        rateLimiter.Unwrap(),
		},
		count: getProfilesCountFunc(strategy),
		next:  next,
	}
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

func (r *LogsRateLimiterProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	hits := r.count(ld)
	if err := r.rl.RateLimit(ctx, hits); err != nil {
		return err
	}
	return r.next(ctx, ld)
}

func (r *MetricsRateLimiterProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	hits := r.count(md)
	if err := r.rl.RateLimit(ctx, hits); err != nil {
		return err
	}
	return r.next(ctx, md)
}

func (r *TracesRateLimiterProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	hits := r.count(td)
	if err := r.rl.RateLimit(ctx, hits); err != nil {
		return err
	}
	return r.next(ctx, td)
}

func (r *ProfilesRateLimiterProcessor) ConsumeProfiles(ctx context.Context, pd pprofile.Profiles) error {
	hits := r.count(pd)
	if err := r.rl.RateLimit(ctx, hits); err != nil {
		return err
	}
	return r.next(ctx, pd)
}

func getLogsCountFunc(strategy Strategy) func(ld plog.Logs) int {
	switch strategy {
	case StrategyRateLimitRequests:
		return func(ld plog.Logs) int {
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
	return nil
}

func getMetricsCountFunc(strategy Strategy) func(md pmetric.Metrics) int {
	switch strategy {
	case StrategyRateLimitRequests:
		return func(md pmetric.Metrics) int {
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
	// cannot happen, prevented by config.Validate()
	return nil
}

func getTracesCountFunc(strategy Strategy) func(td ptrace.Traces) int {
	switch strategy {
	case StrategyRateLimitRequests:
		return func(td ptrace.Traces) int {
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
	// cannot happen, prevented by config.Validate()
	return nil
}

func getProfilesCountFunc(strategy Strategy) func(pd pprofile.Profiles) int {
	switch strategy {
	case StrategyRateLimitRequests:
		return func(pd pprofile.Profiles) int {
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
	// cannot happen, prevented by config.Validate()
	return nil
}
