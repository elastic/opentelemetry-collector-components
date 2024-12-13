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
	"go.opentelemetry.io/collector/consumer/consumerprofiles"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorprofiles"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/sharedcomponent"
)

var rateLimiters = sharedcomponent.NewMap[*Config, rateLimiterComponent]()

type rateLimiterComponent interface {
	component.Component
	RateLimiter
}

func NewFactory() processorprofiles.Factory {
	return processorprofiles.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processorprofiles.WithProfiles(createProfilesProcessor, metadata.ProfilesStability),
		processorprofiles.WithTraces(createTracesProcessor, metadata.TracesStability),
		processorprofiles.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
		processorprofiles.WithLogs(createLogsProcessor, metadata.LogsStability),
	)
}

func getRateLimiter(
	config *Config,
	set processor.Settings,
) (*sharedcomponent.Component[rateLimiterComponent], error) {
	return rateLimiters.LoadOrStore(config, func() (rateLimiterComponent, error) {
		if config.Gubernator != nil {
			return newGubernatorRateLimiter(config, set)
		}
		return newLocalRateLimiter(config, set)
	})
}

func createLogsProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	config := cfg.(*Config)
	rateLimiter, err := getRateLimiter(config, set)
	if err != nil {
		return nil, err
	}
	return NewLogsRateLimiterProcessor(
		rateLimiter,
		config.Strategy,
		func(ctx context.Context, ld plog.Logs) error {
			return nextConsumer.ConsumeLogs(ctx, ld)
		},
	), nil
}

func createMetricsProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	config := cfg.(*Config)
	rateLimiter, err := getRateLimiter(config, set)
	if err != nil {
		return nil, err
	}
	return NewMetricsRateLimiterProcessor(
		rateLimiter,
		config.Strategy,
		func(ctx context.Context, md pmetric.Metrics) error {
			return nextConsumer.ConsumeMetrics(ctx, md)
		},
	), nil
}

func createTracesProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	config := cfg.(*Config)
	rateLimiter, err := getRateLimiter(config, set)
	if err != nil {
		return nil, err
	}
	return NewTracesRateLimiterProcessor(
		rateLimiter,
		config.Strategy,
		func(ctx context.Context, td ptrace.Traces) error {
			return nextConsumer.ConsumeTraces(ctx, td)
		},
	), nil
}

func createProfilesProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumerprofiles.Profiles,
) (processorprofiles.Profiles, error) {
	config := cfg.(*Config)
	rateLimiter, err := getRateLimiter(config, set)
	if err != nil {
		return nil, err
	}
	return NewProfilesRateLimiterProcessor(
		rateLimiter,
		config.Strategy,
		func(ctx context.Context, td pprofile.Profiles) error {
			return nextConsumer.ConsumeProfiles(ctx, td)
		},
	), nil
}
