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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/xprocessor"
	"go.opentelemetry.io/otel/trace"

	"github.com/elastic/opentelemetry-collector-components/internal/sharedcomponent"
	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"
)

var rateLimiters = sharedcomponent.NewMap[*Config, rateLimiterComponent]()

type rateLimiterComponent interface {
	component.Component
	RateLimiter
}

func NewFactory() xprocessor.Factory {
	return xprocessor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		xprocessor.WithProfiles(createProfilesProcessor, metadata.ProfilesStability),
		xprocessor.WithTraces(createTracesProcessor, metadata.TracesStability),
		xprocessor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
		xprocessor.WithLogs(createLogsProcessor, metadata.LogsStability),
	)
}

func getRateLimiter(
	config *Config,
	set processor.Settings,
	telemetryBuilder *metadata.TelemetryBuilder,
	tracerProvider trace.TracerProvider,
) (*sharedcomponent.Component[rateLimiterComponent], error) {
	return rateLimiters.LoadOrStore(config, func() (rateLimiterComponent, error) {
		if config.Type == GubernatorRateLimiter {
			return newGubernatorRateLimiter(config, set.Logger, telemetryBuilder, tracerProvider)
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
	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	rateLimiter, err := getRateLimiter(config, set, tb, set.TracerProvider)
	if err != nil {
		return nil, err
	}
	return NewLogsRateLimiterProcessor(
		rateLimiter,
		set.Logger,
		tb,
		set.TracerProvider,
		config.Strategy,
		func(ctx context.Context, ld plog.Logs) error {
			return nextConsumer.ConsumeLogs(ctx, ld)
		},
		config.MetadataKeys,
	)
}

func createMetricsProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	config := cfg.(*Config)
	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	rateLimiter, err := getRateLimiter(config, set, tb, set.TracerProvider)
	if err != nil {
		return nil, err
	}
	return NewMetricsRateLimiterProcessor(
		rateLimiter,
		set.Logger,
		tb,
		set.TracerProvider,
		config.Strategy,
		func(ctx context.Context, md pmetric.Metrics) error {
			return nextConsumer.ConsumeMetrics(ctx, md)
		},
		config.MetadataKeys,
	)
}

func createTracesProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	config := cfg.(*Config)
	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	rateLimiter, err := getRateLimiter(config, set, tb, set.TracerProvider)
	if err != nil {
		return nil, err
	}
	return NewTracesRateLimiterProcessor(
		rateLimiter,
		set.Logger,
		tb,
		set.TracerProvider,
		config.Strategy,
		func(ctx context.Context, td ptrace.Traces) error {
			return nextConsumer.ConsumeTraces(ctx, td)
		},
		config.MetadataKeys,
	)
}

func createProfilesProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer xconsumer.Profiles,
) (xprocessor.Profiles, error) {
	config := cfg.(*Config)
	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	rateLimiter, err := getRateLimiter(config, set, tb, set.TracerProvider)
	if err != nil {
		return nil, err
	}
	return NewProfilesRateLimiterProcessor(
		rateLimiter,
		set.Logger,
		tb,
		set.TracerProvider,
		config.Strategy,
		func(ctx context.Context, td pprofile.Profiles) error {
			return nextConsumer.ConsumeProfiles(ctx, td)
		},
		config.MetadataKeys,
	)
}
