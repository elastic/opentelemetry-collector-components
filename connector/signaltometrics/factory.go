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

package signaltometricsconnector // import "github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector"

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"

	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/config"
	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/model"
	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/ottlget"
)

// NewFactory returns a ConnectorFactory.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		metadata.Type,
		createDefaultConfig,
		connector.WithTracesToMetrics(createTracesToMetrics, metadata.TracesToMetricsStability),
		connector.WithMetricsToMetrics(createMetricsToMetrics, metadata.MetricsToMetricsStability),
		connector.WithLogsToMetrics(createLogsToMetrics, metadata.LogsToMetricsStability),
	)
}

// createDefaultConfig creates the default configuration.
func createDefaultConfig() component.Config {
	return &config.Config{}
}

// createTracesToMetrics creates a traces to metrics connector based on provided config.
func createTracesToMetrics(
	_ context.Context,
	set connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (connector.Traces, error) {
	c := cfg.(*config.Config)
	parser, err := ottlspan.NewParser(
		ottlget.CustomFuncs[ottlspan.TransformContext](),
		set.TelemetrySettings,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTTL statement parser for datapoints: %w", err)
	}

	metricDefs := make([]model.MetricDef[ottlspan.TransformContext], 0, len(c.Spans))
	for _, info := range c.Spans {
		var md model.MetricDef[ottlspan.TransformContext]
		if err := md.FromMetricInfo(info, parser); err != nil {
			return nil, fmt.Errorf("failed to parse provided metric information; %w", err)
		}
		metricDefs = append(metricDefs, md)
	}

	return &signalToMetrics{
		next:           nextConsumer,
		spanMetricDefs: metricDefs,
		ephemeralID:    uuid.NewString(),
	}, nil
}

func createMetricsToMetrics(
	_ context.Context,
	set connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (connector.Metrics, error) {
	c := cfg.(*config.Config)
	parser, err := ottldatapoint.NewParser(
		ottlget.CustomFuncs[ottldatapoint.TransformContext](),
		set.TelemetrySettings,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTTL statement parser for datapoints: %w", err)
	}

	metricDefs := make([]model.MetricDef[ottldatapoint.TransformContext], 0, len(c.Datapoints))
	for _, info := range c.Datapoints {
		var md model.MetricDef[ottldatapoint.TransformContext]
		if err := md.FromMetricInfo(info, parser); err != nil {
			return nil, fmt.Errorf("failed to parse provided metric information; %w", err)
		}
		metricDefs = append(metricDefs, md)
	}

	return &signalToMetrics{
		next:         nextConsumer,
		dpMetricDefs: metricDefs,
		ephemeralID:  uuid.NewString(),
	}, nil
}

func createLogsToMetrics(
	_ context.Context,
	set connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (connector.Logs, error) {
	c := cfg.(*config.Config)
	parser, err := ottllog.NewParser(
		ottlget.CustomFuncs[ottllog.TransformContext](),
		set.TelemetrySettings,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTTL statement parser for datapoints: %w", err)
	}

	metricDefs := make([]model.MetricDef[ottllog.TransformContext], 0, len(c.Logs))
	for _, info := range c.Logs {
		var md model.MetricDef[ottllog.TransformContext]
		if err := md.FromMetricInfo(info, parser); err != nil {
			return nil, fmt.Errorf("failed to parse provided metric information; %w", err)
		}
		metricDefs = append(metricDefs, md)
	}

	return &signalToMetrics{
		next:          nextConsumer,
		logMetricDefs: metricDefs,
		ephemeralID:   uuid.NewString(),
	}, nil
}
