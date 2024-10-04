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

package spanmetricsconnectorv2 // import "github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2"

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/config"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/model"
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
		customOTTLFuncs[ottlspan.TransformContext](),
		set.TelemetrySettings,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTTL statement parser for datapoints: %w", err)
	}

	metricDefs := make([]model.MetricDef[ottlspan.TransformContext], 0, len(c.Spans))
	for _, info := range c.Spans {
		resAttrs, err := parseAttributeConfigs(info.IncludeResourceAttributes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse resource attribute config: %w", err)
		}
		attrs, err := parseAttributeConfigs(info.Attributes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse attribute config: %w", err)
		}
		md := model.MetricDef[ottlspan.TransformContext]{
			Key: model.MetricKey{
				Name:        info.Name,
				Description: info.Description,
			},
			EphemeralResourceAttribute: info.EphemeralResourceAttribute,
			IncludeResourceAttributes:  resAttrs,
			Attributes:                 attrs,
			Counter:                    info.Counter,
			ValueCountMetric: model.ValueCountMetric[ottlspan.TransformContext]{
				Unit:                 info.Unit,
				ExplicitHistogram:    info.Histogram.Explicit,
				ExponentialHistogram: info.Histogram.Exponential,
				Summary:              info.Summary,
				SumAndCount:          info.SumAndCount,
			},
		}
		if info.Statements.Count != "" {
			md.ValueCountMetric.CountStatement, err = parser.ParseStatement(info.Statements.Count)
			if err != nil {
				return nil, fmt.Errorf("failed to parse count OTTL statement: %w", err)
			}
		}
		if info.Statements.Value != "" {
			md.ValueCountMetric.ValueStatement, err = parser.ParseStatement(info.Statements.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to parse value OTTL statement: %w", err)
			}
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
		customOTTLFuncs[ottldatapoint.TransformContext](),
		set.TelemetrySettings,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTTL statement parser for datapoints: %w", err)
	}

	metricDefs := make([]model.MetricDef[ottldatapoint.TransformContext], 0, len(c.Datapoints))
	for _, info := range c.Datapoints {
		resAttrs, err := parseAttributeConfigs(info.IncludeResourceAttributes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse resource attribute config: %w", err)
		}
		attrs, err := parseAttributeConfigs(info.Attributes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse attribute config: %w", err)
		}
		md := model.MetricDef[ottldatapoint.TransformContext]{
			Key: model.MetricKey{
				Name:        info.Name,
				Description: info.Description,
			},
			EphemeralResourceAttribute: info.EphemeralResourceAttribute,
			IncludeResourceAttributes:  resAttrs,
			Attributes:                 attrs,
			Counter:                    info.Counter,
			ValueCountMetric: model.ValueCountMetric[ottldatapoint.TransformContext]{
				Unit:                 info.Unit,
				ExplicitHistogram:    info.Histogram.Explicit,
				ExponentialHistogram: info.Histogram.Exponential,
				Summary:              info.Summary,
				SumAndCount:          info.SumAndCount,
			},
		}
		if info.Statements.Count != "" {
			md.ValueCountMetric.CountStatement, err = parser.ParseStatement(info.Statements.Count)
			if err != nil {
				return nil, fmt.Errorf("failed to parse count OTTL statement: %w", err)
			}
		}
		if info.Statements.Value != "" {
			md.ValueCountMetric.ValueStatement, err = parser.ParseStatement(info.Statements.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to parse value OTTL statement: %w", err)
			}
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
		customOTTLFuncs[ottllog.TransformContext](),
		set.TelemetrySettings,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTTL statement parser for datapoints: %w", err)
	}

	metricDefs := make([]model.MetricDef[ottllog.TransformContext], 0, len(c.Logs))
	for _, info := range c.Logs {
		resAttrs, err := parseAttributeConfigs(info.IncludeResourceAttributes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse resource attribute config: %w", err)
		}
		attrs, err := parseAttributeConfigs(info.Attributes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse attribute config: %w", err)
		}
		md := model.MetricDef[ottllog.TransformContext]{
			Key: model.MetricKey{
				Name:        info.Name,
				Description: info.Description,
			},
			EphemeralResourceAttribute: info.EphemeralResourceAttribute,
			IncludeResourceAttributes:  resAttrs,
			Attributes:                 attrs,
			Counter:                    info.Counter,
			ValueCountMetric: model.ValueCountMetric[ottllog.TransformContext]{
				Unit:                 info.Unit,
				ExplicitHistogram:    info.Histogram.Explicit,
				ExponentialHistogram: info.Histogram.Exponential,
				Summary:              info.Summary,
				SumAndCount:          info.SumAndCount,
			},
		}
		if info.Statements.Count != "" {
			md.ValueCountMetric.CountStatement, err = parser.ParseStatement(info.Statements.Count)
			if err != nil {
				return nil, fmt.Errorf("failed to parse count OTTL statement: %w", err)
			}
		}
		if info.Statements.Value != "" {
			md.ValueCountMetric.ValueStatement, err = parser.ParseStatement(info.Statements.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to parse value OTTL statement: %w", err)
			}
		}
		metricDefs = append(metricDefs, md)
	}

	return &signalToMetrics{
		next:          nextConsumer,
		logMetricDefs: metricDefs,
		ephemeralID:   uuid.NewString(),
	}, nil
}

func parseAttributeConfigs(cfgs []config.Attribute) ([]model.AttributeKeyValue, error) {
	var errs []error
	kvs := make([]model.AttributeKeyValue, len(cfgs))
	for i, attr := range cfgs {
		val := pcommon.NewValueEmpty()
		if err := val.FromRaw(attr.DefaultValue); err != nil {
			errs = append(errs, err)
		}
		kvs[i] = model.AttributeKeyValue{
			Key:          attr.Key,
			DefaultValue: val,
		}
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return kvs, nil
}

func customOTTLFuncs[K any]() map[string]ottl.Factory[K] {
	getFactory := NewGetFactory[K]()
	standard := ottlfuncs.StandardFuncs[K]()
	standard[getFactory.Name()] = getFactory
	return standard
}
