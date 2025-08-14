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

package elasticapmconnector // import "github.com/elastic/opentelemetry-collector-components/connector/elasticapmconnector"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"

	"github.com/elastic/opentelemetry-collector-components/connector/elasticapmconnector/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/internal/sharedcomponent"
)

type sharedcomponentKey struct {
	component.ID
	component.Config
}

var connectors = sharedcomponent.NewMap[sharedcomponentKey, *elasticapmConnector]()

// NewFactory returns a connector.Factory.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		metadata.Type,
		createDefaultConfig,
		connector.WithLogsToMetrics(createLogsToMetrics, metadata.LogsToMetricsStability),
		connector.WithMetricsToMetrics(createMetricsToMetrics, metadata.MetricsToMetricsStability),
		connector.WithTracesToMetrics(createTracesToMetrics, metadata.TracesToMetricsStability),
	)
}

// createDefaultConfig creates the default configuration.
func createDefaultConfig() component.Config {
	return &Config{
		Aggregation: &AggregationConfig{
			Limits: AggregationLimitConfig{
				ResourceLimit: LimitConfig{
					MaxCardinality: 8000,
				},
				ScopeLimit: LimitConfig{
					MaxCardinality: 4000,
				},
				MetricLimit: LimitConfig{
					MaxCardinality: 4000,
				},
				DatapointLimit: LimitConfig{
					MaxCardinality: 4000,
				},
			},
		},
	}
}

func createLogsToMetrics(
	ctx context.Context,
	set connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (connector.Logs, error) {
	c, err := connectors.LoadOrStore(sharedcomponentKey{ID: set.ID, Config: cfg}, func() (*elasticapmConnector, error) {
		return newElasticAPMConnector(ctx, cfg.(*Config), set, nextConsumer)
	})
	if err != nil {
		return nil, err
	}
	logsConsumer, err := c.Unwrap().newLogsConsumer(ctx)
	if err != nil {
		return nil, err
	}
	type sharedConnector struct {
		*sharedcomponent.Component[*elasticapmConnector]
		consumer.Logs
	}
	return sharedConnector{Component: c, Logs: logsConsumer}, nil
}

func createMetricsToMetrics(
	ctx context.Context,
	set connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (connector.Metrics, error) {
	c, err := connectors.LoadOrStore(sharedcomponentKey{ID: set.ID, Config: cfg}, func() (*elasticapmConnector, error) {
		return newElasticAPMConnector(ctx, cfg.(*Config), set, nextConsumer)
	})
	if err != nil {
		return nil, err
	}
	metricsConsumer, err := c.Unwrap().newMetricsConsumer(ctx)
	if err != nil {
		return nil, err
	}
	type sharedConnector struct {
		*sharedcomponent.Component[*elasticapmConnector]
		consumer.Metrics
	}
	return sharedConnector{Component: c, Metrics: metricsConsumer}, nil
}

func createTracesToMetrics(
	ctx context.Context,
	set connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (connector.Traces, error) {
	c, err := connectors.LoadOrStore(sharedcomponentKey{ID: set.ID, Config: cfg}, func() (*elasticapmConnector, error) {
		return newElasticAPMConnector(ctx, cfg.(*Config), set, nextConsumer)
	})
	if err != nil {
		return nil, err
	}
	tracesConsumer, err := c.Unwrap().newTracesToMetrics(ctx)
	if err != nil {
		return nil, err
	}
	type sharedConnector struct {
		*sharedcomponent.Component[*elasticapmConnector]
		consumer.Traces
	}
	return sharedConnector{Component: c, Traces: tracesConsumer}, nil
}
