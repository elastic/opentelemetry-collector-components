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

package elasticapmprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

	"github.com/elastic/opentelemetry-collector-components/internal/sharedcomponent"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/metadata"
)

type processorKey struct {
	component.ID
	component.Config
}

var processors = sharedcomponent.NewMap[processorKey, *elasticapmProcessor]()

// NewFactory returns a processor.Factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, metadata.LogsStability),
		processor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
	)
}

// createDefaultConfig creates the default configuration.
func createDefaultConfig() component.Config {
	return &Config{}
}

func createLogsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	p, err := processors.LoadOrStore(processorKey{ID: set.ID, Config: cfg}, func() (*elasticapmProcessor, error) {
		return newElasticAPMProcessor(ctx, cfg.(*Config), set)
	})
	if err != nil {
		return nil, err
	}
	logsConsumer, err := p.Unwrap().newLogsConsumer(ctx, nextConsumer)
	if err != nil {
		return nil, err
	}
	type wrapper struct {
		*sharedcomponent.Component[*elasticapmProcessor]
		consumer.Logs
	}
	return wrapper{Component: p, Logs: logsConsumer}, nil
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	p, err := processors.LoadOrStore(processorKey{ID: set.ID, Config: cfg}, func() (*elasticapmProcessor, error) {
		return newElasticAPMProcessor(ctx, cfg.(*Config), set)
	})
	if err != nil {
		return nil, err
	}
	if err := p.Unwrap().setNextMetrics(nextConsumer); err != nil {
		return nil, err
	}

	metricsConsumer, err := p.Unwrap().newMetricsConsumer(ctx, nextConsumer)
	if err != nil {
		return nil, err
	}
	type wrapper struct {
		*sharedcomponent.Component[*elasticapmProcessor]
		consumer.Metrics
	}
	return wrapper{Component: p, Metrics: metricsConsumer}, nil
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	p, err := processors.LoadOrStore(processorKey{ID: set.ID, Config: cfg}, func() (*elasticapmProcessor, error) {
		return newElasticAPMProcessor(ctx, cfg.(*Config), set)
	})
	if err != nil {
		return nil, err
	}
	tracesConsumer, err := p.Unwrap().newTracesConsumer(ctx, nextConsumer)
	if err != nil {
		return nil, err
	}
	type wrapper struct {
		*sharedcomponent.Component[*elasticapmProcessor]
		consumer.Traces
	}
	return wrapper{Component: p, Traces: tracesConsumer}, nil
}
