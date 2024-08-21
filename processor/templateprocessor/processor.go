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

package templateprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/templateprocessor"

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"

	"github.com/elastic/opentelemetry-collector-components/internal/templates"
)

type templateProcessor struct {
	params              processor.Settings
	config              *Config
	components          []component.Component
	capabilities        consumer.Capabilities
	nextMetricsConsumer consumer.Metrics
	nextLogsConsumer    consumer.Logs
	nextTracesConsumer  consumer.Traces

	metrics consumer.Metrics
	logs    consumer.Logs
	traces  consumer.Traces
}

func newTemplateLogsProcessor(params processor.Settings, config *Config, consumer consumer.Logs) *templateProcessor {
	return &templateProcessor{
		params:           params,
		config:           config,
		nextLogsConsumer: consumer,
	}
}

func newTemplateMetricsProcessor(params processor.Settings, config *Config, consumer consumer.Metrics) *templateProcessor {
	return &templateProcessor{
		params:              params,
		config:              config,
		nextMetricsConsumer: consumer,
	}
}

func newTemplateTracesProcessor(params processor.Settings, config *Config, consumer consumer.Traces) *templateProcessor {
	return &templateProcessor{
		params:             params,
		config:             config,
		nextTracesConsumer: consumer,
	}
}

// factoryGetter is an interface that the component.Host passed to processorcreator's Start function must implement
// GetFactory is optional in hosts since 107.0, but we require it.
type factoryGetter interface {
	component.Host
	GetFactory(component.Kind, component.Type) component.Factory
}

func (r *templateProcessor) Start(ctx context.Context, ch component.Host) error {
	host, ok := ch.(factoryGetter)
	if !ok {
		return fmt.Errorf("templateprocessor is not compatible with the provided component.Host")
	}

	template, err := templates.Find(ctx, r.params.Logger, host, r.config.Name, r.config.Version)
	if err != nil {
		return fmt.Errorf("failed to find template %q: %w", r.config.Name, err)
	}

	resolver, err := newResolver(template, r.config.Parameters)
	if err != nil {
		return fmt.Errorf("failed to create configuration resolver: %w", err)
	}
	conf, err := resolver.Resolve(ctx)
	if err != nil {
		return fmt.Errorf("failed to resolve template %q: %w", r.config.Name, err)
	}

	var templateConfig templates.Config
	err = conf.Unmarshal(&templateConfig)
	if err != nil {
		return fmt.Errorf("failed to decode template %q: %w", r.config.Name, err)
	}

	selectedPipelines := []component.ID{}
	if r.config.Pipeline != nil {
		selectedPipelines = append(selectedPipelines, *r.config.Pipeline)
	}
	pipelines, err := selectComponents(templateConfig.Pipelines, selectedPipelines)
	if err != nil {
		return fmt.Errorf("failed to select component: %w", err)
	}
	if len(pipelines) > 1 {
		return errors.New("multiple pipelines selected, select only one")
	}
	for id, pipeline := range pipelines {
		err := r.startPipeline(ctx, host, templateConfig, id, pipeline)
		if err != nil {
			// Shutdown components that had been already started for cleanup.
			if err := r.Shutdown(ctx); err != nil {
				r.params.Logger.Warn("Failed to shutdown all components on error while starting",
					zap.String("error", err.Error()))
			}
			return fmt.Errorf("failed to start pipeline %q: %w", id, err)
		}
	}

	return nil
}

func (r *templateProcessor) startPipeline(ctx context.Context, host factoryGetter, config templates.Config, pipelineID component.ID, pipeline templates.PipelineConfig) error {
	r.logs = r.nextLogsConsumer
	r.metrics = r.nextMetricsConsumer
	r.traces = r.nextTracesConsumer

	processors := slices.Clone(pipeline.Processors)
	slices.Reverse(processors)
	for i, id := range processors {
		processorConfig, found := config.Processors[id]
		if !found {
			return fmt.Errorf("processor %q not found", id)
		}

		factory, ok := host.GetFactory(component.KindProcessor, id.Type()).(processor.Factory)
		if !ok {
			return fmt.Errorf("could not find processor factory for %q", id.Type())
		}

		config, err := prepareComponentConfig(factory.CreateDefaultConfig, processorConfig)
		if err != nil {
			return fmt.Errorf("could not compose processor config for %s: %w", id.String(), err)
		}

		params := processor.Settings(r.params)
		params.ID = component.NewIDWithName(factory.Type(), fmt.Sprintf("%s-%s-%d", r.params.ID, pipelineID, i))
		params.Logger = params.Logger.With(zap.String("name", params.ID.String()))
		if r.logs != nil {
			logs, err := factory.CreateLogsProcessor(ctx, params, config, r.logs)
			if err != nil {
				return fmt.Errorf("failed to create logs processor %s: %w", params.ID, err)
			}
			r.logs = logs
			r.components = append(r.components, logs)
		}
		if r.metrics != nil {
			metrics, err := factory.CreateMetricsProcessor(ctx, params, config, r.metrics)
			if err != nil {
				return fmt.Errorf("failed to create metrics processor %s: %w", params.ID, err)
			}
			r.metrics = metrics
			r.components = append(r.components, metrics)
		}
		if r.traces != nil {
			traces, err := factory.CreateTracesProcessor(ctx, params, config, r.traces)
			if err != nil {
				return fmt.Errorf("failed to create traces processor %s: %w", params.ID, err)
			}
			r.traces = traces
			r.components = append(r.components, traces)
		}
	}

	for _, component := range r.components {
		err := component.Start(ctx, host)
		if err != nil {
			return fmt.Errorf("failed to start component %q: %w", component, err)
		}
	}

	return nil
}

func (r *templateProcessor) Shutdown(ctx context.Context) error {
	// Shutdown them in reverse order as they were created.
	components := slices.Clone(r.components)
	slices.Reverse(r.components)
	for _, c := range components {
		err := c.Shutdown(ctx)
		if err != nil {
			return fmt.Errorf("failed to shutdown component %s: %w", c, err)
		}
	}

	return nil
}

func (r *templateProcessor) Capabilities() consumer.Capabilities {
	return r.capabilities
}

func (r *templateProcessor) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	return r.logs.ConsumeLogs(ctx, logs)
}

func (r *templateProcessor) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	return r.metrics.ConsumeMetrics(ctx, metrics)
}

func (r *templateProcessor) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	return r.traces.ConsumeTraces(ctx, traces)
}

func newResolver(template templates.Template, variables map[string]any) (*confmap.Resolver, error) {
	settings := confmap.ResolverSettings{
		URIs: []string{template.URI()},
		ProviderFactories: []confmap.ProviderFactory{
			template.ProviderFactory(),
			newVariablesProviderFactory(variables),
		},
	}
	return confmap.NewResolver(settings)
}

func selectComponents[C any](from map[component.ID]C, selection []component.ID) (map[component.ID]C, error) {
	if len(selection) == 0 {
		// If no selection, select all.
		return from, nil
	}
	selected := make(map[component.ID]C)
	for _, id := range selection {
		component, found := from[id]
		if !found {
			return nil, fmt.Errorf("component %s not found", id.String())
		}
		selected[id] = component
	}
	return selected, nil
}

func prepareComponentConfig(create func() component.Config, config map[string]any) (component.Config, error) {
	prepared := create()
	received := confmap.NewFromStringMap(config)
	err := received.Unmarshal(prepared)
	return prepared, err
}
