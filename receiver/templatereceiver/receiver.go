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

package templatereceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/templatereceiver"

import (
	"context"
	"fmt"
	"slices"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"

	"github.com/elastic/opentelemetry-collector-components/pkg/templates"
)

type templateReceiver struct {
	params              receiver.Settings
	config              *Config
	components          []component.Component
	nextMetricsConsumer consumer.Metrics
	nextLogsConsumer    consumer.Logs
	nextTracesConsumer  consumer.Traces
}

func newTemplateLogsReceiver(params receiver.Settings, config *Config, consumer consumer.Logs) *templateReceiver {
	return &templateReceiver{
		params:           params,
		config:           config,
		nextLogsConsumer: consumer,
	}
}

func newTemplateMetricsReceiver(params receiver.Settings, config *Config, consumer consumer.Metrics) *templateReceiver {
	return &templateReceiver{
		params:              params,
		config:              config,
		nextMetricsConsumer: consumer,
	}
}

func newTemplateTracesReceiver(params receiver.Settings, config *Config, consumer consumer.Traces) *templateReceiver {
	return &templateReceiver{
		params:             params,
		config:             config,
		nextTracesConsumer: consumer,
	}
}

// factoryGetter is an interface that the component.Host passed to receivercreator's Start function must implement
// GetFactory is optional in hosts since 107.0, but we require it.
type factoryGetter interface {
	component.Host
	GetFactory(component.Kind, component.Type) component.Factory
}

func (r *templateReceiver) Start(ctx context.Context, ch component.Host) error {
	host, ok := ch.(factoryGetter)
	if !ok {
		return fmt.Errorf("templatereceiver is not compatible with the provided component.Host")
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

	pipelines, err := selectComponents(templateConfig.Pipelines, r.config.Pipelines)
	if err != nil {
		return fmt.Errorf("failed to select component: %w", err)
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

func (r *templateReceiver) startPipeline(ctx context.Context, host factoryGetter, config templates.Config, pipelineID component.ID, pipeline templates.PipelineConfig) error {
	consumerChain := struct {
		logs    consumer.Logs
		metrics consumer.Metrics
		traces  consumer.Traces
	}{
		logs:    r.nextLogsConsumer,
		metrics: r.nextMetricsConsumer,
		traces:  r.nextTracesConsumer,
	}

	receiverConfig, found := config.Receivers[pipeline.Receiver]
	if !found {
		return fmt.Errorf("receiver %q not found", pipeline.Receiver)
	}

	receiverFactory, ok := host.GetFactory(component.KindReceiver, pipeline.Receiver.Type()).(receiver.Factory)
	if !ok {
		return fmt.Errorf("could not find receiver factory for %q", pipeline.Receiver.Type())
	}

	preparedConfig, err := prepareComponentConfig(receiverFactory.CreateDefaultConfig, receiverConfig)
	if err != nil {
		return fmt.Errorf("could not compose receiver config for %s: %w", pipeline.Receiver.String(), err)
	}

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
		if consumerChain.logs != nil {
			logs, err := factory.CreateLogsProcessor(ctx, params, config, consumerChain.logs)
			if err != nil {
				return fmt.Errorf("failed to create logs processor %s: %w", params.ID, err)
			}
			consumerChain.logs = logs
			r.components = append(r.components, logs)
		}
		if consumerChain.metrics != nil {
			metrics, err := factory.CreateMetricsProcessor(ctx, params, config, consumerChain.metrics)
			if err != nil {
				return fmt.Errorf("failed to create metrics processor %s: %w", params.ID, err)
			}
			consumerChain.metrics = metrics
			r.components = append(r.components, metrics)
		}
		if consumerChain.traces != nil {
			traces, err := factory.CreateTracesProcessor(ctx, params, config, consumerChain.traces)
			if err != nil {
				return fmt.Errorf("failed to create traces processor %s: %w", params.ID, err)
			}
			consumerChain.traces = traces
			r.components = append(r.components, traces)
		}
	}

	params := r.params
	params.ID = component.NewIDWithName(receiverFactory.Type(), fmt.Sprintf("%s-receiver", pipelineID))
	params.Logger = params.Logger.With(zap.String("name", params.ID.String()))
	if consumerChain.logs != nil {
		logs, err := receiverFactory.CreateLogsReceiver(ctx, params, preparedConfig, consumerChain.logs)
		if err != nil {
			return fmt.Errorf("failed to create logs processor %s: %w", params.ID, err)
		}
		r.components = append(r.components, logs)
	}
	if consumerChain.metrics != nil {
		metrics, err := receiverFactory.CreateMetricsReceiver(ctx, params, preparedConfig, consumerChain.metrics)
		if err != nil {
			return fmt.Errorf("failed to create metrics processor %s: %w", params.ID, err)
		}
		r.components = append(r.components, metrics)
	}
	if consumerChain.traces != nil {
		traces, err := receiverFactory.CreateTracesReceiver(ctx, params, preparedConfig, consumerChain.traces)
		if err != nil {
			return fmt.Errorf("failed to create traces processor %s: %w", params.ID, err)
		}
		r.components = append(r.components, traces)
	}

	for _, component := range r.components {
		err := component.Start(ctx, host)
		if err != nil {
			return fmt.Errorf("failed to start component %q: %w", component, err)
		}
	}

	return nil
}

func (r *templateReceiver) Shutdown(ctx context.Context) error {
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
