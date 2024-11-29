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

package integrationreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/integrationreceiver"

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	otelpipeline "go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"

	"github.com/elastic/opentelemetry-collector-components/internal/integrations"
)

type integrationReceiver struct {
	params              receiver.Settings
	config              *Config
	components          []component.Component
	nextMetricsConsumer consumer.Metrics
	nextLogsConsumer    consumer.Logs
	nextTracesConsumer  consumer.Traces
}

func newTemplateLogsReceiver(params receiver.Settings, config *Config, consumer consumer.Logs) *integrationReceiver {
	return &integrationReceiver{
		params:           params,
		config:           config,
		nextLogsConsumer: consumer,
	}
}

func newTemplateMetricsReceiver(params receiver.Settings, config *Config, consumer consumer.Metrics) *integrationReceiver {
	return &integrationReceiver{
		params:              params,
		config:              config,
		nextMetricsConsumer: consumer,
	}
}

func newTemplateTracesReceiver(params receiver.Settings, config *Config, consumer consumer.Traces) *integrationReceiver {
	return &integrationReceiver{
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

func (r *integrationReceiver) Start(ctx context.Context, ch component.Host) error {
	host, ok := ch.(factoryGetter)
	if !ok {
		return fmt.Errorf("integrationreceiver is not compatible with the provided component.Host")
	}

	integration, err := integrations.Find(ctx, r.params.Logger, host, r.config.Name, r.config.Version)
	if err != nil {
		return fmt.Errorf("failed to find integration %q: %w", r.config.Name, err)
	}

	resolver, err := newResolver(integration, r.config.Parameters)
	if err != nil {
		return fmt.Errorf("failed to create configuration resolver: %w", err)
	}
	conf, err := resolver.Resolve(ctx)
	if err != nil {
		return fmt.Errorf("failed to resolve integration %q: %w", r.config.Name, err)
	}

	var integrationConfig integrations.Config
	err = conf.Unmarshal(&integrationConfig)
	if err != nil {
		return fmt.Errorf("failed to decode integration %q: %w", r.config.Name, err)
	}

	pipelines, err := selectComponents(integrationConfig.Pipelines, r.config.Pipelines)
	if err != nil {
		return fmt.Errorf("failed to select component: %w", err)
	}
	for id, pipeline := range pipelines {
		if !r.hasConsumerForPipelineType(id) {
			continue
		}
		err := r.startPipeline(ctx, host, integrationConfig, id, pipeline)
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

func (r *integrationReceiver) hasConsumerForPipelineType(id component.ID) bool {
	switch id.Type().String() {
	case "logs":
		return r.nextLogsConsumer != nil
	case "metrics":
		return r.nextMetricsConsumer != nil
	case "traces":
		return r.nextTracesConsumer != nil
	default:
		r.params.Logger.Warn("unexpected pipeline type in integration", zap.String("id", id.String()))
		return false
	}
}

func (r *integrationReceiver) startPipeline(ctx context.Context, host factoryGetter, config integrations.Config, pipelineID component.ID, pipeline integrations.PipelineConfig) error {
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

	var components []component.Component
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
			logs, err := factory.CreateLogs(ctx, params, config, consumerChain.logs)
			if err != nil {
				return fmt.Errorf("failed to create logs processor %s: %w", params.ID, err)
			}
			consumerChain.logs = logs
			components = append(components, logs)
		}
		if consumerChain.metrics != nil {
			metrics, err := factory.CreateMetrics(ctx, params, config, consumerChain.metrics)
			if err != nil {
				return fmt.Errorf("failed to create metrics processor %s: %w", params.ID, err)
			}
			consumerChain.metrics = metrics
			components = append(components, metrics)
		}
		if consumerChain.traces != nil {
			traces, err := factory.CreateTraces(ctx, params, config, consumerChain.traces)
			if err != nil {
				return fmt.Errorf("failed to create traces processor %s: %w", params.ID, err)
			}
			consumerChain.traces = traces
			components = append(components, traces)
		}
	}

	params := r.params
	params.ID = component.NewIDWithName(receiverFactory.Type(), fmt.Sprintf("%s-receiver", pipelineID))
	params.Logger = params.Logger.With(zap.String("name", params.ID.String()))
	receiversCreated := 0
	if consumerChain.logs != nil {
		logs, err := receiverFactory.CreateLogs(ctx, params, preparedConfig, consumerChain.logs)
		switch {
		case err == nil:
			components = append(components, logs)
			receiversCreated += 1
		case errors.Is(err, otelpipeline.ErrSignalNotSupported):
			r.params.Logger.Debug("receiver does not support logs telemetry type",
				zap.String("integration", r.params.ID.String()),
				zap.String("receiver", params.ID.String()))
		default:
			return fmt.Errorf("failed to create logs receiver %s: %w", params.ID, err)
		}
	}
	if consumerChain.metrics != nil {
		metrics, err := receiverFactory.CreateMetrics(ctx, params, preparedConfig, consumerChain.metrics)
		switch {
		case err == nil:
			components = append(components, metrics)
			receiversCreated += 1
		case errors.Is(err, otelpipeline.ErrSignalNotSupported):
			r.params.Logger.Debug("receiver does not support metrics telemetry type",
				zap.String("integration", r.params.ID.String()),
				zap.String("receiver", params.ID.String()))
		default:
			return fmt.Errorf("failed to create metrics receiver %s: %w", params.ID, err)
		}
	}
	if consumerChain.traces != nil {
		traces, err := receiverFactory.CreateTraces(ctx, params, preparedConfig, consumerChain.traces)
		switch {
		case err == nil:
			components = append(components, traces)
			receiversCreated += 1
		case errors.Is(err, otelpipeline.ErrSignalNotSupported):
			r.params.Logger.Debug("receiver does not support traces telemetry type",
				zap.String("integration", r.params.ID.String()),
				zap.String("receiver", params.ID.String()))
		default:
			return fmt.Errorf("failed to create traces receiver %s: %w", params.ID, err)
		}
	}

	// If no receiver has been created the rest of the pipeline won't be used, so don't keep it.
	if receiversCreated == 0 {
		// Shutting down created components out of kindness, because they haven't been started yet.
		if err := shutdownComponents(ctx, components); err != nil {
			r.params.Logger.Error("failed to cleanup processors after receiver was not created",
				zap.String("integration", r.params.ID.String()),
				zap.String("receiver", params.ID.String()))
		}
		return nil
	}

	for _, component := range components {
		err := component.Start(ctx, host)
		if err != nil {
			return fmt.Errorf("failed to start component %q: %w", component, err)
		}
	}
	r.components = append(r.components, components...)

	return nil
}

func (r *integrationReceiver) Shutdown(ctx context.Context) error {
	return shutdownComponents(ctx, r.components)
}

func shutdownComponents(ctx context.Context, components []component.Component) error {
	// Shutdown them in reverse order as they were created.
	components = slices.Clone(components)
	slices.Reverse(components)
	for _, c := range components {
		err := c.Shutdown(ctx)
		if err != nil {
			return fmt.Errorf("failed to shutdown component %s: %w", c, err)
		}
	}

	return nil
}

func newResolver(integration integrations.Template, variables map[string]any) (*confmap.Resolver, error) {
	settings := confmap.ResolverSettings{
		URIs: []string{integration.URI()},
		ProviderFactories: []confmap.ProviderFactory{
			integration.ProviderFactory(),
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
