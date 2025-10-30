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

package pipeline

import (
	"context"
	"fmt"
	"slices"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/extension/elasticpipelineextension/internal/elasticsearch"
)

// Signal-specific component creation methods

// createSignalExporters creates exporters for a specific signal type.
func (m *Manager) createSignalExporters(
	ctx context.Context,
	signalType string,
	exporterNames []string,
	config elasticsearch.PipelineConfig,
	pipelineID string,
) ([]component.Component, error) {
	var exporters []component.Component

	for _, exporterName := range exporterNames {
		exporterConfigInterface, exists := config.Exporters[exporterName]
		if !exists {
			return nil, fmt.Errorf("exporter %q not found in configuration", exporterName)
		}

		exporterConfig, ok := exporterConfigInterface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid exporter config format for %q", exporterName)
		}

		exporterType := component.MustNewType(exporterName)
		factory, ok := m.host.GetFactory(component.KindExporter, exporterType).(exporter.Factory)
		if !ok {
			return nil, fmt.Errorf("exporter factory not found for type %q", exporterName)
		}

		// Create default config and merge with provided config
		defaultCfg := factory.CreateDefaultConfig()
		mergedCfg, err := m.mergeComponentConfig(defaultCfg, exporterConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to merge exporter config for %q: %w", exporterName, err)
		}

		exporterID := component.NewIDWithName(factory.Type(), fmt.Sprintf("%s-%s-%s", pipelineID, signalType, exporterName))
		settings := exporter.Settings{
			ID:                exporterID,
			TelemetrySettings: component.TelemetrySettings{},
			BuildInfo:         component.BuildInfo{},
		}

		// Create exporter for the specific signal type
		var exp component.Component
		switch signalType {
		case "logs":
			if factory.LogsStability() != component.StabilityLevelUndefined {
				exp, err = factory.CreateLogs(ctx, settings, mergedCfg)
			} else {
				m.logger.Debug("Exporter does not support logs", zap.String("exporter", exporterName))
				continue
			}
		case "metrics":
			if factory.MetricsStability() != component.StabilityLevelUndefined {
				exp, err = factory.CreateMetrics(ctx, settings, mergedCfg)
			} else {
				m.logger.Debug("Exporter does not support metrics", zap.String("exporter", exporterName))
				continue
			}
		case "traces":
			if factory.TracesStability() != component.StabilityLevelUndefined {
				exp, err = factory.CreateTraces(ctx, settings, mergedCfg)
			} else {
				m.logger.Debug("Exporter does not support traces", zap.String("exporter", exporterName))
				continue
			}
		default:
			return nil, fmt.Errorf("unsupported signal type: %s", signalType)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to create %s exporter %q: %w", signalType, exporterName, err)
		}

		if exp != nil {
			exporters = append(exporters, exp)
		}
	}

	if len(exporters) == 0 {
		return nil, fmt.Errorf("no valid exporters created for signal type %s", signalType)
	}

	return exporters, nil
}

// createSignalProcessors creates processors for a specific signal type.
func (m *Manager) createSignalProcessors(
	ctx context.Context,
	signalType string,
	processorNames []string,
	config elasticsearch.PipelineConfig,
	nextConsumer interface{}, // consumer.Logs, consumer.Metrics, or consumer.Traces
	pipelineID string,
) ([]component.Component, interface{}, error) {
	if len(processorNames) == 0 {
		return nil, nextConsumer, nil
	}

	var processors []component.Component
	currentConsumer := nextConsumer

	// Process in reverse order to build chain correctly
	reversedNames := make([]string, len(processorNames))
	copy(reversedNames, processorNames)
	slices.Reverse(reversedNames)

	for _, processorName := range reversedNames {
		processorConfigInterface, exists := config.Processors[processorName]
		if !exists {
			return nil, nil, fmt.Errorf("processor %q not found in configuration", processorName)
		}

		processorConfig, ok := processorConfigInterface.(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("invalid processor config format for %q", processorName)
		}

		processorType := component.MustNewType(processorName)
		factory, ok := m.host.GetFactory(component.KindProcessor, processorType).(processor.Factory)
		if !ok {
			return nil, nil, fmt.Errorf("processor factory not found for type %q", processorName)
		}

		// Create default config and merge with provided config
		defaultCfg := factory.CreateDefaultConfig()
		mergedCfg, err := m.mergeComponentConfig(defaultCfg, processorConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to merge processor config for %q: %w", processorName, err)
		}

		processorID := component.NewIDWithName(factory.Type(), fmt.Sprintf("%s-%s-%s", pipelineID, signalType, processorName))
		settings := processor.Settings{
			ID:                processorID,
			TelemetrySettings: component.TelemetrySettings{},
			BuildInfo:         component.BuildInfo{},
		}

		// Create processor for the specific signal type
		var proc component.Component
		switch signalType {
		case "logs":
			if factory.LogsStability() != component.StabilityLevelUndefined {
				logsConsumer, ok := currentConsumer.(consumer.Logs)
				if !ok {
					return nil, nil, fmt.Errorf("invalid logs consumer for processor %q", processorName)
				}
				proc, err = factory.CreateLogs(ctx, settings, mergedCfg, logsConsumer)
				if err == nil {
					currentConsumer = proc.(consumer.Logs)
				}
			} else {
				m.logger.Debug("Processor does not support logs", zap.String("processor", processorName))
				continue
			}
		case "metrics":
			if factory.MetricsStability() != component.StabilityLevelUndefined {
				metricsConsumer, ok := currentConsumer.(consumer.Metrics)
				if !ok {
					return nil, nil, fmt.Errorf("invalid metrics consumer for processor %q", processorName)
				}
				proc, err = factory.CreateMetrics(ctx, settings, mergedCfg, metricsConsumer)
				if err == nil {
					currentConsumer = proc.(consumer.Metrics)
				}
			} else {
				m.logger.Debug("Processor does not support metrics", zap.String("processor", processorName))
				continue
			}
		case "traces":
			if factory.TracesStability() != component.StabilityLevelUndefined {
				tracesConsumer, ok := currentConsumer.(consumer.Traces)
				if !ok {
					return nil, nil, fmt.Errorf("invalid traces consumer for processor %q", processorName)
				}
				proc, err = factory.CreateTraces(ctx, settings, mergedCfg, tracesConsumer)
				if err == nil {
					currentConsumer = proc.(consumer.Traces)
				}
			} else {
				m.logger.Debug("Processor does not support traces", zap.String("processor", processorName))
				continue
			}
		default:
			return nil, nil, fmt.Errorf("unsupported signal type: %s", signalType)
		}

		if err != nil {
			return nil, nil, fmt.Errorf("failed to create %s processor %q: %w", signalType, processorName, err)
		}

		if proc != nil {
			processors = append(processors, proc)
		}
	}

	// Reverse processors back to normal order for storage
	slices.Reverse(processors)

	return processors, currentConsumer, nil
}

// createSignalReceivers creates receivers for a specific signal type.
func (m *Manager) createSignalReceivers(
	ctx context.Context,
	signalType string,
	receiverNames []string,
	config elasticsearch.PipelineConfig,
	nextConsumer interface{}, // consumer.Logs, consumer.Metrics, or consumer.Traces
	pipelineID string,
) ([]component.Component, error) {
	var receivers []component.Component

	for _, receiverName := range receiverNames {
		receiverConfigInterface, exists := config.Receivers[receiverName]
		if !exists {
			return nil, fmt.Errorf("receiver %q not found in configuration", receiverName)
		}

		receiverConfig, ok := receiverConfigInterface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid receiver config format for %q", receiverName)
		}

		receiverType := component.MustNewType(receiverName)
		factory, ok := m.host.GetFactory(component.KindReceiver, receiverType).(receiver.Factory)
		if !ok {
			return nil, fmt.Errorf("receiver factory not found for type %q", receiverName)
		}

		// Create default config and merge with provided config
		defaultCfg := factory.CreateDefaultConfig()
		mergedCfg, err := m.mergeComponentConfig(defaultCfg, receiverConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to merge receiver config for %q: %w", receiverName, err)
		}

		receiverID := component.NewIDWithName(factory.Type(), fmt.Sprintf("%s-%s-%s", pipelineID, signalType, receiverName))
		settings := receiver.Settings{
			ID:                receiverID,
			TelemetrySettings: component.TelemetrySettings{},
			BuildInfo:         component.BuildInfo{},
		}

		// Create receiver for the specific signal type
		var recv component.Component
		switch signalType {
		case "logs":
			if factory.LogsStability() != component.StabilityLevelUndefined {
				logsConsumer, ok := nextConsumer.(consumer.Logs)
				if !ok {
					return nil, fmt.Errorf("invalid logs consumer for receiver %q", receiverName)
				}
				recv, err = factory.CreateLogs(ctx, settings, mergedCfg, logsConsumer)
			} else {
				m.logger.Debug("Receiver does not support logs", zap.String("receiver", receiverName))
				continue
			}
		case "metrics":
			if factory.MetricsStability() != component.StabilityLevelUndefined {
				metricsConsumer, ok := nextConsumer.(consumer.Metrics)
				if !ok {
					return nil, fmt.Errorf("invalid metrics consumer for receiver %q", receiverName)
				}
				recv, err = factory.CreateMetrics(ctx, settings, mergedCfg, metricsConsumer)
			} else {
				m.logger.Debug("Receiver does not support metrics", zap.String("receiver", receiverName))
				continue
			}
		case "traces":
			if factory.TracesStability() != component.StabilityLevelUndefined {
				tracesConsumer, ok := nextConsumer.(consumer.Traces)
				if !ok {
					return nil, fmt.Errorf("invalid traces consumer for receiver %q", receiverName)
				}
				recv, err = factory.CreateTraces(ctx, settings, mergedCfg, tracesConsumer)
			} else {
				m.logger.Debug("Receiver does not support traces", zap.String("receiver", receiverName))
				continue
			}
		default:
			return nil, fmt.Errorf("unsupported signal type: %s", signalType)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to create %s receiver %q: %w", signalType, receiverName, err)
		}

		if recv != nil {
			receivers = append(receivers, recv)
		}
	}

	return receivers, nil
}
