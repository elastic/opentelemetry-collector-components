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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/extension/elasticpipelineextension/internal/elasticsearch"
)

// Fanout consumer implementations for handling multiple exporters

// fanoutLogsConsumer fans out logs to multiple consumers.
type fanoutLogsConsumer struct {
	consumers []consumer.Logs
}

func (f *fanoutLogsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (f *fanoutLogsConsumer) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	var errs []error
	for _, c := range f.consumers {
		if err := c.ConsumeLogs(ctx, logs); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to consume logs: %v", errs)
	}
	return nil
}

// fanoutMetricsConsumer fans out metrics to multiple consumers.
type fanoutMetricsConsumer struct {
	consumers []consumer.Metrics
}

func (f *fanoutMetricsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (f *fanoutMetricsConsumer) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	var errs []error
	for _, c := range f.consumers {
		if err := c.ConsumeMetrics(ctx, metrics); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to consume metrics: %v", errs)
	}
	return nil
}

// fanoutTracesConsumer fans out traces to multiple consumers.
type fanoutTracesConsumer struct {
	consumers []consumer.Traces
}

func (f *fanoutTracesConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (f *fanoutTracesConsumer) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	var errs []error
	for _, c := range f.consumers {
		if err := c.ConsumeTraces(ctx, traces); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to consume traces: %v", errs)
	}
	return nil
}

// Consumer chain building methods

// buildSignalConsumerChain creates a consumer chain for a specific signal type.
// It handles multiple exporters by creating a fanout consumer if needed.
func (m *Manager) buildSignalConsumerChain(signalType string, exporters []component.Component) (interface{}, error) {
	if len(exporters) == 0 {
		return nil, fmt.Errorf("no exporters provided for signal type %s", signalType)
	}

	switch signalType {
	case "logs":
		var logsConsumers []consumer.Logs
		for _, exp := range exporters {
			if logsConsumer, ok := exp.(consumer.Logs); ok {
				logsConsumers = append(logsConsumers, logsConsumer)
			}
		}
		if len(logsConsumers) == 0 {
			return nil, fmt.Errorf("no exporters support logs signal type")
		}
		if len(logsConsumers) == 1 {
			return logsConsumers[0], nil
		}
		return &fanoutLogsConsumer{consumers: logsConsumers}, nil

	case "metrics":
		var metricsConsumers []consumer.Metrics
		for _, exp := range exporters {
			if metricsConsumer, ok := exp.(consumer.Metrics); ok {
				metricsConsumers = append(metricsConsumers, metricsConsumer)
			}
		}
		if len(metricsConsumers) == 0 {
			return nil, fmt.Errorf("no exporters support metrics signal type")
		}
		if len(metricsConsumers) == 1 {
			return metricsConsumers[0], nil
		}
		return &fanoutMetricsConsumer{consumers: metricsConsumers}, nil

	case "traces":
		var tracesConsumers []consumer.Traces
		for _, exp := range exporters {
			if tracesConsumer, ok := exp.(consumer.Traces); ok {
				tracesConsumers = append(tracesConsumers, tracesConsumer)
			}
		}
		if len(tracesConsumers) == 0 {
			return nil, fmt.Errorf("no exporters support traces signal type")
		}
		if len(tracesConsumers) == 1 {
			return tracesConsumers[0], nil
		}
		return &fanoutTracesConsumer{consumers: tracesConsumers}, nil

	default:
		return nil, fmt.Errorf("unsupported signal type: %s", signalType)
	}
}

// buildSignalPipeline creates a complete signal pipeline for a specific signal type.
func (m *Manager) buildSignalPipeline(
	ctx context.Context,
	pipelineName string,
	signalType string,
	pipelineDef elasticsearch.PipelineDefinition,
	config elasticsearch.PipelineConfig,
	pipelineID string,
) (*SignalPipeline, error) {
	m.logger.Debug("Building signal pipeline",
		zap.String("pipeline_name", pipelineName),
		zap.String("signal_type", signalType))

	// 1. Create exporters first (they are the final consumers)
	exporters, err := m.createSignalExporters(ctx, signalType, pipelineDef.Exporters, config, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("failed to create exporters: %w", err)
	}

	// 2. Build consumer chain from exporters
	exporterConsumer, err := m.buildSignalConsumerChain(signalType, exporters)
	if err != nil {
		return nil, fmt.Errorf("failed to build exporter consumer chain: %w", err)
	}

	// 3. Create processor chain (connects to exporter consumer)
	processors, processorConsumer, err := m.createSignalProcessors(ctx, signalType, pipelineDef.Processors, config, exporterConsumer, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("failed to create processors: %w", err)
	}

	// 4. Use processor consumer or exporter consumer if no processors
	finalConsumer := processorConsumer
	if finalConsumer == nil {
		finalConsumer = exporterConsumer
	}

	// 5. Create receivers (connect to final consumer in chain)
	receivers, err := m.createSignalReceivers(ctx, signalType, pipelineDef.Receivers, config, finalConsumer, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("failed to create receivers: %w", err)
	}

	// 6. Create signal pipeline structure
	signalPipeline := &SignalPipeline{
		SignalType:   signalType,
		PipelineName: pipelineName,
		Receivers:    receivers,
		Processors:   processors,
		Exporters:    exporters,
	}

	// 7. Set the appropriate consumer based on signal type
	switch signalType {
	case "logs":
		if logsConsumer, ok := finalConsumer.(consumer.Logs); ok {
			signalPipeline.LogsConsumer = logsConsumer
		}
	case "metrics":
		if metricsConsumer, ok := finalConsumer.(consumer.Metrics); ok {
			signalPipeline.MetricsConsumer = metricsConsumer
		}
	case "traces":
		if tracesConsumer, ok := finalConsumer.(consumer.Traces); ok {
			signalPipeline.TracesConsumer = tracesConsumer
		}
	}

	return signalPipeline, nil
}
