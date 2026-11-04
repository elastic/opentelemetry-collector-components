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

package dynamicbatchprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/dynamicbatchprocessor"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"

	"github.com/elastic/opentelemetry-collector-components/processor/dynamicbatchprocessor/internal/metadata"
)

// NewFactory returns a new factory for the dynamic batch processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
		processor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
		processor.WithLogs(createLogsProcessor, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		QueueBatchConfig:         exporterhelper.NewDefaultQueueConfig(),
		MetadataCardinalityLimit: 1000,
		IdleTimeout:              5 * time.Minute,
	}
}

// exporterSettings bridges processor.Settings to exporter.Settings.
func exporterSettings(set processor.Settings) exporter.Settings {
	return exporter.Settings{
		ID:                set.ID,
		TelemetrySettings: set.TelemetrySettings,
		BuildInfo:         set.BuildInfo,
	}
}

// queueOpts returns the exporterhelper options for a shard.
func queueOpts(cfg *Config) []exporterhelper.Option {
	return []exporterhelper.Option{
		exporterhelper.WithQueue(configoptional.Some(cfg.QueueBatchConfig)),
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: 0}),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
	}
}

func createTracesProcessor(_ context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	c := cfg.(*Config)
	expSet := exporterSettings(set)
	opts := queueOpts(c)
	dp := newDynamicProcessor[ptrace.Traces](c, func(_ component.Host) (*shard[ptrace.Traces], error) {
		exp, err := exporterhelper.NewTraces(context.Background(), expSet, c, next.ConsumeTraces, opts...)
		if err != nil {
			return nil, err
		}
		return newShard(exp, exp.ConsumeTraces), nil
	})
	return &tracesProcessor{dp}, nil
}

func createMetricsProcessor(_ context.Context, set processor.Settings, cfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
	c := cfg.(*Config)
	expSet := exporterSettings(set)
	opts := queueOpts(c)
	dp := newDynamicProcessor[pmetric.Metrics](c, func(_ component.Host) (*shard[pmetric.Metrics], error) {
		exp, err := exporterhelper.NewMetrics(context.Background(), expSet, c, next.ConsumeMetrics, opts...)
		if err != nil {
			return nil, err
		}
		return newShard(exp, exp.ConsumeMetrics), nil
	})
	return &metricsProcessor{dp}, nil
}

func createLogsProcessor(_ context.Context, set processor.Settings, cfg component.Config, next consumer.Logs) (processor.Logs, error) {
	c := cfg.(*Config)
	expSet := exporterSettings(set)
	opts := queueOpts(c)
	dp := newDynamicProcessor[plog.Logs](c, func(_ component.Host) (*shard[plog.Logs], error) {
		exp, err := exporterhelper.NewLogs(context.Background(), expSet, c, next.ConsumeLogs, opts...)
		if err != nil {
			return nil, err
		}
		return newShard(exp, exp.ConsumeLogs), nil
	})
	return &logsProcessor{dp}, nil
}
