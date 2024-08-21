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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

	"github.com/elastic/opentelemetry-collector-components/processor/templateprocessor/internal/metadata"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, metadata.LogsStability),
		processor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createLogsProcessor(_ context.Context, params processor.Settings, cfg component.Config, consumer consumer.Logs) (processor.Logs, error) {
	return newTemplateLogsProcessor(params, cfg.(*Config), consumer), nil
}

func createMetricsProcessor(_ context.Context, params processor.Settings, cfg component.Config, consumer consumer.Metrics) (processor.Metrics, error) {
	return newTemplateMetricsProcessor(params, cfg.(*Config), consumer), nil
}

func createTracesProcessor(_ context.Context, params processor.Settings, cfg component.Config, consumer consumer.Traces) (processor.Traces, error) {
	return newTemplateTracesProcessor(params, cfg.(*Config), consumer), nil
}
