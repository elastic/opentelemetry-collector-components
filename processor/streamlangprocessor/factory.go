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

package streamlangprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/metadata"
)

// NewFactory returns a processor.Factory for the streamlang processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithLogs(createLogs, metadata.LogsStability),
		processor.WithMetrics(createMetrics, metadata.MetricsStability),
		processor.WithTraces(createTraces, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		FailureMode: FailureModeDrop,
	}
}

func createLogs(
	_ context.Context, set processor.Settings, cfg component.Config, next consumer.Logs,
) (processor.Logs, error) {
	pCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsing error")
	}
	return newLogsProcessor(set, pCfg, next)
}

func createMetrics(
	_ context.Context, set processor.Settings, cfg component.Config, next consumer.Metrics,
) (processor.Metrics, error) {
	pCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsing error")
	}
	return newMetricsProcessor(set, pCfg, next)
}

func createTraces(
	_ context.Context, set processor.Settings, cfg component.Config, next consumer.Traces,
) (processor.Traces, error) {
	pCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsing error")
	}
	return newTracesProcessor(set, pCfg, next)
}
