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
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/metadata"
	"github.com/elastic/opentelemetry-lib/enrichments/config"
)

// NewFactory returns a processor.Factory that constructs elastic
// trace processor instances.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(createTraces, metadata.TracesStability),
		processor.WithLogs(createLogs, metadata.TracesStability),
		processor.WithMetrics(createMetrics, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Config:         config.Enabled(),
		SkipEnrichment: false,
		HostIPEnabled:  true,
	}
}

func createLogs(
	_ context.Context, set processor.Settings, cfg component.Config, next consumer.Logs,
) (processor.Logs, error) {
	processorCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsing error")
	}
	return newLogProcessor(processorCfg, next, set.Logger), nil
}

func createTraces(
	_ context.Context, set processor.Settings, cfg component.Config, next consumer.Traces,
) (processor.Traces, error) {
	processorCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsing error")
	}
	return NewTraceProcessor(processorCfg, next, set.Logger), nil
}

func createMetrics(
	_ context.Context, set processor.Settings, cfg component.Config, next consumer.Metrics,
) (processor.Metrics, error) {
	processorCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsing error")
	}
	return newMetricProcessor(processorCfg, next, set.Logger), nil
}
