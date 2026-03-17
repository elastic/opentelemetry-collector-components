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

package partitioningprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/partitioningprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/xprocessor"

	"github.com/elastic/opentelemetry-collector-components/processor/partitioningprocessor/internal/metadata"
)

// NewFactory returns a new xprocessor.Factory for the partitioning processor.
func NewFactory() xprocessor.Factory {
	return xprocessor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		xprocessor.WithLogs(createLogsProcessor, metadata.LogsStability),
		xprocessor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
		xprocessor.WithTraces(createTracesProcessor, metadata.TracesStability),
		xprocessor.WithProfiles(createProfilesProcessor, metadata.ProfilesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createLogsProcessor(_ context.Context, _ processor.Settings, cfg component.Config, next consumer.Logs) (processor.Logs, error) {
	return &partitioningProcessor{nextLogs: next}, nil
}

func createMetricsProcessor(_ context.Context, _ processor.Settings, cfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
	return &partitioningProcessor{nextMetrics: next}, nil
}

func createTracesProcessor(_ context.Context, _ processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	return &partitioningProcessor{nextTraces: next}, nil
}

func createProfilesProcessor(_ context.Context, _ processor.Settings, cfg component.Config, next xconsumer.Profiles) (xprocessor.Profiles, error) {
	return &partitioningProcessor{nextProfiles: next}, nil
}
