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

package elasticpipelineextension

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"

	"github.com/elastic/opentelemetry-collector-components/extension/elasticpipelineextension/internal/metadata"
)

const (
	defaultPollInterval             = 30 * time.Second
	defaultCacheDuration            = 5 * time.Minute
	defaultNamespace                = "elastic"
	defaultHealthInterval           = 60 * time.Second
	defaultIndex                    = ".otel-pipeline-config"
	defaultStartupTimeout           = 60 * time.Second
	defaultShutdownTimeout          = 30 * time.Second
	defaultMaxPipelines             = 50
	defaultMaxComponentsPerPipeline = 20
)

// NewFactory creates a factory for the elasticpipeline extension.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		metadata.Type,
		createDefaultConfig,
		createExtension,
		metadata.ExtensionStability,
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Source: SourceConfig{
			Elasticsearch: &ElasticsearchConfig{
				Index: defaultIndex,
			},
		},
		Watcher: WatcherConfig{
			PollInterval:  defaultPollInterval,
			CacheDuration: defaultCacheDuration,
		},
		PipelineManagement: PipelineManagementConfig{
			Namespace:                defaultNamespace,
			EnableHealthReporting:    true,
			HealthReportInterval:     defaultHealthInterval,
			StartupTimeout:           defaultStartupTimeout,
			ShutdownTimeout:          defaultShutdownTimeout,
			MaxPipelines:             defaultMaxPipelines,
			MaxComponentsPerPipeline: defaultMaxComponentsPerPipeline,
			ValidateConfigs:          true,
			DryRunMode:               false,
		},
	}
}

func createExtension(ctx context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	config := cfg.(*Config)
	return newElasticPipelineExtension(config, set), nil
}
