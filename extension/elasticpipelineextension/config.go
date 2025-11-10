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
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

// Config represents the configuration for the elasticpipeline extension.
type Config struct {
	// Source defines the remote configuration source settings.
	Source SourceConfig `mapstructure:"source"`

	// Watcher configures how the extension monitors for configuration changes.
	Watcher WatcherConfig `mapstructure:"watcher"`

	// PipelineManagement configures pipeline lifecycle management.
	PipelineManagement PipelineManagementConfig `mapstructure:"pipeline_management"`
}

// SourceConfig defines the configuration for remote configuration sources.
type SourceConfig struct {
	// Elasticsearch configures fetching from an Elasticsearch cluster.
	Elasticsearch *ElasticsearchConfig `mapstructure:"elasticsearch"`
}

// ElasticsearchConfig configures the Elasticsearch source for pipeline configurations.
type ElasticsearchConfig struct {
	// Endpoint is the Elasticsearch endpoint to connect to.
	Endpoint string `mapstructure:"endpoint"`

	// Username for basic authentication
	Username string `mapstructure:"username"`

	// Password for basic authentication
	Password string `mapstructure:"password"`

	// Index is the Elasticsearch index containing pipeline configurations.
	Index string `mapstructure:"index"`

	// CacheDuration specifies how long fetched configurations should be cached.
	CacheDuration time.Duration `mapstructure:"cache_duration"`
}

// WatcherConfig configures the configuration change monitoring.
type WatcherConfig struct {
	// PollInterval defines how often to check for configuration changes.
	PollInterval time.Duration `mapstructure:"poll_interval"`

	// CacheDuration defines how long to cache fetched configurations.
	CacheDuration time.Duration `mapstructure:"cache_duration"`

	// Filters define query filters for selective pipeline fetching.
	Filters []FilterConfig `mapstructure:"filters"`
}

// FilterConfig defines a filter for querying pipeline configurations.
type FilterConfig struct {
	// Field is the document field to filter on.
	Field string `mapstructure:"field"`

	// Value is the value to match against.
	Value string `mapstructure:"value"`
}

// PipelineManagementConfig configures pipeline lifecycle management.
type PipelineManagementConfig struct {
	// Namespace for generated pipeline IDs.
	Namespace string `mapstructure:"namespace"`

	// EnableHealthReporting enables reporting pipeline health to Elasticsearch.
	EnableHealthReporting bool `mapstructure:"enable_health_reporting"`

	// HealthReportInterval defines how often to report health status.
	HealthReportInterval time.Duration `mapstructure:"health_report_interval"`

	// StartupTimeout defines timeout for pipeline startup.
	StartupTimeout time.Duration `mapstructure:"startup_timeout"`

	// ShutdownTimeout defines timeout for pipeline shutdown.
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`

	// MaxPipelines defines the maximum number of dynamic pipelines.
	MaxPipelines int `mapstructure:"max_pipelines"`

	// MaxComponentsPerPipeline defines the maximum components per pipeline.
	MaxComponentsPerPipeline int `mapstructure:"max_components_per_pipeline"`

	// ValidateConfigs enables configuration validation before applying.
	ValidateConfigs bool `mapstructure:"validate_configs"`

	// DryRunMode enables validation without applying configurations.
	DryRunMode bool `mapstructure:"dry_run_mode"`
}

var (
	_ component.Config    = (*Config)(nil)
	_ confmap.Unmarshaler = (*Config)(nil)
)

// Unmarshal implements confmap.Unmarshaler.
func (cfg *Config) Unmarshal(conf *confmap.Conf) error {
	if err := conf.Unmarshal(cfg); err != nil {
		return err
	}
	return cfg.Validate()
}

// Validate checks if the configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.Source.Elasticsearch == nil {
		return errors.New("elasticsearch source configuration is required")
	}

	if cfg.Source.Elasticsearch.Endpoint == "" {
		return errors.New("elasticsearch endpoint must be specified")
	}

	if cfg.Source.Elasticsearch.Index == "" {
		return errors.New("elasticsearch index must be specified")
	}

	if cfg.Watcher.PollInterval <= 0 {
		return errors.New("poll_interval must be positive")
	}

	if cfg.Watcher.CacheDuration <= 0 {
		return errors.New("cache_duration must be positive")
	}

	if cfg.PipelineManagement.MaxPipelines <= 0 {
		return errors.New("max_pipelines must be positive")
	}

	if cfg.PipelineManagement.MaxComponentsPerPipeline <= 0 {
		return errors.New("max_components_per_pipeline must be positive")
	}

	if cfg.PipelineManagement.StartupTimeout <= 0 {
		return errors.New("startup_timeout must be positive")
	}

	if cfg.PipelineManagement.ShutdownTimeout <= 0 {
		return errors.New("shutdown_timeout must be positive")
	}

	if cfg.PipelineManagement.EnableHealthReporting && cfg.PipelineManagement.HealthReportInterval <= 0 {
		return errors.New("health_report_interval must be positive when health reporting is enabled")
	}

	return nil
}
