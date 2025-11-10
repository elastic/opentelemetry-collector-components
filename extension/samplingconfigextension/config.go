// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package samplingconfigextension

import (
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config defines the configuration for the sampling config extension.
type Config struct {
	// Endpoint is the Elasticsearch endpoint to connect to.
	Endpoint string `mapstructure:"endpoint"`

	// Username for basic authentication
	Username string `mapstructure:"username"`

	// Password for basic authentication
	Password string `mapstructure:"password"`

	// ConfigIndex is the Elasticsearch index containing sampling configuration.
	// Default: .elastic-sampling-config
	ConfigIndex string `mapstructure:"config_index"`

	// PollInterval is how often to poll Elasticsearch for config updates.
	// Default: 30s
	PollInterval time.Duration `mapstructure:"poll_interval"`

	// DefaultSampleRate is the fallback sample rate when no rules match.
	// Default: 0.0 (no sampling)
	DefaultSampleRate float64 `mapstructure:"default_sample_rate"`
}

var _ component.Config = (*Config)(nil)

// Validate validates the configuration.
func (cfg *Config) Validate() error {
	if cfg.Endpoint == "" {
		return errMissingEndpoint
	}
	if cfg.PollInterval <= 0 {
		return errInvalidPollInterval
	}
	if cfg.DefaultSampleRate < 0 || cfg.DefaultSampleRate > 1 {
		return errInvalidSampleRate
	}
	return nil
}
