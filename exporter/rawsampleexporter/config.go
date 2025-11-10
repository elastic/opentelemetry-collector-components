// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package rawsampleexporter

import (
	"time"

	"go.opentelemetry.io/collector/config/configretry"
)

// Config defines configuration for the raw sample exporter.
type Config struct {
	// Endpoint is the Elasticsearch base URL (e.g., "http://localhost:9200")
	Endpoint string `mapstructure:"endpoint"`

	// Username for Basic Auth
	Username string `mapstructure:"username"`

	// Password for Basic Auth
	Password string `mapstructure:"password"`

	// Index is the Elasticsearch index name for the sample API
	// (e.g., "logs-raw-samples")
	Index string `mapstructure:"index"`

	// MaxBatchSize is the maximum number of documents to send in a single request
	MaxBatchSize int `mapstructure:"max_batch_size"`

	// Timeout for HTTP requests
	Timeout time.Duration `mapstructure:"timeout"`

	// RetrySettings configures retry behavior
	RetrySettings configretry.BackOffConfig `mapstructure:"retry_on_failure"`
}

// Validate validates the configuration.
func (cfg *Config) Validate() error {
	if cfg.Endpoint == "" {
		return ErrMissingEndpoint
	}
	if cfg.Index == "" {
		return ErrMissingIndex
	}
	return nil
}
