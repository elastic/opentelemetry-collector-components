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

package akamaisiemreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver"

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
)

const (
	defaultPollInterval     = time.Minute
	defaultInitialLookback  = 12 * time.Hour
	maxInitialLookback      = 12 * time.Hour
	defaultEventLimit       = 10000
	maxEventLimit           = 600000
	defaultOffsetTTL        = 120 * time.Second
	defaultMaxRecovery      = 3
	defaultInvalidTSRetry   = 2
	defaultBatchSize        = 1000
	defaultStreamBufferSize = 4
)

// Config defines the configuration for the Akamai SIEM receiver.
type Config struct {
	// Endpoint is the Akamai API host (e.g. "https://akab-xxx.luna.akamaiapis.net").
	Endpoint string `mapstructure:"endpoint"`

	// ConfigIDs is a semicolon or comma-separated list of security configuration IDs.
	ConfigIDs string `mapstructure:"config_ids"`

	// Authentication holds Akamai EdgeGrid HMAC-SHA256 credentials.
	Authentication EdgeGridAuth `mapstructure:"authentication"`

	// PollInterval is the time between polling cycles.
	PollInterval time.Duration `mapstructure:"poll_interval"`

	// InitialLookback is the lookback window for the first poll. Max 12h (Akamai limit).
	InitialLookback time.Duration `mapstructure:"initial_lookback"`

	// EventLimit is the max events per API request. Max 600000.
	EventLimit int `mapstructure:"event_limit"`

	// OffsetTTL is the max age of a stored offset before proactive replay. Zero disables.
	OffsetTTL time.Duration `mapstructure:"offset_ttl"`

	// MaxRecoveryAttempts caps consecutive recovery actions per poll cycle. Zero disables.
	MaxRecoveryAttempts int `mapstructure:"max_recovery_attempts"`

	// InvalidTimestampRetries is the number of immediate retries for 400 "invalid timestamp".
	InvalidTimestampRetries int `mapstructure:"invalid_timestamp_retries"`

	// BatchSize is the number of events per ConsumeLogs call. Default 1000.
	BatchSize int `mapstructure:"batch_size"`

	// StreamBufferSize is the bounded channel capacity between the NDJSON scanner
	// and the batch consumer. Controls back-pressure. Default 4.
	StreamBufferSize int `mapstructure:"stream_buffer_size"`

	// OutputFormat controls the structure of emitted log records.
	// "raw" (default): raw Akamai JSON placed in LogRecord.Body as a map with key
	// "message". Use a transform processor to set the elastic.mapping.mode scope
	// attribute for Elasticsearch backends (see README for details).
	// "otel": receiver parses each JSON event and maps 30+ fields to OTel semantic
	// convention attributes on the LogRecord. Use for non-ES backends or structured querying.
	OutputFormat string `mapstructure:"output_format"`

	// HTTP provides TLS, proxy, timeout, and other HTTP client settings.
	HTTP confighttp.ClientConfig `mapstructure:"http"`

	// StorageID references a storage extension for persisting cursor state across
	// restarts. If nil, cursor persistence is disabled and the receiver starts fresh.
	// Use with the file_storage extension for file-based persistence.
	StorageID *component.ID `mapstructure:"storage"`
}

// EdgeGridAuth holds Akamai EdgeGrid HMAC-SHA256 credentials.
type EdgeGridAuth struct {
	ClientToken  configopaque.String `mapstructure:"client_token"`
	ClientSecret configopaque.String `mapstructure:"client_secret"`
	AccessToken  configopaque.String `mapstructure:"access_token"`
}

func createDefaultConfig() component.Config {
	return &Config{
		PollInterval:            defaultPollInterval,
		InitialLookback:         defaultInitialLookback,
		EventLimit:              defaultEventLimit,
		OffsetTTL:               defaultOffsetTTL,
		MaxRecoveryAttempts:     defaultMaxRecovery,
		InvalidTimestampRetries: defaultInvalidTSRetry,
		BatchSize:               defaultBatchSize,
		StreamBufferSize:        defaultStreamBufferSize,
		OutputFormat:            "raw",
		HTTP: confighttp.ClientConfig{
			Timeout: 60 * time.Second,
		},
	}
}

// Validate checks the configuration for correctness.
func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	if c.ConfigIDs == "" {
		return errors.New("config_ids is required")
	}
	if string(c.Authentication.ClientToken) == "" {
		return errors.New("auth.client_token is required")
	}
	if string(c.Authentication.ClientSecret) == "" {
		return errors.New("auth.client_secret is required")
	}
	if string(c.Authentication.AccessToken) == "" {
		return errors.New("auth.access_token is required")
	}
	if c.PollInterval <= 0 {
		return errors.New("poll_interval must be greater than 0")
	}
	if c.InitialLookback <= 0 {
		return errors.New("initial_lookback must be greater than 0")
	}
	if c.InitialLookback > maxInitialLookback {
		return fmt.Errorf("initial_lookback cannot exceed %v (Akamai API limit)", maxInitialLookback)
	}
	if c.EventLimit <= 0 {
		return errors.New("event_limit must be greater than 0")
	}
	if c.EventLimit > maxEventLimit {
		return fmt.Errorf("event_limit cannot exceed %d", maxEventLimit)
	}
	if c.OffsetTTL < 0 {
		return errors.New("offset_ttl must be non-negative")
	}
	if c.MaxRecoveryAttempts < 0 {
		return errors.New("max_recovery_attempts must be non-negative")
	}
	if c.InvalidTimestampRetries < 0 {
		return errors.New("invalid_timestamp_retries must be non-negative")
	}
	if c.BatchSize <= 0 {
		return errors.New("batch_size must be greater than 0")
	}
	if c.StreamBufferSize <= 0 {
		return errors.New("stream_buffer_size must be greater than 0")
	}
	if c.OutputFormat != "raw" && c.OutputFormat != "otel" {
		return fmt.Errorf("output_format must be \"raw\" or \"otel\", got %q", c.OutputFormat)
	}
	return nil
}
