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

package elasticdynamictransform

import (
	"errors"
	"time"
)

// FallbackMode defines the behavior when processor configuration is unavailable
type FallbackMode string

const (
	// FallbackModePassthrough forwards data unchanged when config is unavailable
	FallbackModePassthrough FallbackMode = "passthrough"

	// FallbackModeError returns an error when config is unavailable
	FallbackModeError FallbackMode = "error"

	// FallbackModeDrop silently drops data when config is unavailable
	FallbackModeDrop FallbackMode = "drop"
)

// Config defines configuration for the dynamic transform processor
type Config struct {
	// ExtensionID is the component ID of the elasticpipeline extension
	// Example: "elasticpipeline" or "elasticpipeline/custom"
	ExtensionID string `mapstructure:"extension"`

	// ProcessorKey is the key to look up in the extension's processor configs
	// This should match the key in the Elasticsearch document
	// Example: "transform/stream_processing"
	ProcessorKey string `mapstructure:"processor_key"`

	// ReloadInterval is how often to check for config updates
	// Default: 30s
	ReloadInterval time.Duration `mapstructure:"reload_interval"`

	// FallbackMode defines behavior when config is unavailable
	// Options: "passthrough" (forward unchanged), "error" (return error), "drop" (drop data)
	// Default: "passthrough"
	FallbackMode FallbackMode `mapstructure:"fallback_mode"`

	// InitialWaitTimeout is how long to wait for initial config load
	// If exceeded and no config available, uses fallback mode
	// Default: 5s
	InitialWaitTimeout time.Duration `mapstructure:"initial_wait_timeout"`
}

// Validate checks if the configuration is valid
func (cfg *Config) Validate() error {
	if cfg.ExtensionID == "" {
		return errors.New("extension must be specified")
	}
	if cfg.ProcessorKey == "" {
		return errors.New("processor_key must be specified")
	}
	if cfg.FallbackMode != "" &&
		cfg.FallbackMode != FallbackModePassthrough &&
		cfg.FallbackMode != FallbackModeError &&
		cfg.FallbackMode != FallbackModeDrop {
		return errors.New("fallback_mode must be one of: passthrough, error, drop")
	}
	if cfg.ReloadInterval < 0 {
		return errors.New("reload_interval must be positive")
	}
	if cfg.InitialWaitTimeout < 0 {
		return errors.New("initial_wait_timeout must be positive")
	}
	return nil
}
