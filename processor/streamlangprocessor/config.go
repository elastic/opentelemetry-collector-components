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
	"errors"
	"fmt"
)

// FailureMode controls how doc-level errors surface from a pipeline run.
type FailureMode string

const (
	// FailureModeDrop drops the offending document and continues processing the
	// rest of the batch. Mirrors Elasticsearch ingest pipeline semantics.
	FailureModeDrop FailureMode = "drop"

	// FailureModePropagate surfaces the first per-document error to the
	// downstream consumer. Useful for tests; not recommended for production.
	FailureModePropagate FailureMode = "propagate"
)

// Config is the configuration for the streamlang processor.
type Config struct {
	// Steps is the inline Streamlang DSL pipeline (mutually exclusive with Path).
	Steps []map[string]any `mapstructure:"steps"`

	// Path is the filesystem path to a YAML or JSON file containing a Streamlang
	// DSL document (mutually exclusive with Steps). Loaded once at Start.
	Path string `mapstructure:"path"`

	// FailureMode controls per-document error handling. Default: "drop".
	FailureMode FailureMode `mapstructure:"failure_mode"`

	// Logs configures log-record processing.
	Logs SignalConfig `mapstructure:"logs"`

	// Metrics configures metric data-point processing.
	Metrics SignalConfig `mapstructure:"metrics"`

	// Traces configures span processing.
	Traces SignalConfig `mapstructure:"traces"`
}

// SignalConfig holds per-signal toggles.
type SignalConfig struct {
	// Enabled controls whether this signal is processed. Default: true.
	Enabled *bool `mapstructure:"enabled"`
}

// IsEnabled reports whether the signal is enabled, defaulting to true.
func (s SignalConfig) IsEnabled() bool {
	if s.Enabled == nil {
		return true
	}
	return *s.Enabled
}

// Validate checks the processor configuration.
func (c *Config) Validate() error {
	if len(c.Steps) > 0 && c.Path != "" {
		return errors.New("only one of `steps` or `path` may be set")
	}
	if len(c.Steps) == 0 && c.Path == "" {
		return errors.New("`steps` or `path` is required")
	}
	switch c.FailureMode {
	case "", FailureModeDrop, FailureModePropagate:
	default:
		return fmt.Errorf("invalid failure_mode %q (expected %q or %q)",
			c.FailureMode, FailureModeDrop, FailureModePropagate)
	}
	return nil
}
