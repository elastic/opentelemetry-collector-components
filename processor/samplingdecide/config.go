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

package samplingdecide

import (
	"errors"

	"go.opentelemetry.io/collector/component"
)

// Config represents the configuration for the samplingdecide processor.
type Config struct {
	// Condition is an OTTL condition that determines whether a log should be sampled.
	Condition string `mapstructure:"condition"`

	// SampleRate is the probability of sampling when condition matches (0.0-1.0).
	SampleRate float64 `mapstructure:"sample_rate"`

	// InvertMatch inverts the condition logic.
	InvertMatch bool `mapstructure:"invert_match"`

	// ExtensionName is the name of the samplingconfigextension to use for dynamic configuration.
	// If set, the processor will query the extension for sampling rules that override the
	// static configuration above.
	ExtensionName string `mapstructure:"extension_name"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.Condition == "" {
		return errors.New("condition must not be empty")
	}
	if cfg.SampleRate < 0.0 || cfg.SampleRate > 1.0 {
		return errors.New("sample_rate must be between 0.0 and 1.0")
	}
	return nil
}
