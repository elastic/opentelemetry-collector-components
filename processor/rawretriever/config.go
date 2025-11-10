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

package rawretriever

import (
	"errors"

	"go.opentelemetry.io/collector/component"
)

// Config represents the configuration for the rawretriever processor.
type Config struct {
	// AttributeKey is the name of the attribute containing the UUID.
	AttributeKey string `mapstructure:"attribute_key"`

	// ExtensionName is the name of the rawsamplingbuffer extension to use.
	ExtensionName string `mapstructure:"extension_name"`

	// RemoveAttribute determines whether to remove the UUID attribute after retrieval.
	RemoveAttribute bool `mapstructure:"remove_attribute"`

	// OnRetrievalError determines what to do when retrieval fails.
	// Options: "drop", "keep_processed", "error"
	OnRetrievalError string `mapstructure:"on_retrieval_error"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.AttributeKey == "" {
		return errors.New("attribute_key must not be empty")
	}
	if cfg.ExtensionName == "" {
		return errors.New("extension_name must not be empty")
	}
	if cfg.OnRetrievalError != "drop" && cfg.OnRetrievalError != "keep_processed" && cfg.OnRetrievalError != "error" {
		return errors.New("on_retrieval_error must be 'drop', 'keep_processed', or 'error'")
	}
	return nil
}
