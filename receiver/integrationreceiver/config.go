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

package integrationreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/integrationreceiver"

import (
	"errors"

	"go.opentelemetry.io/collector/pipeline"
)

// Config is the structured configuration of the receiver.
type Config struct {
	// Name of the integration to instantiate, resolved by the finder extensions.
	Name string `mapstructure:"name"`

	// Pipelines is the list of pipelines to instantiate, from the referenced integration.
	Pipelines []pipeline.ID `mapstructure:"pipelines"`

	// Parameters are used to resolve the placeholders in parameterized integrations.
	Parameters map[string]any `mapstructure:"parameters"`
}

// Validate validates the configuration.
func (cfg *Config) Validate() error {
	if cfg.Name == "" {
		return errors.New("name is required")
	}

	return nil
}
