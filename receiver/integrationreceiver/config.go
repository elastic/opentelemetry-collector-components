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

	"go.opentelemetry.io/collector/component"
)

type Config struct {
	Name       string         `mapstructure:"name"`
	Pipelines  []component.ID `mapstructure:"pipelines"`
	Version    string         `mapstructure:"version"`
	Parameters map[string]any `mapstructure:"parameters"`
}

func (cfg *Config) Validate() error {
	if cfg.Name == "" {
		return errors.New("name is required")
	}

	return nil
}
