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

package streamlangprocessor

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
)

var _ component.Config = (*Config)(nil)

// Config holds configuration for the streamlang processor.
type Config struct {
	// TransportMode treats the log body map as the document instead of attributes.
	TransportMode bool `mapstructure:"transport_mode"`

	// Pipeline is the streamlang DSL definition.
	Pipeline DSL `mapstructure:"pipeline"`
}

func (c *Config) Validate() error {
	if len(c.Pipeline.Steps) == 0 {
		return fmt.Errorf("pipeline must have at least one step")
	}
	return nil
}
