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

package configintegrationextension // import "github.com/elastic/opentelemetry-collector-components/extension/configintegrationextension"

import (
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/confmap/xconfmap"

	"github.com/elastic/opentelemetry-collector-components/internal/integrations"
)

// Config is the structured configuration of the extension.
type Config struct {
	// Integrations is a map with the embedded definition of integrations.
	Integrations map[string]string `mapstructure:"integrations"`
}

var _ xconfmap.Validator = &Config{}

// Validate checks that the embedded integrations are syntactically valid.
func (c *Config) Validate() error {
	for name, integration := range c.Integrations {
		raw := []byte(strings.ReplaceAll(integration, `$$`, `$`))
		if err := integrations.ValidateRawTemplate(raw); err != nil {
			return fmt.Errorf("embedded integration %q is not valid: %w", name, err)
		}
	}

	return nil
}
