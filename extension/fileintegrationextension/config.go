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

package fileintegrationextension // import "github.com/elastic/opentelemetry-collector-components/extension/fileintegrationextension"

import (
	"errors"
	"fmt"
	"os"

	"go.opentelemetry.io/collector/confmap/xconfmap"
)

// Config is the structured configuration of the extension.
type Config struct {
	// Path is the directory containing integrations.
	Path string `mapstructure:"path"`
}

var _ xconfmap.Validator = &Config{}

// Validate validates the configuration, it checks that the path is defined and is the path to a directory.
func (c *Config) Validate() error {
	if c.Path == "" {
		return errors.New("path is required")
	}
	info, err := os.Stat(c.Path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("path (%s) must be a directory", c.Path)
	}

	return nil
}
