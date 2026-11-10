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

package compressedotlpencodingextension // import "github.com/elastic/opentelemetry-collector-components/extension/compressedotlpencodingextension"

import (
	"fmt"

	"go.opentelemetry.io/collector/confmap"
)

var _ confmap.Validator = (*Config)(nil)

type Config struct {
	EncodingEnabled bool   `mapstructure:"encoding_enabled"`
	Protocol        string `mapstructure:"protocol"`
	Compression     string `mapstructure:"compression"`
	// prevent unkeyed literal initialization
	_ struct{}
}

func isValidProtocol(protocol string) bool {
	for p := protocolMin; p < protocolMax; p++ {
		if protocol == p.String() {
			return true
		}
	}
	return false
}

func isValidCompression(compression string) bool {
	for c := compressionMin; c < compressionMax; c++ {
		if compression == c.String() {
			return true
		}
	}
	return false
}

func (c *Config) Validate() error {
	if !isValidProtocol(c.Protocol) {
		return fmt.Errorf("unsupported protocol: %q", c.Protocol)
	}

	if !isValidCompression(c.Compression) {
		return fmt.Errorf("unsupported compression: %q", c.Compression)
	}

	return nil
}
