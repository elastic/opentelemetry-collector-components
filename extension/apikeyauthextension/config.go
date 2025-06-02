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

package apikeyauthextension // import "github.com/elastic/opentelemetry-collector-components/extension/apikeyauthextension"

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
)

type Config struct {
	confighttp.ClientConfig `mapstructure:",squash"`

	// ApplicationPrivileges defines the application privileges
	// that are queried when verifying API Keys.
	ApplicationPrivileges []ApplicationPrivilegesConfig `mapstructure:"application_privileges,omitempty"`

	// Cache holds configuration related to caching
	// API Key verification results.
	Cache CacheConfig `mapstructure:"cache"`
}

type ApplicationPrivilegesConfig struct {
	// Application holds the name of the application for which
	// privileges are defined, e.g. "apm".
	Application string `mapstructure:"application"`

	// Privileges holds the list of application-specific privileges
	// that the API Key must have to be considered valid.
	Privileges []string `mapstructure:"privileges"`

	// Resources holds the list of application-specific resources
	// that the API Key must have access to to be considered valid.
	Resources []string `mapstructure:"resources"`
}

type CacheConfig struct {
	// KeyHeaders holds an optional set of headers to include in the
	// cache key, for partitioning the API Key space. If any headers
	// are missing from the request, an error will be returned.
	KeyHeaders []string `mapstructure:"key_headers,omitempty"`

	// PBKDF2Iterations defines the iteration count for PBKDF2
	// for key derivation in cached API Keys.
	PBKDF2Iterations int `mapstructure:"pbkdf2_iterations,omitempty"`

	// Capacity defines the maximum number of API Key verification
	// results to cache. Once this is reached, the least recently
	// used entries will be evicted.
	Capacity uint32 `mapstructure:"capacity"`

	// TTL defines the duration before the cache key gets evicted
	TTL time.Duration `mapstructure:"ttl"`
}

func createDefaultConfig() component.Config {
	return &Config{
		ClientConfig: confighttp.NewDefaultClientConfig(),
		Cache: CacheConfig{
			Capacity:         1000,
			PBKDF2Iterations: 1000,
			TTL:              30 * time.Second,
		},
	}
}

func (cfg *CacheConfig) Validate() error {
	if cfg.Capacity <= 0 {
		return fmt.Errorf("invalid capacity: %d, must be greater than 0", cfg.Capacity)
	}
	if cfg.PBKDF2Iterations <= 0 {
		return fmt.Errorf("invalid pbkdf2_iterations: %d, must be greater than 0", cfg.PBKDF2Iterations)
	}
	if cfg.TTL <= 0 {
		return fmt.Errorf("invalid ttl: %s, must be greater than 0", cfg.TTL)
	}
	return nil
}
