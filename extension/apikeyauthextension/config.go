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
	"strings"
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

	// DynamicResources holds the list of dynamic resources that are
	// extracted from client metadata at runtime and combined with
	// static Resources.
	DynamicResources []DynamicResource `mapstructure:"dynamic_resources,omitempty"`
}

// DynamicResource defines a resource that is extracted from client metadata
// at runtime, with optional formatting.
type DynamicResource struct {
	// Metadata holds the client metadata key to extract the resource value from.
	Metadata string `mapstructure:"metadata"`

	// Format holds an optional fmt.Sprintf-style format string for formatting
	// the metadata value. Must contain exactly one %s placeholder.
	// Defaults to "%s" if empty.
	Format string `mapstructure:"format"`
}

type CacheConfig struct {
	// KeyHeaders holds an optional set of headers to include in the
	// cache key, for partitioning the API Key space. If any headers
	// are missing from the request, an error will be returned.
	KeyHeaders []string `mapstructure:"key_headers,omitempty"`

	// KeyMetadata holds an optional set of client metadata keys to
	// include in the cache key, for partitioning the API Key space.
	// If any keys are missing from the client metadata, an error
	// will be returned.
	KeyMetadata []string `mapstructure:"key_metadata,omitempty"`

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

// Validate validates the DynamicResource configuration.
func (dr *DynamicResource) Validate() error {
	if dr.Metadata == "" {
		return fmt.Errorf("metadata must be non-empty")
	}
	format := dr.Format
	if format == "" {
		format = "%s"
	}
	// Count occurrences of %s
	count := strings.Count(format, "%s")
	// Check for other format verbs (like %d, %v, etc.)
	// We'll do a simple check: count all % that aren't followed by s or %
	otherVerbs := 0
	for i := 0; i < len(format)-1; i++ {
		if format[i] == '%' {
			next := format[i+1]
			if next != 's' && next != '%' {
				otherVerbs++
			}
		}
	}
	if count != 1 {
		return fmt.Errorf("format must contain exactly one %%s placeholder, found %d", count)
	}
	if otherVerbs > 0 {
		return fmt.Errorf("format may only contain %%s placeholder and %%%% (literal percent), found other format verbs")
	}
	return nil
}

// Validate validates the Config.
func (cfg *Config) Validate() error {
	// Build a set of metadata keys in cache.key_metadata for quick lookup
	keyMetadataSet := make(map[string]bool)
	for _, key := range cfg.Cache.KeyMetadata {
		keyMetadataSet[key] = true
	}
	// Validate each application privilege config
	for i, app := range cfg.ApplicationPrivileges {
		// Check that dynamic resource metadata keys are in cache.key_metadata
		for j, dr := range app.DynamicResources {
			if dr.Metadata != "" && !keyMetadataSet[dr.Metadata] {
				return fmt.Errorf(""+
					"application_privileges::%d::dynamic_resources::%d: "+
					"dynamic resource metadata %q must be included in cache.key_metadata",
					i, j, dr.Metadata,
				)
			}
		}
	}
	return nil
}
