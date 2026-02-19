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

package apikeyauthextension

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"

	"github.com/elastic/opentelemetry-collector-components/extension/apikeyauthextension/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		id                 component.ID
		expected           *Config
		expectedErrMessage string
	}{
		{
			id:       component.NewID(metadata.Type),
			expected: createDefaultConfig().(*Config),
		},
		{
			id: component.NewIDWithName(metadata.Type, "application_privileges"),
			expected: func() *Config {
				config := createDefaultConfig().(*Config)
				config.ApplicationPrivileges = []ApplicationPrivilegesConfig{{
					Application: "my_app",
					Privileges:  []string{"read", "write"},
					Resources:   []string{"*"},
				}}
				return config
			}(),
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "invalid_cache_capacity"),
			expectedErrMessage: "cache: invalid capacity: 0, must be greater than 0",
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "invalid_cache_ttl"),
			expectedErrMessage: "cache: invalid ttl: 0s, must be greater than 0",
		},
		{
			id: component.NewIDWithName(metadata.Type, "dynamic_resources"),
			expected: func() *Config {
				config := createDefaultConfig().(*Config)
				config.ApplicationPrivileges = []ApplicationPrivilegesConfig{{
					Application: "my_app",
					Privileges:  []string{"write"},
					Resources:   []string{"static-resource"},
					DynamicResources: []DynamicResource{{
						Metadata: "X-Resource-Name",
						Format:   "resource:%s",
					}, {
						Metadata: "X-Resource-Name-Default-Format",
						Format:   "%s",
					}},
				}}
				config.Cache.KeyMetadata = []string{
					"X-Resource-Name",
					"X-Resource-Name-Default-Format",
				}
				return config
			}(),
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "dynamic_resources_missing_metadata"),
			expectedErrMessage: `application_privileges::0::dynamic_resources::0: metadata must be non-empty`,
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "dynamic_resources_missing_cache_key_metadata"),
			expectedErrMessage: `application_privileges::0::dynamic_resources::0: dynamic resource metadata "X-Resource-Name" must be included in cache.key_metadata`,
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "dynamic_resources_invalid_format"),
			expectedErrMessage: `application_privileges::0::dynamic_resources::0: format must contain exactly one %s placeholder, found 0`,
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "dynamic_resources_multiple_format"),
			expectedErrMessage: `application_privileges::0::dynamic_resources::0: format must contain exactly one %s placeholder, found 2`,
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "dynamic_resources_empty_metadata"),
			expectedErrMessage: `application_privileges::0::dynamic_resources::0: metadata must be non-empty`,
		},
		{
			id: component.NewIDWithName(metadata.Type, "custom_retry"),
			expected: func() *Config {
				config := createDefaultConfig().(*Config)
				config.Retry = RetryConfig{
					Enabled:         true,
					MaxRetries:      5,
					InitialInterval: 200 * time.Millisecond,
					MaxInterval:     10 * time.Second,
				}
				return config
			}(),
		},
		{
			id: component.NewIDWithName(metadata.Type, "retry_disabled"),
			expected: func() *Config {
				config := createDefaultConfig().(*Config)
				config.Retry.Enabled = false
				return config
			}(),
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "invalid_retry_max_retries"),
			expectedErrMessage: `retry: invalid max_retries: -1, must be non-negative`,
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "invalid_retry_initial_interval"),
			expectedErrMessage: `retry: invalid initial_interval: 0s, must be greater than 0`,
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "invalid_retry_max_interval"),
			expectedErrMessage: `retry: invalid max_interval: 0s, must be greater than 0`,
		},
		{
			id:                 component.NewIDWithName(metadata.Type, "invalid_retry_interval_order"),
			expectedErrMessage: `retry: max_interval (1s) must be greater than or equal to initial_interval (10s)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
			assert.NoError(t, err)

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()
			sub, err := cm.Sub(tt.id.String())
			assert.NoError(t, err)
			assert.NoError(t, sub.Unmarshal(cfg))

			err = xconfmap.Validate(cfg)
			if tt.expectedErrMessage != "" {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedErrMessage)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, cfg)
		})
	}

}
