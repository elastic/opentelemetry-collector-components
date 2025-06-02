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

	"github.com/stretchr/testify/assert"
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
				assert.EqualError(t, err, tt.expectedErrMessage)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, cfg)
		})
	}

}
