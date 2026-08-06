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

package vercelreceiver

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"

	"github.com/elastic/opentelemetry-collector-components/receiver/vercelreceiver/internal/metadata"
)

func TestDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	require.Equal(t, defaultEndpoint, cfg.NetAddr.Endpoint)
	require.Equal(t, defaultRoute, cfg.Route)
}

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		id           component.ID
		expected     func() *Config
		expectedErrs []error
	}{
		{
			id: component.NewID(metadata.Type),
			expected: func() *Config {
				cfg := createDefaultConfig().(*Config)
				cfg.NetAddr.Endpoint = "localhost:0"
				cfg.Route = "/vercel"
				cfg.Encoding = EncodingConfig{
					Extension: component.MustNewID("vercel"),
					Flush: FlushConfig{
						Items: 500,
						Bytes: 524288,
					},
				}
				return cfg
			},
		},
		{
			id:           component.NewIDWithName(metadata.Type, "missing_route"),
			expectedErrs: []error{errMissingRoute},
		},
		{
			id:           component.NewIDWithName(metadata.Type, "invalid_route"),
			expectedErrs: []error{errInvalidRoute},
		},
		{
			id:           component.NewIDWithName(metadata.Type, "missing_encoding"),
			expectedErrs: []error{errMissingEncoding},
		},
		{
			id:           component.NewIDWithName(metadata.Type, "negative_flush"),
			expectedErrs: []error{errNegativeFlush},
		},
	}
	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
			require.NoError(t, err)

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()
			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			err = xconfmap.Validate(cfg)
			if len(tt.expectedErrs) > 0 {
				for _, expectedErr := range tt.expectedErrs {
					require.ErrorIs(t, err, expectedErr)
				}
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.expected(), cfg)
		})
	}
}
