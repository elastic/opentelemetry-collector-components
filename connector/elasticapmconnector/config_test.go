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

package elasticapmconnector // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor"

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"

	"github.com/elastic/opentelemetry-collector-components/connector/elasticapmconnector/internal/metadata"
)

func TestConfig(t *testing.T) {
	for _, tc := range []struct {
		path     string // relative to testdata/configs directory
		expected *Config
		// It's currently impossible for the config to be invalid, excluding invalid YAML/unknown keys
	}{
		{
			path:     "empty",
			expected: &Config{},
		},
		{
			path: "customattrs",
			expected: &Config{
				CustomResourceAttributes: []CustomResourceAttribute{
					{Key: "res.1"},
					{Key: "res.2"},
				},
				CustomSpanAttributes: []string{
					"span.1",
					"span.2",
				},
			},
		},
		{
			path: "dynamicattrs",
			expected: &Config{
				CustomResourceAttributes: []CustomResourceAttribute{
					{Key: "res.1"},
					{KeysExpression: `otelcol.client.metadata["x-elastic-dynamic-resource-attributes"]`},
				},
			},
		},
		{
			path: "full",
			expected: &Config{
				ErrorMode: ottl.SilentError,
				Aggregation: &AggregationConfig{
					Directory:    "/path/to/aggregation/state",
					MetadataKeys: []string{"a", "B", "c"},
					Intervals:    []time.Duration{time.Second, time.Minute},
					Limits: AggregationLimitConfig{
						ResourceLimit: LimitConfig{
							MaxCardinality: 1,
						},
						ScopeLimit: LimitConfig{
							MaxCardinality: 1,
						},
						MetricLimit: LimitConfig{
							MaxCardinality: 1,
						},
						DatapointLimit: LimitConfig{
							MaxCardinality: 1,
						},
					},
				},
				CustomResourceAttributes: []CustomResourceAttribute{
					{Key: "res.1"},
					{Key: "res.2"},
				},
				CustomSpanAttributes: []string{
					"span.1",
					"span.2",
				},
			},
		},
	} {
		t.Run(tc.path, func(t *testing.T) {
			dir := filepath.Join("testdata", "config")
			cfg := &Config{}
			cm, err := confmaptest.LoadConf(filepath.Join(dir, tc.path+".yaml"))
			require.NoError(t, err)

			sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(&cfg))

			err = xconfmap.Validate(cfg)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, cfg)
		})
	}
}

func TestCustomResourceAttributeValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid static key",
			cfg: Config{
				CustomResourceAttributes: []CustomResourceAttribute{
					{Key: "res.1"},
				},
			},
		},
		{
			name: "valid keys_expression",
			cfg: Config{
				CustomResourceAttributes: []CustomResourceAttribute{
					{KeysExpression: `otelcol.client.metadata["x"]`},
				},
			},
		},
		{
			name: "invalid both set",
			cfg: Config{
				CustomResourceAttributes: []CustomResourceAttribute{
					{Key: "res.1", KeysExpression: `otelcol.client.metadata["x"]`},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid neither set",
			cfg: Config{
				CustomResourceAttributes: []CustomResourceAttribute{
					{},
				},
			},
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := xconfmap.Validate(tc.cfg)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
