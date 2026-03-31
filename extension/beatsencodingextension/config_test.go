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

package beatsencodingextension

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"

	"github.com/elastic/opentelemetry-collector-components/extension/beatsencodingextension/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		id       component.ID
		expected *Config
		wantErr  string
	}{
		{
			id: component.NewIDWithName(metadata.Type, "json_with_unwrap"),
			expected: func() *Config {
				cfg := createDefaultConfig().(*Config)
				cfg.Unwrap = "$.records[*]"
				cfg.unwrapKeys = []string{"records"}
				cfg.DataStream.Dataset = "azure.events"
				return cfg
			}(),
		},
		{
			id: component.NewIDWithName(metadata.Type, "json_without_unwrap"),
			expected: func() *Config {
				cfg := createDefaultConfig().(*Config)
				cfg.DataStream.Dataset = "azure.events"
				return cfg
			}(),
		},
		{
			id: component.NewIDWithName(metadata.Type, "text"),
			expected: func() *Config {
				cfg := createDefaultConfig().(*Config)
				cfg.Format = FormatText
				cfg.DataStream.Dataset = "aws.vpcflow"
				return cfg
			}(),
		},
		{
			id: component.NewIDWithName(metadata.Type, "nested_unwrap"),
			expected: func() *Config {
				cfg := createDefaultConfig().(*Config)
				cfg.Unwrap = "$.data.items[*]"
				cfg.unwrapKeys = []string{"data", "items"}
				cfg.DataStream.Dataset = "test"
				return cfg
			}(),
		},
		{
			id:      component.NewIDWithName(metadata.Type, "invalid_format"),
			wantErr: `invalid format "xml"`,
		},
		{
			id:      component.NewIDWithName(metadata.Type, "unwrap_with_text"),
			wantErr: `unwrap is only supported when format is "json"`,
		},
		{
			id:      component.NewIDWithName(metadata.Type, "unwrap_missing_prefix"),
			wantErr: `must start with "$."`,
		},
		{
			id:      component.NewIDWithName(metadata.Type, "unwrap_missing_suffix"),
			wantErr: `must end with "[*]"`,
		},
		{
			id:      component.NewIDWithName(metadata.Type, "unwrap_with_index"),
			wantErr: `must end with "[*]"`,
		},
		{
			id:      component.NewIDWithName(metadata.Type, "unwrap_recursive_descent"),
			wantErr: "empty key segment",
		},
		{
			id:      component.NewIDWithName(metadata.Type, "unwrap_no_key_segment"),
			wantErr: "must contain at least one key segment",
		},
		{
			id: component.NewIDWithName(metadata.Type, "with_fields"),
			expected: func() *Config {
				cfg := createDefaultConfig().(*Config)
				cfg.Format = FormatText
				cfg.DataStream.Dataset = "aws.vpcflow"
				cfg.Fields = map[string]any{"environment": "production", "team": "security"}
				return cfg
			}(),
		},
		{
			id: component.NewIDWithName(metadata.Type, "empty_fields"),
			expected: func() *Config {
				cfg := createDefaultConfig().(*Config)
				cfg.Format = FormatText
				cfg.DataStream.Dataset = "test"
				cfg.Fields = map[string]any{}
				return cfg
			}(),
		},
		{
			id:      component.NewIDWithName(metadata.Type, "missing_dataset"),
			wantErr: "data_stream.dataset is required",
		},
		{
			id:      component.NewIDWithName(metadata.Type, "missing_namespace"),
			wantErr: "data_stream.namespace is required",
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
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, cfg)
		})
	}
}

func TestParseUnwrapPath(t *testing.T) {
	tests := []struct {
		expr    string
		want    []string
		wantErr string
	}{
		{expr: "$.records[*]", want: []string{"records"}},
		{expr: "$.Records[*]", want: []string{"Records"}},
		{expr: "$.data.items[*]", want: []string{"data", "items"}},
		{expr: "$.a.b.c[*]", want: []string{"a", "b", "c"}},
		{expr: "records[*]", wantErr: `must start with "$."`},
		{expr: "$.records", wantErr: `must end with "[*]"`},
		{expr: "$.[*]", wantErr: "must contain at least one key segment"},
		{expr: "$..records[*]", wantErr: "empty key segment"},
		{expr: "$.a..b[*]", wantErr: "empty key segment"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			got, err := parseUnwrapPath(tt.expr)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
