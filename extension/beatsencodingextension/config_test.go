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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name: "valid json format with unwrap",
			config: Config{
				Format:      FormatJSON,
				Unwrap:      "$.records[*]",
					DataStream:     DataStreamConfig{Dataset: "azure.events", Namespace: "default"},
			},
		},
		{
			name: "valid json format without unwrap",
			config: Config{
				Format:      FormatJSON,
					DataStream:     DataStreamConfig{Dataset: "azure.events", Namespace: "default"},
			},
		},
		{
			name: "valid text format",
			config: Config{
				Format:      FormatText,
					DataStream:     DataStreamConfig{Dataset: "aws.vpcflow", Namespace: "default"},
			},
		},
		{
			name: "valid nested unwrap path",
			config: Config{
				Format:      FormatJSON,
				Unwrap:      "$.data.items[*]",
					DataStream:     DataStreamConfig{Dataset: "test", Namespace: "default"},
			},
		},
		{
			name: "invalid format",
			config: Config{
				Format:  "xml",
				DataStream: DataStreamConfig{Dataset: "test", Namespace: "default"},
			},
			wantErr: `invalid format "xml"`,
		},
		{
			name: "unwrap with text format",
			config: Config{
				Format:  FormatText,
				Unwrap:  "$.records[*]",
				DataStream: DataStreamConfig{Dataset: "test", Namespace: "default"},
			},
			wantErr: `unwrap is only supported when format is "json"`,
		},
		{
			name: "unwrap missing $. prefix",
			config: Config{
				Format:  FormatJSON,
				Unwrap:  "records[*]",
				DataStream: DataStreamConfig{Dataset: "test", Namespace: "default"},
			},
			wantErr: `must start with "$."`,
		},
		{
			name: "unwrap missing [*] suffix",
			config: Config{
				Format:  FormatJSON,
				Unwrap:  "$.records",
				DataStream: DataStreamConfig{Dataset: "test", Namespace: "default"},
			},
			wantErr: `must end with "[*]"`,
		},
		{
			name: "unwrap with index access (unsupported)",
			config: Config{
				Format:  FormatJSON,
				Unwrap:  "$.records[0]",
				DataStream: DataStreamConfig{Dataset: "test", Namespace: "default"},
			},
			wantErr: `must end with "[*]"`,
		},
		{
			name: "unwrap with recursive descent (unsupported)",
			config: Config{
				Format:  FormatJSON,
				Unwrap:  "$..records[*]",
				DataStream: DataStreamConfig{Dataset: "test", Namespace: "default"},
			},
			wantErr: "empty key segment",
		},
		{
			name: "unwrap with no key segment",
			config: Config{
				Format:  FormatJSON,
				Unwrap:  "$.[*]",
				DataStream: DataStreamConfig{Dataset: "test", Namespace: "default"},
			},
			wantErr: "must contain at least one key segment",
		},
		{
			name: "valid config with fields",
			config: Config{
				Format:     FormatText,
				DataStream: DataStreamConfig{Dataset: "aws.vpcflow", Namespace: "default"},
				Fields:     map[string]string{"environment": "production", "team": "security"},
			},
		},
		{
			name: "valid config with empty fields",
			config: Config{
				Format:     FormatText,
				DataStream: DataStreamConfig{Dataset: "test", Namespace: "default"},
				Fields:     map[string]string{},
			},
		},
		{
			name: "missing dataset",
			config: Config{
				Format:  FormatJSON,
				DataStream: DataStreamConfig{Namespace: "default"},
			},
			wantErr: "data_stream.dataset is required",
		},
		{
			name: "missing namespace",
			config: Config{
				Format:      FormatJSON,
					DataStream:     DataStreamConfig{Dataset: "test"},
			},
			wantErr: "data_stream.namespace is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
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
