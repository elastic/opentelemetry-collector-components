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
				TargetField: "message",
				Routing:     RoutingConfig{Dataset: "azure.events", Namespace: "default"},
			},
		},
		{
			name: "valid json format without unwrap",
			config: Config{
				Format:      FormatJSON,
				TargetField: "message",
				Routing:     RoutingConfig{Dataset: "azure.events", Namespace: "default"},
			},
		},
		{
			name: "valid text format",
			config: Config{
				Format:      FormatText,
				TargetField: "message",
				Routing:     RoutingConfig{Dataset: "aws.vpcflow", Namespace: "default"},
			},
		},
		{
			name: "valid ndjson format",
			config: Config{
				Format:      FormatNDJSON,
				TargetField: "message",
				Routing:     RoutingConfig{Dataset: "custom", Namespace: "default"},
			},
		},
		{
			name: "invalid format",
			config: Config{
				Format:  "xml",
				Routing: RoutingConfig{Dataset: "test", Namespace: "default"},
			},
			wantErr: `invalid format "xml"`,
		},
		{
			name: "unwrap with text format",
			config: Config{
				Format:  FormatText,
				Unwrap:  "$.records[*]",
				Routing: RoutingConfig{Dataset: "test", Namespace: "default"},
			},
			wantErr: `unwrap is only supported when format is "json"`,
		},
		{
			name: "invalid jsonpath expression",
			config: Config{
				Format:  FormatJSON,
				Unwrap:  "$[invalid",
				Routing: RoutingConfig{Dataset: "test", Namespace: "default"},
			},
			wantErr: "invalid unwrap JSONPath expression",
		},
		{
			name: "missing dataset",
			config: Config{
				Format:  FormatJSON,
				Routing: RoutingConfig{Namespace: "default"},
			},
			wantErr: "routing.dataset is required",
		},
		{
			name: "missing namespace",
			config: Config{
				Format:  FormatJSON,
				Routing: RoutingConfig{Dataset: "test"},
			},
			wantErr: "routing.namespace is required",
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
