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
	"strings"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNavigateToArray(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		keys    []string
		wantErr string
		// If wantErr is empty, these verify the decoder is positioned inside the array.
		wantMore      bool   // expected dec.More() result after navigation
		wantFirstElem string // if wantMore is true, the raw JSON of the first element
	}{
		{
			name:          "single-level key",
			input:         `{"records": ["a", "b"]}`,
			keys:          []string{"records"},
			wantMore:      true,
			wantFirstElem: `"a"`,
		},
		{
			name:          "multi-level key",
			input:         `{"data": {"items": ["x", "y"]}}`,
			keys:          []string{"data", "items"},
			wantMore:      true,
			wantFirstElem: `"x"`,
		},
		{
			name:          "target key preceded by other keys",
			input:         `{"other": 123, "also": "skipped", "records": ["z"]}`,
			keys:          []string{"records"},
			wantMore:      true,
			wantFirstElem: `"z"`,
		},
		{
			name:          "multi-level with siblings at each level",
			input:         `{"meta": "ignored", "data": {"count": 3, "items": [{"id": 1}]}}`,
			keys:          []string{"data", "items"},
			wantMore:      true,
			wantFirstElem: `{"id":1}`,
		},
		{
			name:     "empty array",
			input:    `{"records": []}`,
			keys:     []string{"records"},
			wantMore: false,
		},
		{
			name:    "key not found at top level",
			input:   `{"other": 1}`,
			keys:    []string{"records"},
			wantErr: `key "records" not found in object`,
		},
		{
			name:    "key not found at nested level",
			input:   `{"data": {"count": 1}}`,
			keys:    []string{"data", "items"},
			wantErr: `key "items" not found in object`,
		},
		{
			name:    "root is not an object — array",
			input:   `[1, 2, 3]`,
			keys:    []string{"records"},
			wantErr: `expected '{' before key "records"`,
		},
		{
			name:    "root is not an object — string",
			input:   `"hello"`,
			keys:    []string{"records"},
			wantErr: `expected '{' before key "records"`,
		},
		{
			name:    "last key value is not an array — object",
			input:   `{"records": {"nested": true}}`,
			keys:    []string{"records"},
			wantErr: `expected '[' at key "records"`,
		},
		{
			name:    "last key value is not an array — string",
			input:   `{"records": "oops"}`,
			keys:    []string{"records"},
			wantErr: `expected '[' at key "records"`,
		},
		{
			name:    "intermediate key value is not an object",
			input:   `{"data": "not-an-object"}`,
			keys:    []string{"data", "items"},
			wantErr: `expected '{' before key "items"`,
		},
		{
			name:    "empty input",
			input:   ``,
			keys:    []string{"records"},
			wantErr: `expected object at key "records"`,
		},
		{
			name:    "empty object",
			input:   `{}`,
			keys:    []string{"records"},
			wantErr: `key "records" not found in object`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dec := json.NewDecoder(strings.NewReader(tc.input))
			err := navigateToArray(dec, tc.keys)

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantMore, dec.More())

			if tc.wantMore && tc.wantFirstElem != "" {
				var raw json.RawMessage
				require.NoError(t, dec.Decode(&raw))
				assert.JSONEq(t, tc.wantFirstElem, string(raw))
			}
		})
	}
}
