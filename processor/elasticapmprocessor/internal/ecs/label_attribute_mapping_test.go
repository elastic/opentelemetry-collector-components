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

package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// TestSetLabelAttributeValue verifies that setLabelAttributeValue stores
// supported value types under the correct labels.* / numeric_labels.* prefix
// and rejects unsupported types (Map, Bytes, Empty). This matches
// apm-data's setLabel behaviour (input/otlp/metadata.go).
func TestSetLabelAttributeValue(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   func() pcommon.Value
		wantOK  bool
		wantKey string // expected destination key; empty when wantOK is false
		wantRaw any    // expected value: string, float64, or []any
	}{
		// --- Scalar types ---
		{
			name:    "string",
			key:     "str_key",
			value:   func() pcommon.Value { return pcommon.NewValueStr("hello") },
			wantOK:  true,
			wantKey: "labels.str_key",
			wantRaw: "hello",
		},
		{
			name:    "bool",
			key:     "bool_key",
			value:   func() pcommon.Value { return pcommon.NewValueBool(true) },
			wantOK:  true,
			wantKey: "labels.bool_key",
			wantRaw: "true",
		},
		{
			name:    "int",
			key:     "int_key",
			value:   func() pcommon.Value { return pcommon.NewValueInt(42) },
			wantOK:  true,
			wantKey: "numeric_labels.int_key",
			wantRaw: float64(42),
		},
		{
			name:    "double",
			key:     "double_key",
			value:   func() pcommon.Value { return pcommon.NewValueDouble(3.14) },
			wantOK:  true,
			wantKey: "numeric_labels.double_key",
			wantRaw: 3.14,
		},

		// --- Homogeneous slice types ---
		{
			name: "string slice",
			key:  "str_slice",
			value: func() pcommon.Value {
				v := pcommon.NewValueSlice()
				v.Slice().AppendEmpty().SetStr("a")
				v.Slice().AppendEmpty().SetStr("b")
				return v
			},
			wantOK:  true,
			wantKey: "labels.str_slice",
			wantRaw: []any{"a", "b"},
		},
		{
			name: "int slice",
			key:  "int_slice",
			value: func() pcommon.Value {
				v := pcommon.NewValueSlice()
				v.Slice().AppendEmpty().SetInt(1)
				v.Slice().AppendEmpty().SetInt(2)
				return v
			},
			wantOK:  true,
			wantKey: "numeric_labels.int_slice",
			wantRaw: []any{float64(1), float64(2)},
		},
		{
			name: "double slice",
			key:  "double_slice",
			value: func() pcommon.Value {
				v := pcommon.NewValueSlice()
				v.Slice().AppendEmpty().SetDouble(1.1)
				v.Slice().AppendEmpty().SetDouble(2.2)
				return v
			},
			wantOK:  true,
			wantKey: "numeric_labels.double_slice",
			wantRaw: []any{1.1, 2.2},
		},
		{
			name: "bool slice",
			key:  "bool_slice",
			value: func() pcommon.Value {
				v := pcommon.NewValueSlice()
				v.Slice().AppendEmpty().SetBool(true)
				v.Slice().AppendEmpty().SetBool(false)
				return v
			},
			wantOK:  true,
			wantKey: "labels.bool_slice",
			wantRaw: []any{"true", "false"},
		},

		// --- Unsupported types (should NOT be stored) ---
		{
			name:   "empty slice",
			key:    "empty",
			value:  func() pcommon.Value { return pcommon.NewValueSlice() },
			wantOK: false,
		},
		{
			name: "map",
			key:  "map_key",
			value: func() pcommon.Value {
				v := pcommon.NewValueMap()
				v.Map().PutStr("nested", "value")
				return v
			},
			wantOK: false,
		},
		{
			name: "bytes",
			key:  "bytes_key",
			value: func() pcommon.Value {
				v := pcommon.NewValueBytes()
				v.Bytes().Append(0x01, 0x02)
				return v
			},
			wantOK: false,
		},
		{
			name:   "empty value",
			key:    "empty_key",
			value:  func() pcommon.Value { return pcommon.NewValueEmpty() },
			wantOK: false,
		},
		{
			name: "slice with map element",
			key:  "map_slice",
			value: func() pcommon.Value {
				v := pcommon.NewValueSlice()
				v.Slice().AppendEmpty().SetEmptyMap().PutStr("a", "b")
				return v
			},
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			attrs := pcommon.NewMap()
			ok := setLabelAttributeValue(attrs, tc.key, tc.value())
			assert.Equal(t, tc.wantOK, ok)

			if !tc.wantOK {
				assert.Equal(t, 0, attrs.Len(), "unsupported type should not add attributes")
				return
			}

			got, exists := attrs.Get(tc.wantKey)
			require.True(t, exists, "expected %s to be set", tc.wantKey)

			switch want := tc.wantRaw.(type) {
			case string:
				assert.Equal(t, want, got.Str())
			case float64:
				assert.InDelta(t, want, got.Double(), 1e-9)
			case []any:
				assert.Equal(t, want, got.Slice().AsRaw())
			default:
				t.Fatalf("unsupported wantRaw type %T", tc.wantRaw)
			}
		})
	}
}
