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

package attribute

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestPut(t *testing.T) {
	var (
		key = "test_key"

		oldStr    = "old_str_value"
		oldInt    = int64(123)
		oldDouble = 2.71
		oldBool   = false

		newStr    = "test_str_value"
		newInt    = int64(42)
		newDouble = 3.14
		newBool   = true
	)

	tests := []struct {
		name     string
		value    any
		exists   bool
		expected any
	}{
		{name: "PutStr attr does not exist", value: newStr, exists: false, expected: newStr},
		{name: "PutStr attr exists", value: newStr, exists: true, expected: oldStr},
		{name: "PutInt attr does not exist", value: newInt, exists: false, expected: newInt},
		{name: "PutInt attr exists", value: newInt, exists: true, expected: oldInt},
		{name: "PutDouble attr does not exist", value: newDouble, exists: false, expected: newDouble},
		{name: "PutDouble attr exists", value: newDouble, exists: true, expected: oldDouble},
		{name: "PutBool attr does not exist", value: newBool, exists: false, expected: newBool},
		{name: "PutBool attr exists", value: newBool, exists: true, expected: oldBool},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup map based on the value type and if a prior value should exists
			attrs := pcommon.NewMap()
			if tt.exists {
				switch v := tt.value.(type) {
				case string:
					attrs.PutStr(key, oldStr)
				case int64:
					attrs.PutInt(key, oldInt)
				case float64:
					attrs.PutDouble(key, oldDouble)
				case bool:
					attrs.PutBool(key, oldBool)
				default:
					t.Fatalf("unexpected value type: %T", v)
				}
			}

			// Attempt to add attribute based on value type
			switch v := tt.value.(type) {
			case string:
				PutStr(attrs, key, v)
			case int64:
				PutInt(attrs, key, v)
			case float64:
				PutDouble(attrs, key, v)
			case bool:
				PutBool(attrs, key, v)
			default:
				t.Fatalf("unexpected value type: %T", v)
			}

			// Read value from map
			val, exists := attrs.Get(key)
			if !exists {
				t.Error("expected attribute to exist")
			}

			// Validate the read value
			var actualValue any
			switch val.Type() {
			case pcommon.ValueTypeStr:
				actualValue = val.Str()
			case pcommon.ValueTypeInt:
				actualValue = val.Int()
			case pcommon.ValueTypeDouble:
				actualValue = val.Double()
			case pcommon.ValueTypeBool:
				actualValue = val.Bool()
			default:
				t.Fatalf("unexpected value type: %v", val.Type())
			}

			if actualValue != tt.expected {
				t.Errorf("value = %v, expected %v", actualValue, tt.expected)
			}
		})
	}
}

func TestPutNonEmptyStr(t *testing.T) {
	key := "test_key"

	cases := []struct {
		name      string
		value     string
		existing  string
		wantExist bool
		expected  string
	}{
		{
			name:      "non_empty_value_no_existing",
			value:     "hello",
			wantExist: true,
			expected:  "hello",
		},
		{
			name:      "empty_value_no_existing",
			value:     "",
			wantExist: false,
		},
		{
			name:      "empty_value_with_existing",
			value:     "",
			existing:  "existing",
			wantExist: true,
			expected:  "existing",
		},
		{
			name:      "non_empty_value_with_existing",
			value:     "new",
			existing:  "existing",
			wantExist: true,
			expected:  "existing",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			attrs := pcommon.NewMap()
			if tc.existing != "" {
				attrs.PutStr(key, tc.existing)
			}

			PutNonEmptyStr(attrs, key, tc.value)

			val, exists := attrs.Get(key)
			if exists != tc.wantExist {
				t.Fatalf("exists = %v, want %v", exists, tc.wantExist)
			}
			if exists && val.Str() != tc.expected {
				t.Errorf("value = %q, expected %q", val.Str(), tc.expected)
			}
		})
	}
}
