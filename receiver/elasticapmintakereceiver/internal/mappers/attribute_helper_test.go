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

package mappers

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestPutNonEmptyStr(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		value     string
		wantKey   string
		wantValue string
		wantFound bool
	}{
		{
			name:      "empty string should not be added",
			key:       "test.key",
			value:     "",
			wantKey:   "test.key",
			wantFound: false,
		},
		{
			name:      "non-empty string should be added",
			key:       "test.key",
			value:     "test-value",
			wantKey:   "test.key",
			wantValue: "test-value",
			wantFound: true,
		},
		{
			name:      "whitespace string should be added",
			key:       "test.key",
			value:     " ",
			wantKey:   "test.key",
			wantValue: " ",
			wantFound: true,
		},
		{
			name:      "long string should be added",
			key:       "test.key",
			value:     "this is a very long string value",
			wantKey:   "test.key",
			wantValue: "this is a very long string value",
			wantFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attributes := pcommon.NewMap()
			putNonEmptyStr(attributes, tt.key, tt.value)

			val, found := attributes.Get(tt.wantKey)
			assert.Equal(t, tt.wantFound, found)
			if tt.wantFound {
				assert.Equal(t, tt.wantValue, val.Str())
			}
		})
	}
}

func TestPutPtrInt(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		value     func() interface{} // function that returns a pointer to any integer type
		wantValue int64
		wantFound bool
	}{
		// nil pointer cases
		{
			name:      "nil uint32 pointer should not be added",
			key:       "test.key",
			value:     func() interface{} { return (*uint32)(nil) },
			wantFound: false,
		},
		{
			name:      "nil uint64 pointer should not be added",
			key:       "test.key",
			value:     func() interface{} { return (*uint64)(nil) },
			wantFound: false,
		},
		{
			name:      "nil int32 pointer should not be added",
			key:       "test.key",
			value:     func() interface{} { return (*int32)(nil) },
			wantFound: false,
		},
		{
			name:      "nil int64 pointer should not be added",
			key:       "test.key",
			value:     func() interface{} { return (*int64)(nil) },
			wantFound: false,
		},
		// uint8 cases
		{
			name: "uint8 zero value",
			key:  "test.key",
			value: func() interface{} {
				v := uint8(0)
				return &v
			},
			wantValue: 0,
			wantFound: true,
		},
		{
			name: "uint8 max value",
			key:  "test.key",
			value: func() interface{} {
				v := uint8(255)
				return &v
			},
			wantValue: 255,
			wantFound: true,
		},
		// uint16 cases
		{
			name: "uint16 value",
			key:  "test.key",
			value: func() interface{} {
				v := uint16(65535)
				return &v
			},
			wantValue: 65535,
			wantFound: true,
		},
		// uint32 cases
		{
			name: "uint32 zero value",
			key:  "test.key",
			value: func() interface{} {
				v := uint32(0)
				return &v
			},
			wantValue: 0,
			wantFound: true,
		},
		{
			name: "uint32 small value",
			key:  "test.key",
			value: func() interface{} {
				v := uint32(42)
				return &v
			},
			wantValue: 42,
			wantFound: true,
		},
		{
			name: "uint32 max value",
			key:  "test.key",
			value: func() interface{} {
				v := uint32(math.MaxUint32)
				return &v
			},
			wantValue: math.MaxUint32,
			wantFound: true,
		},
		// uint64 cases
		{
			name: "uint64 zero value",
			key:  "test.key",
			value: func() interface{} {
				v := uint64(0)
				return &v
			},
			wantValue: 0,
			wantFound: true,
		},
		{
			name: "uint64 small value",
			key:  "test.key",
			value: func() interface{} {
				v := uint64(100)
				return &v
			},
			wantValue: 100,
			wantFound: true,
		},
		{
			name: "uint64 value within int64 range",
			key:  "test.key",
			value: func() interface{} {
				v := uint64(math.MaxInt64)
				return &v
			},
			wantValue: math.MaxInt64,
			wantFound: true,
		},
		{
			name: "uint64 overflow should be clamped to MaxInt64",
			key:  "test.key",
			value: func() interface{} {
				v := uint64(math.MaxInt64 + 1)
				return &v
			},
			wantValue: math.MaxInt64,
			wantFound: true,
		},
		{
			name: "uint64 large overflow should be clamped to MaxInt64",
			key:  "test.key",
			value: func() interface{} {
				v := uint64(math.MaxUint64)
				return &v
			},
			wantValue: math.MaxInt64,
			wantFound: true,
		},
		// int8 cases
		{
			name: "int8 zero value",
			key:  "test.key",
			value: func() interface{} {
				v := int8(0)
				return &v
			},
			wantValue: 0,
			wantFound: true,
		},
		{
			name: "int8 positive value",
			key:  "test.key",
			value: func() interface{} {
				v := int8(127)
				return &v
			},
			wantValue: 127,
			wantFound: true,
		},
		{
			name: "int8 negative value should be preserved",
			key:  "test.key",
			value: func() interface{} {
				v := int8(-1)
				return &v
			},
			wantValue: -1,
			wantFound: true,
		},
		{
			name: "int8 min value should be preserved",
			key:  "test.key",
			value: func() interface{} {
				v := int8(-128)
				return &v
			},
			wantValue: -128,
			wantFound: true,
		},
		// int16 cases
		{
			name: "int16 negative value should be preserved",
			key:  "test.key",
			value: func() interface{} {
				v := int16(-1000)
				return &v
			},
			wantValue: -1000,
			wantFound: true,
		},
		// int32 cases
		{
			name: "int32 zero value",
			key:  "test.key",
			value: func() interface{} {
				v := int32(0)
				return &v
			},
			wantValue: 0,
			wantFound: true,
		},
		{
			name: "int32 positive value",
			key:  "test.key",
			value: func() interface{} {
				v := int32(42)
				return &v
			},
			wantValue: 42,
			wantFound: true,
		},
		{
			name: "int32 negative value should be preserved",
			key:  "test.key",
			value: func() interface{} {
				v := int32(-1)
				return &v
			},
			wantValue: -1,
			wantFound: true,
		},
		{
			name: "int32 min value should be preserved",
			key:  "test.key",
			value: func() interface{} {
				v := int32(math.MinInt32)
				return &v
			},
			wantValue: math.MinInt32,
			wantFound: true,
		},
		// int64 cases
		{
			name: "int64 zero value",
			key:  "test.key",
			value: func() interface{} {
				v := int64(0)
				return &v
			},
			wantValue: 0,
			wantFound: true,
		},
		{
			name: "int64 positive value",
			key:  "test.key",
			value: func() interface{} {
				v := int64(42)
				return &v
			},
			wantValue: 42,
			wantFound: true,
		},
		{
			name: "int64 negative value should be preserved",
			key:  "test.key",
			value: func() interface{} {
				v := int64(-1)
				return &v
			},
			wantValue: -1,
			wantFound: true,
		},
		{
			name: "int64 max value",
			key:  "test.key",
			value: func() interface{} {
				v := int64(math.MaxInt64)
				return &v
			},
			wantValue: math.MaxInt64,
			wantFound: true,
		},
		{
			name: "int64 min value should be preserved",
			key:  "test.key",
			value: func() interface{} {
				v := int64(math.MinInt64)
				return &v
			},
			wantValue: math.MinInt64,
			wantFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attributes := pcommon.NewMap()

			// Call putPtrInt with the appropriate type
			val := tt.value()
			switch v := val.(type) {
			case *uint8:
				putPtrInt(attributes, tt.key, v)
			case *uint16:
				putPtrInt(attributes, tt.key, v)
			case *uint32:
				putPtrInt(attributes, tt.key, v)
			case *uint64:
				putPtrInt(attributes, tt.key, v)
			case *int8:
				putPtrInt(attributes, tt.key, v)
			case *int16:
				putPtrInt(attributes, tt.key, v)
			case *int32:
				putPtrInt(attributes, tt.key, v)
			case *int64:
				putPtrInt(attributes, tt.key, v)
			default:
				t.Fatalf("unexpected value type: %T", v)
			}

			gotVal, found := attributes.Get(tt.key)
			assert.Equal(t, tt.wantFound, found, "key presence mismatch")
			if tt.wantFound {
				require.Equal(t, pcommon.ValueTypeInt, gotVal.Type(), "value type should be Int")
				assert.Equal(t, tt.wantValue, gotVal.Int(), "value mismatch")
			}
		})
	}
}

func TestPutPtrBool(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		value     func() *bool
		wantValue bool
		wantFound bool
	}{
		{
			name:      "nil pointer should not be added",
			key:       "test.key",
			value:     func() *bool { return nil },
			wantFound: false,
		},
		{
			name: "true value should be added",
			key:  "test.key",
			value: func() *bool {
				v := true
				return &v
			},
			wantValue: true,
			wantFound: true,
		},
		{
			name: "false value should be added",
			key:  "test.key",
			value: func() *bool {
				v := false
				return &v
			},
			wantValue: false,
			wantFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attributes := pcommon.NewMap()
			putPtrBool(attributes, tt.key, tt.value())

			val, found := attributes.Get(tt.key)
			assert.Equal(t, tt.wantFound, found)
			if tt.wantFound {
				require.Equal(t, pcommon.ValueTypeBool, val.Type())
				assert.Equal(t, tt.wantValue, val.Bool())
			}
		})
	}
}
