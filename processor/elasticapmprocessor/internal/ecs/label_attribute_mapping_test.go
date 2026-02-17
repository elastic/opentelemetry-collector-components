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
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestSetLabelAttributeValue_ScalarTypes(t *testing.T) {
	attrs := pcommon.NewMap()

	v := pcommon.NewValueStr("hello")
	assert.True(t, setLabelAttributeValue(attrs, "str_key", v))
	got, ok := attrs.Get("labels.str_key")
	assert.True(t, ok)
	assert.Equal(t, "hello", got.Str())

	v = pcommon.NewValueBool(true)
	assert.True(t, setLabelAttributeValue(attrs, "bool_key", v))
	got, ok = attrs.Get("labels.bool_key")
	assert.True(t, ok)
	assert.Equal(t, "true", got.Str())

	v = pcommon.NewValueInt(42)
	assert.True(t, setLabelAttributeValue(attrs, "int_key", v))
	got, ok = attrs.Get("numeric_labels.int_key")
	assert.True(t, ok)
	assert.Equal(t, float64(42), got.Double())

	v = pcommon.NewValueDouble(3.14)
	assert.True(t, setLabelAttributeValue(attrs, "double_key", v))
	got, ok = attrs.Get("numeric_labels.double_key")
	assert.True(t, ok)
	assert.Equal(t, 3.14, got.Double())
}

func TestSetLabelAttributeValue_SliceTypes(t *testing.T) {
	attrs := pcommon.NewMap()

	strSlice := pcommon.NewValueSlice()
	strSlice.Slice().AppendEmpty().SetStr("a")
	strSlice.Slice().AppendEmpty().SetStr("b")
	assert.True(t, setLabelAttributeValue(attrs, "str_slice", strSlice))
	got, ok := attrs.Get("labels.str_slice")
	assert.True(t, ok)
	assert.Equal(t, []any{"a", "b"}, got.Slice().AsRaw())

	intSlice := pcommon.NewValueSlice()
	intSlice.Slice().AppendEmpty().SetInt(1)
	intSlice.Slice().AppendEmpty().SetInt(2)
	assert.True(t, setLabelAttributeValue(attrs, "int_slice", intSlice))
	got, ok = attrs.Get("numeric_labels.int_slice")
	assert.True(t, ok)
	assert.Equal(t, []any{float64(1), float64(2)}, got.Slice().AsRaw())

	doubleSlice := pcommon.NewValueSlice()
	doubleSlice.Slice().AppendEmpty().SetDouble(1.1)
	doubleSlice.Slice().AppendEmpty().SetDouble(2.2)
	assert.True(t, setLabelAttributeValue(attrs, "double_slice", doubleSlice))
	got, ok = attrs.Get("numeric_labels.double_slice")
	assert.True(t, ok)
	assert.Equal(t, []any{1.1, 2.2}, got.Slice().AsRaw())

	boolSlice := pcommon.NewValueSlice()
	boolSlice.Slice().AppendEmpty().SetBool(true)
	boolSlice.Slice().AppendEmpty().SetBool(false)
	assert.True(t, setLabelAttributeValue(attrs, "bool_slice", boolSlice))
	got, ok = attrs.Get("labels.bool_slice")
	assert.True(t, ok)
	assert.Equal(t, []any{"true", "false"}, got.Slice().AsRaw())
}

func TestSetLabelAttributeValue_EmptySlice(t *testing.T) {
	attrs := pcommon.NewMap()
	emptySlice := pcommon.NewValueSlice()
	assert.False(t, setLabelAttributeValue(attrs, "empty", emptySlice))
	assert.Equal(t, 0, attrs.Len())
}

// TestSetLabelAttributeValue_UnsupportedTypes verifies that Map, Bytes,
// and Empty value types are not stored as labels and return false.
// This matches apm-data's setLabel behaviour (input/otlp/metadata.go)
// where these types are intentionally ignored — Elasticsearch label
// mappings only support flat scalar values and homogeneous arrays.
func TestSetLabelAttributeValue_UnsupportedTypes(t *testing.T) {
	attrs := pcommon.NewMap()

	mapVal := pcommon.NewValueMap()
	mapVal.Map().PutStr("nested", "value")
	assert.False(t, setLabelAttributeValue(attrs, "map_key", mapVal))
	_, ok := attrs.Get("labels.map_key")
	assert.False(t, ok)

	bytesVal := pcommon.NewValueBytes()
	bytesVal.Bytes().Append(0x01, 0x02)
	assert.False(t, setLabelAttributeValue(attrs, "bytes_key", bytesVal))
	_, ok = attrs.Get("labels.bytes_key")
	assert.False(t, ok)

	emptyVal := pcommon.NewValueEmpty()
	assert.False(t, setLabelAttributeValue(attrs, "empty_key", emptyVal))
	_, ok = attrs.Get("labels.empty_key")
	assert.False(t, ok)

	assert.Equal(t, 0, attrs.Len())
}

// TestSetLabelAttributeValue_SliceWithUnsupportedElementType verifies that
// slices whose first element is a Map (or other unsupported type) are not
// stored as labels and return false.
func TestSetLabelAttributeValue_SliceWithUnsupportedElementType(t *testing.T) {
	attrs := pcommon.NewMap()

	mapSlice := pcommon.NewValueSlice()
	mapSlice.Slice().AppendEmpty().SetEmptyMap().PutStr("a", "b")
	assert.False(t, setLabelAttributeValue(attrs, "map_slice", mapSlice))
	assert.Equal(t, 0, attrs.Len())
}
