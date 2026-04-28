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

// Package document provides a dotted-path mutable view over a single OTLP
// record (log record, span, or metric data point). It mirrors the semantics
// of the Rust `streamlang-runtime`'s `Document` type 1:1 so that pipelines
// authored once behave consistently across execution targets (Elasticsearch
// ingest pipelines, ES|QL, OTTL, and this collector processor).
package document // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// SignalKind identifies which OTLP signal a Document is wrapping.
type SignalKind int

const (
	SignalLogs SignalKind = iota
	SignalTraces
	SignalMetrics
)

// ErrUnsupportedTarget is returned by Document.Set when the requested path is
// not writable (e.g. setting trace_id, or assigning a string to a numeric
// field).
var ErrUnsupportedTarget = errors.New("streamlang: unsupported target path")

// Document is a dotted-path mutable view over a single OTLP record.
//
// Semantics mirror the Rust runtime's Document: dotted attribute paths are
// expanded into nested pcommon.Map values on Set, while Get also falls back
// to a flat dotted-key lookup for wire-level compatibility (OTLP normally
// serialises attribute keys as flat strings containing dots).
type Document interface {
	Signal() SignalKind
	Get(path string) (Value, bool)
	// Field returns the value at the dotted path as an `any` (string, int64,
	// float64, bool, []any, map[string]any, or nil). Equivalent to
	// Get(path).AsAny() but skips the Value struct on the hot path. This
	// also lets Document satisfy condition.Source structurally — no adapter
	// allocation per condition Get.
	Field(path string) (any, bool)
	Set(path string, v Value) error
	Remove(path string) bool
	RemoveByPrefix(prefix string)
	Has(path string) bool
	Drop()
	IsDropped() bool
}

// ValueType is a small enum mirroring the variants Streamlang cares about.
// We keep it independent of pcommon.ValueType so callers don't have to
// import pdata to inspect a Value's kind in tests.
type ValueType int

const (
	// ValueTypeEmpty is an empty / null value.
	ValueTypeEmpty ValueType = iota
	ValueTypeStr
	ValueTypeBool
	ValueTypeInt
	ValueTypeDouble
	ValueTypeBytes
	ValueTypeSlice
	ValueTypeMap
)

// Value carries a typed scalar/array/map. It can be constructed from a Go
// primitive or from a pcommon.Value. When wrapping a pcommon.Value, the
// underlying storage is shared (no copy on the hot path).
type Value struct {
	// pv is set when the value originated from pdata.
	pv pcommon.Value
	// hasPV is true when pv is the canonical storage; otherwise the go field
	// below carries the value.
	hasPV bool
	// goVal carries primitive constructor values when hasPV == false.
	goVal any
	// goType labels goVal so we can answer Type() without reflection.
	goType ValueType
}

// FromPcommon wraps a pcommon.Value without copying.
func FromPcommon(v pcommon.Value) Value {
	return Value{pv: v, hasPV: true}
}

// StringValue constructs a string Value.
func StringValue(s string) Value {
	return Value{goVal: s, goType: ValueTypeStr}
}

// BoolValue constructs a bool Value.
func BoolValue(b bool) Value {
	return Value{goVal: b, goType: ValueTypeBool}
}

// IntValue constructs an int64 Value.
func IntValue(i int64) Value {
	return Value{goVal: i, goType: ValueTypeInt}
}

// DoubleValue constructs a float64 Value.
func DoubleValue(f float64) Value {
	return Value{goVal: f, goType: ValueTypeDouble}
}

// BytesValue constructs a bytes Value.
func BytesValue(b []byte) Value {
	return Value{goVal: b, goType: ValueTypeBytes}
}

// SliceValue constructs a slice Value.
func SliceValue(items []Value) Value {
	return Value{goVal: items, goType: ValueTypeSlice}
}

// MapValue constructs a map Value.
func MapValue(kvs map[string]Value) Value {
	return Value{goVal: kvs, goType: ValueTypeMap}
}

// NilValue constructs an empty Value.
func NilValue() Value {
	return Value{goType: ValueTypeEmpty}
}

// FromAny constructs a Value from a Go any. Accepts string, bool, int,
// int32, int64, float32, float64, []byte, []any, map[string]any, and nil.
func FromAny(a any) (Value, error) {
	switch v := a.(type) {
	case nil:
		return NilValue(), nil
	case string:
		return StringValue(v), nil
	case bool:
		return BoolValue(v), nil
	case int:
		return IntValue(int64(v)), nil
	case int32:
		return IntValue(int64(v)), nil
	case int64:
		return IntValue(v), nil
	case uint32:
		return IntValue(int64(v)), nil
	case uint64:
		return IntValue(int64(v)), nil
	case float32:
		return DoubleValue(float64(v)), nil
	case float64:
		return DoubleValue(v), nil
	case []byte:
		return BytesValue(v), nil
	case []any:
		out := make([]Value, len(v))
		for i, it := range v {
			cv, err := FromAny(it)
			if err != nil {
				return Value{}, err
			}
			out[i] = cv
		}
		return SliceValue(out), nil
	case map[string]any:
		out := make(map[string]Value, len(v))
		for k, it := range v {
			cv, err := FromAny(it)
			if err != nil {
				return Value{}, err
			}
			out[k] = cv
		}
		return MapValue(out), nil
	case Value:
		return v, nil
	default:
		return Value{}, fmt.Errorf("streamlang: cannot convert %T to Value", a)
	}
}

// Type returns the kind of value this Value carries.
func (v Value) Type() ValueType {
	if v.hasPV {
		switch v.pv.Type() {
		case pcommon.ValueTypeEmpty:
			return ValueTypeEmpty
		case pcommon.ValueTypeStr:
			return ValueTypeStr
		case pcommon.ValueTypeBool:
			return ValueTypeBool
		case pcommon.ValueTypeInt:
			return ValueTypeInt
		case pcommon.ValueTypeDouble:
			return ValueTypeDouble
		case pcommon.ValueTypeBytes:
			return ValueTypeBytes
		case pcommon.ValueTypeSlice:
			return ValueTypeSlice
		case pcommon.ValueTypeMap:
			return ValueTypeMap
		}
		return ValueTypeEmpty
	}
	return v.goType
}

// Str returns the value as a string. Numeric / bool values are stringified;
// for unsupported types returns "".
func (v Value) Str() string {
	if v.hasPV {
		// AsString is convenient and matches Rust's lossy behavior.
		return v.pv.AsString()
	}
	switch v.goType {
	case ValueTypeStr:
		return v.goVal.(string)
	case ValueTypeBool:
		if v.goVal.(bool) {
			return "true"
		}
		return "false"
	case ValueTypeInt:
		return fmt.Sprintf("%d", v.goVal.(int64))
	case ValueTypeDouble:
		return fmt.Sprintf("%g", v.goVal.(float64))
	}
	return ""
}

// Bool returns the value as a bool.
func (v Value) Bool() bool {
	if v.hasPV {
		if v.pv.Type() == pcommon.ValueTypeBool {
			return v.pv.Bool()
		}
		return false
	}
	if v.goType == ValueTypeBool {
		return v.goVal.(bool)
	}
	return false
}

// Int returns the value as an int64.
func (v Value) Int() int64 {
	if v.hasPV {
		switch v.pv.Type() {
		case pcommon.ValueTypeInt:
			return v.pv.Int()
		case pcommon.ValueTypeDouble:
			return int64(v.pv.Double())
		}
		return 0
	}
	switch v.goType {
	case ValueTypeInt:
		return v.goVal.(int64)
	case ValueTypeDouble:
		return int64(v.goVal.(float64))
	}
	return 0
}

// Double returns the value as a float64.
func (v Value) Double() float64 {
	if v.hasPV {
		switch v.pv.Type() {
		case pcommon.ValueTypeDouble:
			return v.pv.Double()
		case pcommon.ValueTypeInt:
			return float64(v.pv.Int())
		}
		return 0
	}
	switch v.goType {
	case ValueTypeDouble:
		return v.goVal.(float64)
	case ValueTypeInt:
		return float64(v.goVal.(int64))
	}
	return 0
}

// Bytes returns the value as a byte slice.
func (v Value) Bytes() []byte {
	if v.hasPV {
		if v.pv.Type() == pcommon.ValueTypeBytes {
			return v.pv.Bytes().AsRaw()
		}
		return nil
	}
	if v.goType == ValueTypeBytes {
		return v.goVal.([]byte)
	}
	return nil
}

// Slice returns the value as a []Value, copying out of pcommon when needed.
func (v Value) Slice() []Value {
	if v.hasPV {
		if v.pv.Type() != pcommon.ValueTypeSlice {
			return nil
		}
		s := v.pv.Slice()
		out := make([]Value, s.Len())
		for i := 0; i < s.Len(); i++ {
			out[i] = FromPcommon(s.At(i))
		}
		return out
	}
	if v.goType == ValueTypeSlice {
		return v.goVal.([]Value)
	}
	return nil
}

// Map returns the value as a map[string]Value.
func (v Value) Map() map[string]Value {
	if v.hasPV {
		if v.pv.Type() != pcommon.ValueTypeMap {
			return nil
		}
		m := v.pv.Map()
		out := make(map[string]Value, m.Len())
		m.Range(func(k string, mv pcommon.Value) bool {
			out[k] = FromPcommon(mv)
			return true
		})
		return out
	}
	if v.goType == ValueTypeMap {
		return v.goVal.(map[string]Value)
	}
	return nil
}

// AsAny returns the value as an arbitrary Go value, recursively converting
// nested pcommon storage. Convenient for tests / fall-through paths.
func (v Value) AsAny() any {
	if v.hasPV {
		return v.pv.AsRaw()
	}
	switch v.goType {
	case ValueTypeEmpty:
		return nil
	case ValueTypeStr:
		return v.goVal.(string)
	case ValueTypeBool:
		return v.goVal.(bool)
	case ValueTypeInt:
		return v.goVal.(int64)
	case ValueTypeDouble:
		return v.goVal.(float64)
	case ValueTypeBytes:
		return v.goVal.([]byte)
	case ValueTypeSlice:
		s := v.goVal.([]Value)
		out := make([]any, len(s))
		for i, it := range s {
			out[i] = it.AsAny()
		}
		return out
	case ValueTypeMap:
		m := v.goVal.(map[string]Value)
		out := make(map[string]any, len(m))
		for k, it := range m {
			out[k] = it.AsAny()
		}
		return out
	}
	return nil
}

// pcommonValue returns a fresh pcommon.Value populated from this Value.
// Used when writing into pcommon.Map fields.
func (v Value) pcommonValue() pcommon.Value {
	out := pcommon.NewValueEmpty()
	v.copyTo(out)
	return out
}

// copyTo writes the value into the supplied pcommon.Value, replacing its
// previous contents.
func (v Value) copyTo(dst pcommon.Value) {
	if v.hasPV {
		v.pv.CopyTo(dst)
		return
	}
	switch v.goType {
	case ValueTypeEmpty:
		// Best we can do — clear by setting an empty map then... actually
		// pcommon.Value has no "Empty" setter; leaving the slot as-is on a
		// fresh value is fine, since NewValueEmpty starts empty. For an
		// already-populated dst we re-init via SetEmptyMap then truncate;
		// in practice callers create fresh slots for empty values.
		dst.SetEmptyMap()
		dst.Map().Clear()
	case ValueTypeStr:
		dst.SetStr(v.goVal.(string))
	case ValueTypeBool:
		dst.SetBool(v.goVal.(bool))
	case ValueTypeInt:
		dst.SetInt(v.goVal.(int64))
	case ValueTypeDouble:
		dst.SetDouble(v.goVal.(float64))
	case ValueTypeBytes:
		dst.SetEmptyBytes().FromRaw(v.goVal.([]byte))
	case ValueTypeSlice:
		s := dst.SetEmptySlice()
		for _, it := range v.goVal.([]Value) {
			child := s.AppendEmpty()
			it.copyTo(child)
		}
	case ValueTypeMap:
		m := dst.SetEmptyMap()
		for k, it := range v.goVal.(map[string]Value) {
			child := m.PutEmpty(k)
			it.copyTo(child)
		}
	}
}
