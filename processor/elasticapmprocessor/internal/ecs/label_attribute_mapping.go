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
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// moveUnsupportedToLabels moves attributes that don't pass the keep
// predicate into labels.* / numeric_labels.* with sanitized keys.
// Attributes whose value type is not representable as a label (Map,
// Bytes, Empty) are removed without replacement.
func moveUnsupportedToLabels(attributes pcommon.Map, keep func(string) bool) {
	type entry struct {
		key   string
		value pcommon.Value
	}
	snapshot := make([]entry, 0, attributes.Len())
	attributes.Range(func(k string, v pcommon.Value) bool {
		snapshot = append(snapshot, entry{k, v})
		return true
	})
	for _, e := range snapshot {
		if keep(e.key) {
			continue
		}
		setLabelAttributeValue(attributes, replaceDots(e.key), e.value)
		attributes.Remove(e.key)
	}
}

// setLabelAttributeValue maps a value into labels.* / numeric_labels.*.
// It returns true if the value was stored, false if the type is not
// representable as a label. Elasticsearch label mappings only support
// flat scalar values and homogeneous arrays thereof; Map, Bytes, and
// Empty types cannot be stored and are intentionally dropped. This
// matches the behaviour of apm-data's setLabel (input/otlp/metadata.go)
// which also silently ignores these types.
func setLabelAttributeValue(attributes pcommon.Map, key string, value pcommon.Value) bool {
	switch value.Type() {
	case pcommon.ValueTypeStr:
		attributes.PutStr("labels."+key, truncate(value.Str()))
	case pcommon.ValueTypeBool:
		attributes.PutStr("labels."+key, strconv.FormatBool(value.Bool()))
	case pcommon.ValueTypeInt:
		attributes.PutDouble("numeric_labels."+key, float64(value.Int()))
	case pcommon.ValueTypeDouble:
		attributes.PutDouble("numeric_labels."+key, value.Double())
	case pcommon.ValueTypeSlice:
		slice := value.Slice()
		if slice.Len() == 0 {
			return false
		}
		switch slice.At(0).Type() {
		case pcommon.ValueTypeStr:
			target := attributes.PutEmptySlice("labels." + key)
			for i := 0; i < slice.Len(); i++ {
				item := slice.At(i)
				if item.Type() == pcommon.ValueTypeStr {
					target.AppendEmpty().SetStr(truncate(item.Str()))
				}
			}
		case pcommon.ValueTypeBool:
			target := attributes.PutEmptySlice("labels." + key)
			for i := 0; i < slice.Len(); i++ {
				item := slice.At(i)
				if item.Type() == pcommon.ValueTypeBool {
					target.AppendEmpty().SetStr(strconv.FormatBool(item.Bool()))
				}
			}
		case pcommon.ValueTypeDouble:
			target := attributes.PutEmptySlice("numeric_labels." + key)
			for i := 0; i < slice.Len(); i++ {
				item := slice.At(i)
				if item.Type() == pcommon.ValueTypeDouble {
					target.AppendEmpty().SetDouble(item.Double())
				}
			}
		case pcommon.ValueTypeInt:
			target := attributes.PutEmptySlice("numeric_labels." + key)
			for i := 0; i < slice.Len(); i++ {
				item := slice.At(i)
				if item.Type() == pcommon.ValueTypeInt {
					target.AppendEmpty().SetDouble(float64(item.Int()))
				}
			}
		default:
			return false
		}
	case pcommon.ValueTypeMap, pcommon.ValueTypeBytes, pcommon.ValueTypeEmpty:
		return false
	default:
		return false
	}
	return true
}
