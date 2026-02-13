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

package ecs // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/ecs"

import (
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv26 "go.opentelemetry.io/otel/semconv/v1.26.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
)

// TranslateOTLPLogAttributesForMIS applies MIS-compatible OTLP log attribute handling.
// Unsupported OTLP log attributes are moved into labels.* / numeric_labels.* with de-dotted keys.
func TranslateOTLPLogAttributesForMIS(attributes pcommon.Map) {
	keys := make([]string, 0, attributes.Len())
	attributes.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})

	for _, key := range keys {
		if shouldKeepLogAttribute(key) {
			continue
		}
		value, ok := attributes.Get(key)
		if !ok {
			continue
		}
		setLabelAttributeValue(attributes, replaceDots(key), value)
		attributes.Remove(key)
	}
}

func shouldKeepLogAttribute(attr string) bool {
	if strings.HasPrefix(attr, "labels.") || strings.HasPrefix(attr, "numeric_labels.") {
		return true
	}
	if strings.HasPrefix(attr, "elasticsearch.") {
		return true
	}

	switch attr {
	case elasticattr.ProcessorEvent,
		string(semconv.ExceptionMessageKey),
		string(semconv.ExceptionStacktraceKey),
		string(semconv.ExceptionTypeKey),
		string(semconv26.ExceptionEscapedKey),
		"event.name",
		"event.domain",
		"session.id",
		string(semconv.NetworkConnectionTypeKey),
		"data_stream.type",
		"data_stream.dataset",
		"data_stream.namespace":
		return true
	}

	return false
}

func setLabelAttributeValue(attributes pcommon.Map, key string, value pcommon.Value) {
	switch value.Type() {
	case pcommon.ValueTypeStr:
		attributes.PutStr("labels."+key, value.Str())
	case pcommon.ValueTypeBool:
		attributes.PutStr("labels."+key, strconv.FormatBool(value.Bool()))
	case pcommon.ValueTypeInt:
		attributes.PutDouble("numeric_labels."+key, float64(value.Int()))
	case pcommon.ValueTypeDouble:
		attributes.PutDouble("numeric_labels."+key, value.Double())
	case pcommon.ValueTypeSlice:
		slice := value.Slice()
		if slice.Len() == 0 {
			return
		}
		switch slice.At(0).Type() {
		case pcommon.ValueTypeStr:
			target := attributes.PutEmptySlice("labels." + key)
			for i := 0; i < slice.Len(); i++ {
				item := slice.At(i)
				if item.Type() == pcommon.ValueTypeStr {
					target.AppendEmpty().SetStr(item.Str())
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
		}
	}
}
