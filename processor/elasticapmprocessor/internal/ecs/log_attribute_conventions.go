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
	"strings"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/datastream"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv26 "go.opentelemetry.io/otel/semconv/v1.26.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
)

// ApplyOTLPLogAttributeConventions applies OTLP log attribute handling used in ECS flow.
// Unsupported OTLP log attributes are moved into labels.* / numeric_labels.* with
// de-dotted keys. Attributes whose value type cannot be represented as a label
// (Map, Bytes, Empty) are removed without replacement, matching the behaviour of
// apm-data's setLabel (input/otlp/metadata.go).
func ApplyOTLPLogAttributeConventions(attributes pcommon.Map) {
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
		if shouldKeepLogAttribute(e.key) {
			continue
		}
		setLabelAttributeValue(attributes, replaceDots(e.key), e.value)
		attributes.Remove(e.key)
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

// ApplyScopeDataStreamConventions sanitizes data_stream dataset/namespace on
// log record attributes. If a value is already present on the record it is
// sanitized in place; otherwise a sanitized copy is taken from the scope.
// This is the single sanitization point for user-provided data_stream values
// — downstream routing functions (EncodeErrorDataStream, etc.) only set
// known-safe hardcoded values.
func ApplyScopeDataStreamConventions(scopeAttributes, logAttributes pcommon.Map) {
	if dataset, exists := logAttributes.Get("data_stream.dataset"); exists {
		if dataset.Type() == pcommon.ValueTypeStr {
			logAttributes.PutStr("data_stream.dataset", datastream.SanitizeDataset(dataset.Str()))
		}
	} else if dataset, ok := scopeAttributes.Get("data_stream.dataset"); ok && dataset.Type() == pcommon.ValueTypeStr {
		logAttributes.PutStr("data_stream.dataset", datastream.SanitizeDataset(dataset.Str()))
	}
	if namespace, exists := logAttributes.Get("data_stream.namespace"); exists {
		if namespace.Type() == pcommon.ValueTypeStr {
			logAttributes.PutStr("data_stream.namespace", datastream.SanitizeNamespace(namespace.Str()))
		}
	} else if namespace, ok := scopeAttributes.Get("data_stream.namespace"); ok && namespace.Type() == pcommon.ValueTypeStr {
		logAttributes.PutStr("data_stream.namespace", datastream.SanitizeNamespace(namespace.Str()))
	}
}
