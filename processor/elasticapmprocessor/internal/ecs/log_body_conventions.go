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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// ApplyOTLPLogBodyConventions applies OTLP body handling for ECS log flow.
// Non-empty non-string bodies are converted to their string representation so ECS encoding
// can emit message. For map bodies, scalar-typed entries are extracted to
// labels.* / numeric_labels.*; nested map or bytes entries within the body
// are intentionally not stored as labels (they have no flat representation)
// and are only preserved in the stringified body message. This matches
// apm-data behaviour (input/otlp/logs.go).
func ApplyOTLPLogBodyConventions(logRecord plog.LogRecord) {
	body := logRecord.Body()
	if body.Type() == pcommon.ValueTypeEmpty {
		return
	}

	message := body.AsString()
	if body.Type() == pcommon.ValueTypeMap {
		body.Map().Range(func(k string, v pcommon.Value) bool {
			setLabelAttributeValue(logRecord.Attributes(), replaceDots(k), v)
			return true
		})
	}
	if body.Type() != pcommon.ValueTypeStr {
		logRecord.Body().SetStr(message)
	}
}
