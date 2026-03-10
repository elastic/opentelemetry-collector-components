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

package enrichments // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
)

type errorEventContext struct {
	exceptionType       string
	exceptionMessage    string
	exceptionStacktrace string
	exceptionEscaped    bool
}

// getErrorEventContext extracts relevant attributes from an error event.
func getErrorEventContext(attributes pcommon.Map) errorEventContext {
	ec := errorEventContext{}

	for k, v := range attributes.All() {
		switch k {
		case string(semconv.ExceptionTypeKey):
			ec.exceptionType = v.Str()
		case string(semconv.ExceptionMessageKey):
			ec.exceptionMessage = v.Str()
		case string(semconv.ExceptionStacktraceKey):
			ec.exceptionStacktrace = v.Str()
		case string(semconv.ExceptionEscapedKey):
			ec.exceptionEscaped = v.Bool()
		}
	}
	return ec
}
