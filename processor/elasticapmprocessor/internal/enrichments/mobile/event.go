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

package mobile // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/mobile"

import (
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/attribute"
)

// EventContext contains contextual information for log event enrichment
type EventContext struct {
	ResourceAttributes map[string]any
	EventName          string
}

func EnrichLogEvent(ctx EventContext, logRecord plog.LogRecord) {
	attribute.PutStr(logRecord.Attributes(), elasticattr.EventKind, "event")

	if ctx.EventName == "device.crash" {
		enrichCrashEvent(logRecord, ctx.ResourceAttributes)
	}
}

func enrichCrashEvent(logRecord plog.LogRecord, resourceAttrs map[string]any) {
	timestamp := logRecord.Timestamp()
	if timestamp == 0 {
		timestamp = logRecord.ObservedTimestamp()
	}
	attribute.PutStr(logRecord.Attributes(), elasticattr.ProcessorEvent, "error")
	attribute.PutInt(logRecord.Attributes(), elasticattr.TimestampUs, attribute.ToTimestampUS(timestamp))
	if id, err := attribute.NewErrorID(); err == nil {
		attribute.PutStr(logRecord.Attributes(), elasticattr.ErrorID, id)
	}
	stacktrace, ok := logRecord.Attributes().Get("exception.stacktrace")
	if ok {
		language, hasLanguage := resourceAttrs["telemetry.sdk.language"]
		if hasLanguage {
			switch language {
			case "java":
				attribute.PutStr(logRecord.Attributes(), elasticattr.ErrorGroupingKey, CreateJavaStacktraceGroupingKey(stacktrace.AsString()))
			case "swift":
				if key, err := CreateSwiftStacktraceGroupingKey(stacktrace.AsString()); err == nil {
					attribute.PutStr(logRecord.Attributes(), elasticattr.ErrorGroupingKey, key)
				}
			}
		}
	}
	attribute.PutStr(logRecord.Attributes(), elasticattr.ErrorType, "crash")
}
