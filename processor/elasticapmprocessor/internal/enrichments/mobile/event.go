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
	"strings"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/attribute"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
)

// EventContext contains contextual information for log event enrichment
type EventContext struct {
	ResourceAttributes map[string]any
	EventName          string
	EventDomain        string
	IsDeviceEvent      bool
	Action             string
}

// NewEventContext builds context for log enrichment. resourceAttrs must be the OTLP resource
// attributes (e.g. telemetry.sdk.language); enrichCrashEvent uses them for language-specific
// grouping keys. logRecord supplies event name and domain from log attributes.
func NewEventContext(resourceAttrs map[string]any, logRecord plog.LogRecord) EventContext {
	eventName, ok := getEventName(logRecord)
	ctx := EventContext{
		ResourceAttributes: resourceAttrs,
	}
	if ok {
		ctx.EventName = eventName
	}
	domainAttr, ok := logRecord.Attributes().Get("event.domain")
	if ok {
		ctx.EventDomain = domainAttr.AsString()
	}

	if isDeviceEvent(logRecord, eventName) {
		ctx.IsDeviceEvent = true
	}

	action := strings.TrimPrefix(eventName, "device.")
	ctx.Action = action

	return ctx
}

func EnrichLogEvent(ctx EventContext, logRecord plog.LogRecord, cfg config.Config) {

	if cfg.Log.EventConfig.EventKind.Enabled {
		attribute.PutStr(logRecord.Attributes(), elasticattr.EventKind, "event")
	}

	if isDeviceEvent(logRecord, ctx.EventName) {
		if cfg.Log.EventConfig.EventCategory.Enabled {
			attribute.PutStr(logRecord.Attributes(), elasticattr.EventCategory, "device")
		}
		action := strings.TrimPrefix(ctx.EventName, "device.")
		if action == "crash" {
			enrichCrashEvent(logRecord, ctx.ResourceAttributes, cfg)
		} else if action != "" && cfg.Log.EventConfig.EventAction.Enabled {
			attribute.PutStr(logRecord.Attributes(), elasticattr.EventAction, action)
		}
	}
}

// getEventName returns the event name from the log record.
// If the event name is not set, it returns an empty string.
func getEventName(logRecord plog.LogRecord) (string, bool) {
	if logRecord.EventName() != "" {
		return logRecord.EventName(), true
	}
	attributeValue, ok := logRecord.Attributes().Get("event.name")
	if ok {
		return attributeValue.AsString(), true
	}
	return "", false
}

func isDeviceEvent(logRecord plog.LogRecord, eventName string) bool {
	domainAttr, ok := logRecord.Attributes().Get("event.domain")
	eventDomain := ""
	if ok {
		eventDomain = domainAttr.AsString()
	}
	return eventDomain == "device" && eventName != "" || strings.HasPrefix(eventName, "device.")
}

func enrichCrashEvent(logRecord plog.LogRecord, resourceAttrs map[string]any, cfg config.Config) {
	timestamp := logRecord.Timestamp()
	if timestamp == 0 {
		timestamp = logRecord.ObservedTimestamp()
	}
	attribute.PutInt(logRecord.Attributes(), elasticattr.TimestampUs, attribute.ToTimestampUS(timestamp))
	if id, err := attribute.NewErrorID(); err == nil {
		attribute.PutStr(logRecord.Attributes(), elasticattr.ErrorID, id)
	}
	stacktrace, ok := logRecord.Attributes().Get("exception.stacktrace")
	if ok && cfg.Log.ErrorConfig.ErrorGroupingKey.Enabled {
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
	if cfg.Log.ErrorConfig.ErrorType.Enabled {
		attribute.PutStr(logRecord.Attributes(), elasticattr.ErrorType, "crash")
	}

	if cfg.Log.EventConfig.EventType.Enabled {
		attribute.PutStr(logRecord.Attributes(), elasticattr.EventType, "error")
	}
}
