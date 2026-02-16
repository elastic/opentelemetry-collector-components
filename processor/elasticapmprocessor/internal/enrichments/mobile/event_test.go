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

package mobile

import (
	"testing"
	"time"

	"maps"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestEnrichEvents(t *testing.T) {
	now := time.Unix(3600, 0)
	timestamp := pcommon.NewTimestampFromTime(now)
	javaStacktrace := "Exception in thread \"main\" java.lang.RuntimeException: Test exception\n at com.example.GenerateTrace.methodB(GenerateTrace.java:13)\n at com.example.GenerateTrace.methodA(GenerateTrace.java:9)\n at com.example.GenerateTrace.main(GenerateTrace.java:5)"
	javaStacktraceHash := "e25c4196dc720d91"

	swiftStacktrace := readSwiftStacktraceFile(t, "thread-8-crash.txt")
	swiftStacktraceHash := "e737b0da1c8f9d5a"

	for _, tc := range []struct {
		name               string
		eventName          string
		input              func() plog.LogRecord
		resourceAttrs      map[string]any
		expectedAttributes map[string]any
	}{
		{
			name:      "crash_event_java",
			eventName: "device.crash",
			resourceAttrs: map[string]any{
				"telemetry.sdk.language": "java",
			},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.SetTimestamp(timestamp)
				logRecord.Attributes().PutStr("event.name", "device.crash")
				logRecord.Attributes().PutStr("exception.message", "Exception message")
				logRecord.Attributes().PutStr("exception.type", "java.lang.RuntimeException")
				logRecord.Attributes().PutStr("exception.stacktrace", javaStacktrace)
				return logRecord
			},
			expectedAttributes: map[string]any{
				"processor.event":    "error",
				"timestamp.us":       timestamp.AsTime().UnixMicro(),
				"error.grouping_key": javaStacktraceHash,
				"error.type":         "crash",
				"event.kind":         "event",
			},
		},
		{
			name:      "crash_event_without_timestamp_java",
			eventName: "device.crash",
			resourceAttrs: map[string]any{
				"telemetry.sdk.language": "java",
			},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.SetObservedTimestamp(timestamp)
				logRecord.Attributes().PutStr("event.name", "device.crash")
				logRecord.Attributes().PutStr("exception.message", "Exception message")
				logRecord.Attributes().PutStr("exception.type", "java.lang.RuntimeException")
				logRecord.Attributes().PutStr("exception.stacktrace", javaStacktrace)
				return logRecord
			},
			expectedAttributes: map[string]any{
				"processor.event":    "error",
				"timestamp.us":       timestamp.AsTime().UnixMicro(),
				"error.grouping_key": javaStacktraceHash,
				"error.type":         "crash",
				"event.kind":         "event",
			},
		},
		{
			name:      "crash_event_non_java",
			eventName: "device.crash",
			resourceAttrs: map[string]any{
				"telemetry.sdk.language": "go",
			},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.SetTimestamp(timestamp)
				logRecord.Attributes().PutStr("event.name", "device.crash")
				logRecord.Attributes().PutStr("exception.message", "Exception message")
				logRecord.Attributes().PutStr("exception.type", "go.error")
				logRecord.Attributes().PutStr("exception.stacktrace", javaStacktrace)
				return logRecord
			},
			expectedAttributes: map[string]any{
				"processor.event": "error",
				"timestamp.us":    timestamp.AsTime().UnixMicro(),
				"error.type":      "crash",
				"event.kind":      "event",
			},
		},
		{
			name:          "non_crash_event",
			eventName:     "othername",
			resourceAttrs: map[string]any{},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.Attributes().PutStr("event.name", "othername")
				return logRecord
			},
			expectedAttributes: map[string]any{
				"event.kind": "event",
			},
		},
		{
			name:      "crash_event_swift",
			eventName: "device.crash",
			resourceAttrs: map[string]any{
				"telemetry.sdk.language": "swift",
			},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.SetTimestamp(timestamp)
				logRecord.Attributes().PutStr("event.name", "device.crash")
				logRecord.Attributes().PutStr("exception.type", "SIGTRAP")
				logRecord.Attributes().PutStr("exception.stacktrace", swiftStacktrace)
				return logRecord
			},
			expectedAttributes: map[string]any{
				"processor.event":    "error",
				"timestamp.us":       timestamp.AsTime().UnixMicro(),
				"error.grouping_key": swiftStacktraceHash,
				"error.type":         "crash",
				"event.kind":         "event",
			},
		},
		{
			name:      "crash_event_all_attributes_present",
			eventName: "device.crash",
			resourceAttrs: map[string]any{
				"telemetry.sdk.language": "java",
			},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.SetTimestamp(timestamp)
				logRecord.Attributes().PutStr("event.name", "device.crash")
				logRecord.Attributes().PutStr("exception.stacktrace", javaStacktrace)
				// Set all attributes that enrichment would normally set
				logRecord.Attributes().PutStr("event.kind", "existing-event-kind")
				logRecord.Attributes().PutStr("processor.event", "existing-processor-event")
				logRecord.Attributes().PutInt("timestamp.us", int64(99999))
				logRecord.Attributes().PutStr("error.id", "0123456789abcdef0123456789abcdef")
				logRecord.Attributes().PutStr("error.type", "existing-error-type")
				logRecord.Attributes().PutStr("error.grouping_key", "existing-grouping-key")
				return logRecord
			},
			expectedAttributes: map[string]any{
				"event.name":           "device.crash",
				"exception.stacktrace": javaStacktrace,
				// existing attributes that are not overridden
				"event.kind":         "existing-event-kind",
				"processor.event":    "existing-processor-event",
				"timestamp.us":       int64(99999),
				"error.id":           "0123456789abcdef0123456789abcdef",
				"error.type":         "existing-error-type",
				"error.grouping_key": "existing-grouping-key",
			},
		},
		{
			name:      "crash_event_some_attributes_missing",
			eventName: "device.crash",
			resourceAttrs: map[string]any{
				"telemetry.sdk.language": "java",
			},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.SetTimestamp(timestamp)
				logRecord.Attributes().PutStr("event.name", "device.crash")
				logRecord.Attributes().PutStr("exception.stacktrace", javaStacktrace)
				logRecord.Attributes().PutStr("event.kind", "existing-event-kind")
				logRecord.Attributes().PutStr("processor.event", "existing-processor-event")
				// timestamp.us, error.id, error.type, and error.grouping_key are missing
				return logRecord
			},
			expectedAttributes: map[string]any{
				// Input attributes
				"event.name":           "device.crash",
				"exception.stacktrace": javaStacktrace,
				// existing attributes that are not overridden
				"event.kind":      "existing-event-kind",
				"processor.event": "existing-processor-event",
				// attributes that are added by enrichment
				"timestamp.us":       timestamp.AsTime().UnixMicro(),
				"error.grouping_key": javaStacktraceHash,
				"error.type":         "crash",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inputLogRecord := tc.input()

			maps.Copy(tc.expectedAttributes, inputLogRecord.Attributes().AsRaw())

			ctx := EventContext{
				ResourceAttributes: tc.resourceAttrs,
				EventName:          tc.eventName,
			}
			EnrichLogEvent(ctx, inputLogRecord)

			assert.Empty(t, cmp.Diff(inputLogRecord.Attributes().AsRaw(), tc.expectedAttributes, ignoreMapKey("error.id")))
			errorId, ok := inputLogRecord.Attributes().Get("error.id")
			if ok {
				assert.Equal(t, "device.crash", tc.eventName)
				assert.Equal(t, 32, len(errorId.AsString()))
			} else {
				assert.NotEqual(t, "device.crash", tc.eventName)
			}
		})
	}
}

func ignoreMapKey(k string) cmp.Option {
	return cmp.FilterPath(func(p cmp.Path) bool {
		mapIndex, ok := p.Last().(cmp.MapIndex)
		if !ok {
			return false
		}
		return mapIndex.Key().String() == k
	}, cmp.Ignore())
}
