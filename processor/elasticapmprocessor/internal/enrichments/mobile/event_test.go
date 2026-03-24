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

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
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
				"timestamp.us":       timestamp.AsTime().UnixMicro(),
				"error.grouping_key": javaStacktraceHash,
				"error.type":       "crash",
				"event.kind":       "event",
				"event.category":   "device",
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
				"timestamp.us":       timestamp.AsTime().UnixMicro(),
				"error.grouping_key": javaStacktraceHash,
				"error.type":       "crash",
				"event.kind":       "event",
				"event.category":   "device",
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
				"timestamp.us":   timestamp.AsTime().UnixMicro(),
				"error.type":     "crash",
				"event.kind":     "event",
				"event.category": "device",
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
			name:          "device_event_via_domain_and_name_without_prefix",
			eventName:     "lifecycle",
			resourceAttrs: map[string]any{},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.Attributes().PutStr("event.domain", "device")
				logRecord.Attributes().PutStr("event.name", "lifecycle")
				return logRecord
			},
			expectedAttributes: map[string]any{
				"event.kind":     "event",
				"event.category": "device",
				"event.action":   "lifecycle",
			},
		},
		{
			name:          "device_event_non_crash_action",
			eventName:     "device.lifecycle",
			resourceAttrs: map[string]any{},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.Attributes().PutStr("event.name", "device.lifecycle")
				return logRecord
			},
			expectedAttributes: map[string]any{
				"event.kind":     "event",
				"event.category": "device",
				"event.action":   "lifecycle",
			},
		},
		{
			name:          "not_device_event_domain_not_device",
			eventName:     "click",
			resourceAttrs: map[string]any{},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.Attributes().PutStr("event.domain", "user")
				logRecord.Attributes().PutStr("event.name", "click")
				return logRecord
			},
			expectedAttributes: map[string]any{
				"event.kind": "event",
			},
		},
		{
			name:          "device_domain_empty_event_name_not_treated_as_device",
			eventName:     "",
			resourceAttrs: map[string]any{},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.Attributes().PutStr("event.domain", "device")
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
				"timestamp.us":       timestamp.AsTime().UnixMicro(),
				"error.grouping_key": swiftStacktraceHash,
				"error.type":       "crash",
				"event.kind":       "event",
				"event.category":   "device",
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
				"event.category":     "device",
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
				"error.type":       "crash",
				"event.category":   "device",
			},
		},
		{
			name:          "device_event_missing_domain_non_prefix_name",
			eventName:     "lifecycle",
			resourceAttrs: map[string]any{},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.Attributes().PutStr("event.name", "lifecycle")
				return logRecord
			},
			expectedAttributes: map[string]any{
				"event.kind": "event",
			},
		},
		{
			name:          "device_event_empty_action",
			eventName:     "device.",
			resourceAttrs: map[string]any{},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.Attributes().PutStr("event.name", "device.")
				return logRecord
			},
			// apm-data convention: omit event.action when empty (see putNonEmptyStr in intake mapper)
			expectedAttributes: map[string]any{
				"event.kind":     "event",
				"event.category": "device",
			},
		},
		{
			name:          "device_event_domain_and_prefix_both_set",
			eventName:     "device.lifecycle",
			resourceAttrs: map[string]any{},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.Attributes().PutStr("event.domain", "device")
				logRecord.Attributes().PutStr("event.name", "device.lifecycle")
				return logRecord
			},
			expectedAttributes: map[string]any{
				"event.kind":     "event",
				"event.category": "device",
				"event.action":   "lifecycle",
			},
		},
		{
			name:          "device_event_non_device_domain_with_prefix",
			eventName:     "device.lifecycle",
			resourceAttrs: map[string]any{},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.Attributes().PutStr("event.domain", "user")
				logRecord.Attributes().PutStr("event.name", "device.lifecycle")
				return logRecord
			},
			expectedAttributes: map[string]any{
				"event.kind":     "event",
				"event.category": "device",
				"event.action":   "lifecycle",
			},
		},
		{
			name:      "crash_event_without_stacktrace",
			eventName: "device.crash",
			resourceAttrs: map[string]any{
				"telemetry.sdk.language": "java",
			},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.SetTimestamp(timestamp)
				logRecord.Attributes().PutStr("event.name", "device.crash")
				logRecord.Attributes().PutStr("exception.message", "Fatal error")
				return logRecord
			},
			expectedAttributes: map[string]any{
				"timestamp.us":   timestamp.AsTime().UnixMicro(),
				"error.type":     "crash",
				"event.kind":     "event",
				"event.category": "device",
			},
		},
		{
			name:      "crash_event_swift_invalid_stacktrace",
			eventName: "device.crash",
			resourceAttrs: map[string]any{
				"telemetry.sdk.language": "swift",
			},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.SetTimestamp(timestamp)
				logRecord.Attributes().PutStr("event.name", "device.crash")
				logRecord.Attributes().PutStr("exception.stacktrace", "invalid stacktrace without Thread N Crashed:")
				return logRecord
			},
			expectedAttributes: map[string]any{
				"timestamp.us":   timestamp.AsTime().UnixMicro(),
				"error.type":     "crash",
				"event.kind":     "event",
				"event.category": "device",
			},
		},
		{
			name:          "crash_event_no_language_key",
			eventName:     "device.crash",
			resourceAttrs: map[string]any{},
			input: func() plog.LogRecord {
				logRecord := plog.NewLogRecord()
				logRecord.SetTimestamp(timestamp)
				logRecord.Attributes().PutStr("event.name", "device.crash")
				logRecord.Attributes().PutStr("exception.stacktrace", javaStacktrace)
				return logRecord
			},
			expectedAttributes: map[string]any{
				"timestamp.us":   timestamp.AsTime().UnixMicro(),
				"error.type":     "crash",
				"event.kind":     "event",
				"event.category": "device",
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
			EnrichLogEvent(ctx, inputLogRecord, config.Enabled())

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

func TestEnrichLogEvent_ConfigDisabled(t *testing.T) {
	now := time.Unix(3600, 0)
	timestamp := pcommon.NewTimestampFromTime(now)
	javaStacktrace := "Exception in thread \"main\" java.lang.RuntimeException: Test\n at com.example.Main.crash(Main.java:10)"

	for _, tc := range []struct {
		name          string
		eventName     string
		setupConfig   func() config.Config
		inputAttrs    map[string]string
		resourceAttrs map[string]any
		wantPresent   []string // attributes that must be set after enrichment
		wantAbsent    []string // attributes that must NOT be set when config is disabled
	}{
		{
			name:          "event_kind_disabled",
			eventName:     "device.crash",
			setupConfig:   func() config.Config { c := config.Enabled(); c.Log.EventConfig.EventKind.Enabled = false; return c },
			inputAttrs:    map[string]string{"event.name": "device.crash"},
			resourceAttrs: map[string]any{},
			wantPresent:   []string{"event.category", "error.type", "timestamp.us"},
			wantAbsent:    []string{"event.kind"},
		},
		{
			name:          "event_category_disabled",
			eventName:     "device.crash",
			setupConfig:   func() config.Config { c := config.Enabled(); c.Log.EventConfig.EventCategory.Enabled = false; return c },
			inputAttrs:    map[string]string{"event.name": "device.crash"},
			resourceAttrs: map[string]any{},
			wantPresent:   []string{"event.kind", "error.type", "timestamp.us"},
			wantAbsent:    []string{"event.category"},
		},
		{
			name:          "event_action_disabled",
			eventName:     "device.lifecycle",
			setupConfig:   func() config.Config { c := config.Enabled(); c.Log.EventConfig.EventAction.Enabled = false; return c },
			inputAttrs:    map[string]string{"event.name": "device.lifecycle"},
			resourceAttrs: map[string]any{},
			wantPresent:   []string{"event.kind", "event.category"},
			wantAbsent:    []string{"event.action"},
		},
		{
			name:      "error_grouping_key_disabled",
			eventName: "device.crash",
			setupConfig: func() config.Config {
				c := config.Enabled()
				c.Log.ErrorConfig.ErrorGroupingKey.Enabled = false
				return c
			},
			inputAttrs: map[string]string{
				"event.name":           "device.crash",
				"exception.stacktrace": javaStacktrace,
			},
			resourceAttrs: map[string]any{"telemetry.sdk.language": "java"},
			wantPresent:   []string{"event.kind", "event.category", "error.type", "timestamp.us"},
			wantAbsent:    []string{"error.grouping_key"},
		},
		{
			name:          "error_type_disabled",
			eventName:     "device.crash",
			setupConfig:   func() config.Config { c := config.Enabled(); c.Log.ErrorConfig.ErrorType.Enabled = false; return c },
			inputAttrs:    map[string]string{"event.name": "device.crash"},
			resourceAttrs: map[string]any{},
			wantPresent:   []string{"event.kind", "event.category", "timestamp.us"},
			wantAbsent:    []string{"error.type"},
		},
		{
			name:      "all_event_and_error_attrs_disabled",
			eventName: "device.crash",
			setupConfig: func() config.Config {
				c := config.Enabled()
				c.Log.EventConfig.EventKind.Enabled = false
				c.Log.EventConfig.EventCategory.Enabled = false
				c.Log.ErrorConfig.ErrorGroupingKey.Enabled = false
				c.Log.ErrorConfig.ErrorType.Enabled = false
				return c
			},
			inputAttrs: map[string]string{
				"event.name":           "device.crash",
				"exception.stacktrace": javaStacktrace,
			},
			resourceAttrs: map[string]any{"telemetry.sdk.language": "java"},
			wantPresent:   []string{"timestamp.us", "error.id"},
			wantAbsent:    []string{"event.kind", "event.category", "error.type", "error.grouping_key"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logRecord := plog.NewLogRecord()
			logRecord.SetTimestamp(timestamp)
			for k, v := range tc.inputAttrs {
				logRecord.Attributes().PutStr(k, v)
			}
			ctx := EventContext{
				ResourceAttributes: tc.resourceAttrs,
				EventName:          tc.eventName,
			}
			EnrichLogEvent(ctx, logRecord, tc.setupConfig())
			attrs := logRecord.Attributes().AsRaw()
			for _, key := range tc.wantPresent {
				_, ok := attrs[key]
				assert.True(t, ok, "expected attribute %q to be set", key)
			}
			for _, key := range tc.wantAbsent {
				_, ok := attrs[key]
				assert.False(t, ok, "expected attribute %q to NOT be set when config disabled", key)
			}
		})
	}
}

func TestIsDeviceEvent(t *testing.T) {
	for _, tc := range []struct {
		name       string
		eventName  string
		logAttrs   map[string]string
		wantDevice bool
	}{
		{
			name:       "domain_device_non_empty_event_name",
			eventName:  "lifecycle",
			logAttrs:   map[string]string{"event.domain": "device"},
			wantDevice: true,
		},
		{
			name:       "domain_device_empty_event_name",
			eventName:  "",
			logAttrs:   map[string]string{"event.domain": "device"},
			wantDevice: false,
		},
		{
			name:       "domain_user_event_name_has_device_prefix",
			eventName:  "device.lifecycle",
			logAttrs:   map[string]string{"event.domain": "user"},
			wantDevice: true,
		},
		{
			name:       "no_domain_event_name_has_device_prefix",
			eventName:  "device.crash",
			logAttrs:   map[string]string{},
			wantDevice: true,
		},
		{
			name:       "no_domain_event_name_no_prefix",
			eventName:  "lifecycle",
			logAttrs:   map[string]string{},
			wantDevice: false,
		},
		{
			name:       "domain_device_event_name_has_prefix",
			eventName:  "device.lifecycle",
			logAttrs:   map[string]string{"event.domain": "device"},
			wantDevice: true,
		},
		{
			name:       "event_name_exactly_device_prefix",
			eventName:  "device.",
			logAttrs:   map[string]string{},
			wantDevice: true,
		},
		{
			name:       "domain_not_device_event_name_no_prefix",
			eventName:  "click",
			logAttrs:   map[string]string{"event.domain": "user"},
			wantDevice: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logRecord := plog.NewLogRecord()
			for k, v := range tc.logAttrs {
				logRecord.Attributes().PutStr(k, v)
			}
			got := isDeviceEvent(logRecord, tc.eventName)
			assert.Equal(t, tc.wantDevice, got)
		})
	}
}
