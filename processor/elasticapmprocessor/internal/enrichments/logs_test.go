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

package enrichments

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
)

func TestEnrichResourceLog(t *testing.T) {
	traceFile := filepath.Join("testdata", "logs.yaml")
	logs, err := golden.ReadLogs(traceFile)
	require.NoError(t, err)
	resourceLogs := logs.ResourceLogs().At(0)
	logRecords := resourceLogs.ScopeLogs().At(0).LogRecords()

	// This is needed because the yaml unmarshalling is not yet aware of this new field
	logRecords.At(2).SetEventName("field.name")

	enricher := NewEnricher(config.Enabled())
	enricher.EnrichLogs(logs)

	t.Run("resource_enrichment", func(t *testing.T) {
		resourceAttributes := resourceLogs.Resource().Attributes()
		expectedResourceAttributes := map[string]any{
			"service.name":           "my.service",
			"agent.name":             "android/java",
			"agent.version":          "unknown",
			"telemetry.sdk.name":     "android",
			"telemetry.sdk.language": "java",
		}
		assert.Empty(t, cmp.Diff(resourceAttributes.AsRaw(), expectedResourceAttributes))
	})

	for i, tc := range []struct {
		name             string
		processedAsEvent bool
	}{
		{
			name:             "regular_log",
			processedAsEvent: false,
		},
		{
			name:             "event_by_attribute",
			processedAsEvent: true,
		},
		{
			name:             "event_by_field",
			processedAsEvent: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventKind, ok := logRecords.At(i).Attributes().Get("event.kind")
			if ok {
				assert.Equal(t, "event", eventKind.AsString())
				assert.True(t, tc.processedAsEvent)
			} else {
				assert.False(t, tc.processedAsEvent)
			}
		})
	}

	t.Run("existing_attributes_not_overridden", func(t *testing.T) {
		// Create a new log record with existing attributes
		logRecord := logRecords.AppendEmpty()
		logRecord.SetEventName("device.crash")
		logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(12345, 0)))
		logRecord.Attributes().PutStr("exception.stacktrace", "test stacktrace")

		// Set existing attributes that enrichment would normally set
		existingAttrs := map[string]any{
			elasticattr.EventKind:        "existing-event-kind",
			elasticattr.EventCategory:    "existing-event-category",
			elasticattr.ProcessorEvent:   "existing-processor-event",
			elasticattr.TimestampUs:      int64(12345),
			elasticattr.ErrorID:          "existing-error-id",
			elasticattr.ErrorType:        "existing-error-type",
			elasticattr.ErrorGroupingKey: "existing-grouping-key",
		}

		for k, v := range existingAttrs {
			_ = logRecord.Attributes().PutEmpty(k).FromRaw(v)
		}

		// Store original attributes
		originalAttrs := logRecord.Attributes().AsRaw()

		// Enrich the log
		enricher := NewEnricher(config.Enabled())
		enricher.EnrichLogs(logs)

		// Verify existing attributes are preserved
		for k, expectedValue := range existingAttrs {
			actualValue, ok := logRecord.Attributes().Get(k)
			assert.True(t, ok, "attribute %s should exist", k)
			assert.Equal(t, expectedValue, actualValue.AsRaw(), "attribute %s should not be overridden", k)
		}

		// Verify the original attributes map is unchanged
		assert.Empty(t, cmp.Diff(originalAttrs, logRecord.Attributes().AsRaw()))
	})
}

func TestEnrichLogError(t *testing.T) {
	newLogRecord := func() plog.LogRecord {
		logs := plog.NewLogs()
		return logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	}

	tests := []struct {
		name                 string
		setupConfig          func() config.Config
		exceptionType        string
		exceptionMessage     string
		exceptionStacktrace  string
		exceptionEscaped     bool
		otherAttributes      map[string]any
		wantAttributes       map[string]any
		wantAbsentAttributes []string
		wantAttributeLengths map[string]int // when set, assert attribute exists and its string value has this length
	}{
		{
			name:            "no op when no exception type or message",
			setupConfig:     config.Enabled,
			otherAttributes: map[string]any{"other": "value"},
			wantAttributes:  map[string]any{"other": "value"},
			wantAbsentAttributes: []string{
				elasticattr.ErrorID,
				"error.exception.message",
				elasticattr.ErrorGroupingKey,
			},
		},
		{
			name:           "empty exception message replaced with placeholder",
			setupConfig:    config.Enabled,
			exceptionType:  "java.lang.RuntimeException",
			wantAttributes: map[string]any{"error.exception.message": emptyExceptionMsg},
		},
		{
			name:             "enriches with exception type and message",
			setupConfig:      config.Enabled,
			exceptionType:    "java.lang.NullPointerException",
			exceptionMessage: "Cannot invoke method on null",
			wantAttributes: map[string]any{
				"error.exception.type":    "java.lang.NullPointerException",
				"error.exception.message": "Cannot invoke method on null",
			},
		},
		{
			name:             "sets exception handled from escaped",
			setupConfig:      config.Enabled,
			exceptionMessage: "err",
			exceptionEscaped: true,
			wantAttributes:   map[string]any{elasticattr.ErrorExceptionHandled: false},
		},
		{
			name:                 "error id has length 32 when config enabled and not already set",
			setupConfig:          config.Enabled,
			exceptionMessage:     "err",
			wantAttributeLengths: map[string]int{elasticattr.ErrorID: 32},
		},
		{
			name:                "sets stacktrace when enabled",
			setupConfig:         config.Enabled,
			exceptionMessage:    "err",
			exceptionStacktrace: "at foo.bar(Baz.java:42)",
			wantAttributes:      map[string]any{elasticattr.ErrorStackTrace: "at foo.bar(Baz.java:42)"},
		},
		{
			name:             "does not set attributes when config disabled",
			setupConfig:      func() config.Config { return config.Config{} },
			exceptionType:    "Ex",
			exceptionMessage: "msg",
			wantAbsentAttributes: []string{
				elasticattr.ErrorID,
				elasticattr.ErrorGroupingKey,
				"error.exception.message",
			},
		},
		// Per-option disabled: ensure each attribute is not set when its config is disabled.
		{
			name: "does not set error id when ErrorID config disabled",
			setupConfig: func() config.Config {
				c := config.Enabled()
				c.Log.ErrorConfig.ErrorID.Enabled = false
				return c
			},
			exceptionType:        "Ex",
			exceptionMessage:     "msg",
			wantAbsentAttributes: []string{elasticattr.ErrorID},
			wantAttributes: map[string]any{
				"error.exception.type":    "Ex",
				"error.exception.message": "msg",
			},
		},
		{
			name: "does not set error exception handled when config disabled",
			setupConfig: func() config.Config {
				c := config.Enabled()
				c.Log.ErrorExceptionConfig.ErrorExceptionHandled = config.AttributeConfig{Enabled: false}
				return c
			},
			exceptionMessage:     "err",
			exceptionEscaped:     true,
			wantAbsentAttributes: []string{elasticattr.ErrorExceptionHandled},
			wantAttributes:       map[string]any{"error.exception.message": "err"},
		},
		{
			name: "does not set error exception message when config disabled",
			setupConfig: func() config.Config {
				c := config.Enabled()
				c.Log.ErrorExceptionConfig.ErrorExceptionMessage = config.AttributeConfig{Enabled: false}
				return c
			},
			exceptionType:        "Ex",
			exceptionMessage:     "msg",
			wantAbsentAttributes: []string{"error.exception.message"},
			wantAttributes:       map[string]any{"error.exception.type": "Ex"},
		},
		{
			name: "does not set error exception type when config disabled",
			setupConfig: func() config.Config {
				c := config.Enabled()
				c.Log.ErrorExceptionConfig.ErrorExceptionType = config.AttributeConfig{Enabled: false}
				return c
			},
			exceptionType:        "Ex",
			exceptionMessage:     "msg",
			wantAbsentAttributes: []string{"error.exception.type"},
			wantAttributes:       map[string]any{"error.exception.message": "msg"},
		},
		{
			name: "does not set error stack trace when config disabled",
			setupConfig: func() config.Config {
				c := config.Enabled()
				c.Log.ErrorConfig.ErrorStackTrace = config.AttributeConfig{Enabled: false}
				return c
			},
			exceptionMessage:     "err",
			exceptionStacktrace:  "at foo(Bar.java:1)",
			wantAbsentAttributes: []string{elasticattr.ErrorStackTrace},
			wantAttributes:       map[string]any{"error.exception.message": "err"},
		},
		{
			name: "does not set error grouping key when config disabled",
			setupConfig: func() config.Config {
				c := config.Enabled()
				c.Log.ErrorConfig.ErrorGroupingKey = config.AttributeConfig{Enabled: false}
				return c
			},
			exceptionType:        "Ex",
			exceptionMessage:     "msg",
			wantAbsentAttributes: []string{elasticattr.ErrorGroupingKey},
			wantAttributes: map[string]any{
				"error.exception.type":    "Ex",
				"error.exception.message": "msg",
			},
		},
		{
			name:                "does not overwrite existing error attributes",
			setupConfig:         config.Enabled,
			exceptionType:       "Ex",
			exceptionMessage:    "msg",
			exceptionStacktrace: "at existing.StackTrace()",
			exceptionEscaped:    true,
			otherAttributes: map[string]any{
				elasticattr.ErrorID:               "existing-error-id",
				elasticattr.ErrorExceptionHandled: true,
				"error.exception.message":         "existing message",
				"error.exception.type":            "existing.type",
				elasticattr.ErrorStackTrace:       "existing stack trace",
				elasticattr.ErrorGroupingKey:      "existing-grouping-key",
			},
			wantAttributes: map[string]any{
				elasticattr.ErrorID:               "existing-error-id",
				elasticattr.ErrorExceptionHandled: true,
				"error.exception.message":         "existing message",
				"error.exception.type":            "existing.type",
				elasticattr.ErrorStackTrace:       "existing stack trace",
				elasticattr.ErrorGroupingKey:      "existing-grouping-key",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr := newLogRecord()
			attrs := lr.Attributes()

			if tt.exceptionType != "" {
				attrs.PutStr(string(semconv.ExceptionTypeKey), tt.exceptionType)
			}
			if tt.exceptionMessage != "" {
				attrs.PutStr(string(semconv.ExceptionMessageKey), tt.exceptionMessage)
			}
			if tt.exceptionStacktrace != "" {
				attrs.PutStr(string(semconv.ExceptionStacktraceKey), tt.exceptionStacktrace)
			}
			if tt.exceptionEscaped {
				attrs.PutBool(string(semconv.ExceptionEscapedKey), true)
			}
			for k, v := range tt.otherAttributes {
				_ = attrs.PutEmpty(k).FromRaw(v)
			}

			EnrichLogError(lr, tt.setupConfig())

			for key, want := range tt.wantAttributes {
				got, ok := attrs.Get(key)
				require.True(t, ok, "attribute %q should be set", key)
				assert.Equal(t, want, got.AsRaw(), "attribute %q", key)
			}
			for _, key := range tt.wantAbsentAttributes {
				_, ok := attrs.Get(key)
				assert.False(t, ok, "attribute %q should not be set", key)
			}
			for key, wantLen := range tt.wantAttributeLengths {
				got, ok := attrs.Get(key)
				require.True(t, ok, "attribute %q should be set", key)
				assert.Len(t, got.Str(), wantLen, "attribute %q", key)
			}
		})
	}
}
