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

// Package streamlangprocessor - Kibana compatibility tests.
//
// These are GOLDEN tests ported from the Kibana kbn-streamlang cross-compatibility
// test suite. If our Go implementation passes these, it matches Kibana's behavior.
//
// Source: kbn-streamlang-tests/test/scout/api/tests/cross_compatibility/*.spec.ts
package streamlangprocessor

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/types"
)

// --- Test helpers ---

// runPipeline executes a streamlang pipeline (list of StreamlangSteps) against
// a map-mode document and returns the Document for field inspection. This mirrors
// what the Kibana cross-compat tests do: define DSL → execute → check output fields.
func runPipeline(t *testing.T, steps []StreamlangStep, fields map[string]any) *Document {
	t.Helper()

	flat := FlattenSteps(steps)
	backend := NewClosureBackend(Processors())
	if err := backend.Compile(flat); err != nil {
		t.Fatalf("compile: %v", err)
	}

	doc := NewDocument(fields)
	err := backend.Execute(doc)
	if err != nil {
		// Check for ErrDropDocument sentinel.
		if err == types.ErrDropDocument {
			return nil // document was dropped
		}
		t.Fatalf("execute: %v", err)
	}
	if doc.IsDropped() {
		return nil
	}
	return doc
}

// runPipelineAllowError is like runPipeline but returns the error instead of
// failing the test, so callers can assert on error conditions.
func runPipelineAllowError(t *testing.T, steps []StreamlangStep, fields map[string]any) (*Document, error) {
	t.Helper()

	flat := FlattenSteps(steps)
	backend := NewClosureBackend(Processors())
	if err := backend.Compile(flat); err != nil {
		return nil, err
	}

	doc := NewDocument(fields)
	err := backend.Execute(doc)
	if err != nil {
		return nil, err
	}
	if doc.IsDropped() {
		return nil, types.ErrDropDocument
	}
	return doc, nil
}

func boolP(b bool) *bool { return &b }

func assertField(t *testing.T, doc *Document, path string, expected any) {
	t.Helper()
	val, ok := doc.Get(path)
	if !ok {
		t.Errorf("field %q not found", path)
		return
	}
	if !reflect.DeepEqual(val, expected) {
		t.Errorf("field %q = %v (%T), want %v (%T)", path, val, val, expected, expected)
	}
}

func assertFieldMissing(t *testing.T, doc *Document, path string) {
	t.Helper()
	if doc.Has(path) {
		v, _ := doc.Get(path)
		t.Errorf("field %q should not exist, got %v", path, v)
	}
}

func assertFieldCloseTo(t *testing.T, doc *Document, path string, expected float64, tolerance float64) {
	t.Helper()
	v, ok := doc.Get(path)
	if !ok {
		t.Errorf("field %q not found", path)
		return
	}
	f, ok := types.ToFloat64(v)
	if !ok {
		t.Errorf("field %q = %v is not numeric", path, v)
		return
	}
	if math.Abs(f-expected) > tolerance {
		t.Errorf("field %q = %v, want ~%v (tolerance %v)", path, f, expected, tolerance)
	}
}

// ============================================================================
// SET PROCESSOR
// ============================================================================

func TestKibanaCompat_Set(t *testing.T) {
	tests := []struct {
		name   string
		steps  []StreamlangStep
		input  map[string]any
		check  func(t *testing.T, result *Document)
	}{
		{
			name: "set a field using a value",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.status", Value: "active"}},
			},
			input: map[string]any{"attributes": map[string]any{"size": 4096}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.status", "active")
			},
		},
		{
			name: "set a field by copying from another field",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.status", CopyFrom: "message"}},
			},
			input: map[string]any{"message": "should-be-copied"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.status", "should-be-copied")
			},
		},
		{
			name: "override existing field when override is true",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.status", Value: "inactive", Override: boolP(true)}},
			},
			input: map[string]any{"attributes": map[string]any{"status": "active"}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.status", "inactive")
			},
		},
		{
			name: "do not override when override is false",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.status", Value: "inactive", Override: boolP(false)}},
			},
			input: map[string]any{"attributes": map[string]any{"status": "active"}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.status", "active")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// RENAME PROCESSOR
// ============================================================================

func TestKibanaCompat_Rename(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "rename a field when override is true",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "rename", From: "host.original", To: "host.renamed", Override: boolP(true)}},
			},
			input: map[string]any{"host": map[string]any{"original": "test-host", "renamed": "old-host"}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "host.renamed", "test-host")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// CONVERT PROCESSOR
// ============================================================================

func TestKibanaCompat_Convert(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "convert field to string",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "convert", From: "attributes.size", Type: "string"}},
			},
			input: map[string]any{"attributes": map[string]any{"size": 4096}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.size", "4096")
			},
		},
		{
			name: "convert field to string into target field",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "convert", From: "attributes.size", To: "attributes.size_str", Type: "string"}},
			},
			input: map[string]any{"attributes": map[string]any{"size": 4096}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.size", 4096) // original preserved
				assertField(t, r, "attributes.size_str", "4096")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// UPPERCASE / LOWERCASE / TRIM PROCESSORS
// ============================================================================

func TestKibanaCompat_Uppercase(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "uppercase a field in-place",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "uppercase", From: "message"}},
			},
			input: map[string]any{"message": "test message 1"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "TEST MESSAGE 1")
			},
		},
		{
			name: "uppercase into target field",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "uppercase", From: "message", To: "message_upper"}},
			},
			input: map[string]any{"message": "test message 1"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message_upper", "TEST MESSAGE 1")
			},
		},
		{
			name: "uppercase with where condition",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "uppercase", From: "message",
					Where: &Condition{Field: "should_uppercase", Eq: "yes"}}},
			},
			input: map[string]any{"message": "test message 1", "should_uppercase": "yes"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "TEST MESSAGE 1")
			},
		},
		{
			name: "uppercase skipped when where condition fails",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "uppercase", From: "message",
					Where: &Condition{Field: "should_uppercase", Eq: "yes"}}},
			},
			input: map[string]any{"message": "test message 2", "should_uppercase": "no"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "test message 2")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

func TestKibanaCompat_Lowercase(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "lowercase a field in-place",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "lowercase", From: "message"}},
			},
			input: map[string]any{"message": "TEST MESSAGE 1"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "test message 1")
			},
		},
		{
			name: "lowercase into target field",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "lowercase", From: "message", To: "message_lowercase"}},
			},
			input: map[string]any{"message": "TEST MESSAGE 1"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message_lowercase", "test message 1")
			},
		},
		{
			name: "lowercase with where condition - matches",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "lowercase", From: "message",
					Where: &Condition{Field: "should_lowercase", Eq: "yes"}}},
			},
			input: map[string]any{"message": "TEST MESSAGE 1", "should_lowercase": "yes"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "test message 1")
			},
		},
		{
			name: "lowercase with where condition - no match",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "lowercase", From: "message",
					Where: &Condition{Field: "should_lowercase", Eq: "yes"}}},
			},
			input: map[string]any{"message": "TEST MESSAGE 2", "should_lowercase": "no"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "TEST MESSAGE 2")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

func TestKibanaCompat_Trim(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "trim a field in-place",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "trim", From: "message"}},
			},
			input: map[string]any{"message": "   test message 1   "},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "test message 1")
			},
		},
		{
			name: "trim into target field",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "trim", From: "message", To: "message_trimmed"}},
			},
			input: map[string]any{"message": "   test message 1   "},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message_trimmed", "test message 1")
			},
		},
		{
			name: "trim with where condition - matches",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "trim", From: "message",
					Where: &Condition{Field: "should_trim", Eq: "yes"}}},
			},
			input: map[string]any{"message": "   test message 1   ", "should_trim": "yes"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "test message 1")
			},
		},
		{
			name: "trim with where condition - no match",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "trim", From: "message",
					Where: &Condition{Field: "should_trim", Eq: "yes"}}},
			},
			input: map[string]any{"message": "   test message 2   ", "should_trim": "no"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "   test message 2   ")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// REPLACE PROCESSOR
// ============================================================================

func TestKibanaCompat_Replace(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "replace literal string in-place",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "replace", From: "message", Pattern: "error", Replacement: "warning"}},
			},
			input: map[string]any{"message": "An error occurred"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "An warning occurred")
			},
		},
		{
			name: "replace to target field",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "replace", From: "message", To: "clean_message", Pattern: "error", Replacement: "warning"}},
			},
			input: map[string]any{"message": "An error occurred"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "An error occurred")         // original preserved
				assertField(t, r, "clean_message", "An warning occurred") // new field
			},
		},
		{
			name: "replace regex pattern",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "replace", From: "message", Pattern: `\d{3}`, Replacement: "[NUM]"}},
			},
			input: map[string]any{"message": "Error code 404 found"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "Error code [NUM] found")
			},
		},
		{
			name: "replace with capture groups",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "replace", From: "message", Pattern: `User (\w+) has (\d+) new (messages?)`, Replacement: "Messages: $2 for user $1"}},
			},
			input: map[string]any{"message": "User alice has 3 new messages"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "Messages: 3 for user alice")
			},
		},
		{
			name: "conditional replacement with where clause",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "replace", From: "message", Pattern: "error", Replacement: "warning",
					Where: &Condition{Field: "event.kind", Eq: "test"}}},
			},
			input: map[string]any{"message": "An error occurred", "event": map[string]any{"kind": "test"}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "An warning occurred")
			},
		},
		{
			name: "conditional replacement skipped when where clause fails",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "replace", From: "message", Pattern: "error", Replacement: "warning",
					Where: &Condition{Field: "event.kind", Eq: "test"}}},
			},
			input: map[string]any{"message": "An error occurred", "event": map[string]any{"kind": "production"}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "An error occurred")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// GROK PROCESSOR
// ============================================================================

func TestKibanaCompat_Grok(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "parse log line with grok",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "grok", From: "message",
					Patterns: []string{`%{IP:client.ip} %{WORD:http.request.method} %{URIPATHPARAM:url.path} %{NUMBER:http.response.body.bytes} %{NUMBER:event.duration}`}}},
			},
			input: map[string]any{"message": "55.3.244.1 GET /index.html 15824 0.043"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "client.ip", "55.3.244.1")
				assertField(t, r, "http.request.method", "GET")
				assertField(t, r, "url.path", "/index.html")
			},
		},
		{
			name: "grok with where clause - matches",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "grok", From: "message",
					Patterns: []string{`%{IP:client.ip}`},
					Where:    &Condition{Field: "attributes.should_exist", Exists: boolP(true)}}},
			},
			input: map[string]any{"attributes": map[string]any{"should_exist": "YES"}, "message": "55.3.244.1"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "client.ip", "55.3.244.1")
			},
		},
		{
			name: "grok with where clause - skipped",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "grok", From: "message",
					Patterns: []string{`%{IP:client.ip}`},
					Where:    &Condition{Field: "attributes.should_exist", Exists: boolP(true)}}},
			},
			input: map[string]any{"attributes": map[string]any{"size": 2048}, "message": "127.0.0.1"},
			check: func(t *testing.T, r *Document) {
				assertFieldMissing(t, r, "client.ip")
			},
		},
		{
			name: "grok leaves source field intact",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "grok", From: "message",
					Patterns: []string{`%{IP:ip}`}}},
			},
			input: map[string]any{"message": "1.2.3.4", "untouched": "preserved"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "1.2.3.4")
				assertField(t, r, "untouched", "preserved")
				assertField(t, r, "ip", "1.2.3.4")
			},
		},
		{
			name: "grok overrides source field when captured in pattern",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "grok", From: "message",
					Patterns: []string{`%{IP:ip} %{GREEDYDATA:message}`}}},
			},
			input: map[string]any{"message": "1.2.3.4 This is the extracted message", "untouched": "preserved"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "ip", "1.2.3.4")
				assertField(t, r, "message", "This is the extracted message")
				assertField(t, r, "untouched", "preserved")
			},
		},
		{
			name: "grok with custom pattern definitions",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "grok", From: "message",
					Patterns:           []string{`%{FAVORITE_CAT:pet}`},
					PatternDefinitions: map[string]string{"FAVORITE_CAT": "burmese"}}},
			},
			input: map[string]any{"message": "I love burmese cats!"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "pet", "burmese")
			},
		},
		{
			name: "grok with nested pattern definitions",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "grok", From: "message",
					Patterns:           []string{`%{FAVORITE_PET:pet}`},
					PatternDefinitions: map[string]string{"FAVORITE_PET": "%{FAVORITE_CAT}", "FAVORITE_CAT": "burmese"}}},
			},
			input: map[string]any{"message": "I love burmese cats!"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "pet", "burmese")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// DISSECT PROCESSOR
// ============================================================================

func TestKibanaCompat_Dissect(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "parse log line with dissect",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "dissect", From: "message",
					Pattern: `[%{@timestamp}] [%{log.level}] %{client.ip} - - "%{@method} %{url.original} HTTP/%{http.version}" %{http.response.status_code} %{http.response.body.bytes}`}},
			},
			input: map[string]any{"message": `[2025-01-01T00:00:00.000Z] [info] 127.0.0.1 - - "GET / HTTP/1.1" 200 123`},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "@timestamp", "2025-01-01T00:00:00.000Z")
				assertField(t, r, "log.level", "info")
				assertField(t, r, "client.ip", "127.0.0.1")
				assertField(t, r, "url.original", "/")
			},
		},
		{
			name: "dissect with append_separator",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "dissect", From: "message",
					Pattern: `%{+field1}-%{+field1}`, AppendSeparator: ","}},
			},
			input: map[string]any{"message": "value1-value2"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "field1", "value1,value2")
			},
		},
		{
			name: "dissect leaves source field intact",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "dissect", From: "message",
					Pattern: `%{client.ip} - %{response_message}`}},
			},
			input: map[string]any{"message": "127.0.0.1 - This is the extracted message", "existing_field": "preserved"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "client.ip", "127.0.0.1")
				assertField(t, r, "message", "127.0.0.1 - This is the extracted message") // preserved if not captured
			},
		},
		{
			name: "dissect overrides source field when in pattern",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "dissect", From: "message",
					Pattern: `%{client.ip} - %{message}`}},
			},
			input: map[string]any{"message": "127.0.0.1 - This is the extracted message"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "client.ip", "127.0.0.1")
				assertField(t, r, "message", "This is the extracted message")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// SPLIT PROCESSOR
// ============================================================================

func TestKibanaCompat_Split(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "split string with comma delimiter",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "split", From: "tags", Separator: ","}},
			},
			input: map[string]any{"tags": "foo,bar,baz"},
			check: func(t *testing.T, r *Document) {
				doc := NewDocument(r)
				v, ok := doc.Get("tags")
				if !ok {
					t.Fatal("tags not found")
				}
				arr, ok := v.([]any)
				if !ok {
					t.Fatalf("tags is %T, want []any", v)
				}
				if len(arr) != 3 || arr[0] != "foo" || arr[1] != "bar" || arr[2] != "baz" {
					t.Fatalf("tags = %v, want [foo bar baz]", arr)
				}
			},
		},
		{
			name: "split to target field",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "split", From: "tags", To: "tags_array", Separator: ","}},
			},
			input: map[string]any{"tags": "foo,bar,baz"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "tags", "foo,bar,baz") // original preserved
				doc := NewDocument(r)
				v, _ := doc.Get("tags_array")
				arr := v.([]any)
				if len(arr) != 3 {
					t.Fatalf("tags_array len = %d, want 3", len(arr))
				}
			},
		},
		{
			name: "split with slash delimiter",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "split", From: "path", Separator: "/"}},
			},
			input: map[string]any{"path": "home/user/documents"},
			check: func(t *testing.T, r *Document) {
				doc := NewDocument(r)
				v, _ := doc.Get("path")
				arr := v.([]any)
				if len(arr) != 3 || arr[0] != "home" || arr[1] != "user" || arr[2] != "documents" {
					t.Fatalf("path = %v", arr)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// JOIN PROCESSOR
// ============================================================================

func TestKibanaCompat_Join(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "join fields with delimiter",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "join", FromFields: []string{"field1", "field2", "field3"}, To: "my_joined_field", Delimiter: ", "}},
			},
			input: map[string]any{"field1": "first", "field2": "second", "field3": "third"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "my_joined_field", "first, second, third")
			},
		},
		{
			name: "join with ignore_missing skips missing fields",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "join", FromFields: []string{"field1", "field2", "field3"}, To: "my_joined_field", Delimiter: ", ", IgnoreMissing: boolP(true)}},
			},
			input: map[string]any{"field1": "first", "field3": "third"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "my_joined_field", "first, third")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// SORT PROCESSOR
// ============================================================================

func TestKibanaCompat_Sort(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "sort array ascending (default)",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "sort", From: "tags"}},
			},
			input: map[string]any{"tags": []any{"charlie", "alpha", "bravo"}},
			check: func(t *testing.T, r *Document) {
				doc := NewDocument(r)
				v, _ := doc.Get("tags")
				arr := v.([]any)
				if arr[0] != "alpha" || arr[1] != "bravo" || arr[2] != "charlie" {
					t.Fatalf("tags = %v, want [alpha bravo charlie]", arr)
				}
			},
		},
		{
			name: "sort array descending",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "sort", From: "tags", Order: "desc"}},
			},
			input: map[string]any{"tags": []any{"charlie", "alpha", "bravo"}},
			check: func(t *testing.T, r *Document) {
				doc := NewDocument(r)
				v, _ := doc.Get("tags")
				arr := v.([]any)
				if arr[0] != "charlie" || arr[1] != "bravo" || arr[2] != "alpha" {
					t.Fatalf("tags = %v, want [charlie bravo alpha]", arr)
				}
			},
		},
		{
			name: "sort to target field",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "sort", From: "tags", To: "sorted_tags", Order: "asc"}},
			},
			input: map[string]any{"tags": []any{"charlie", "alpha", "bravo"}},
			check: func(t *testing.T, r *Document) {
				doc := NewDocument(r)
				v, _ := doc.Get("sorted_tags")
				arr := v.([]any)
				if arr[0] != "alpha" || arr[1] != "bravo" || arr[2] != "charlie" {
					t.Fatalf("sorted_tags = %v", arr)
				}
			},
		},
		{
			name: "sort numeric array",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "sort", From: "numbers", Order: "asc"}},
			},
			input: map[string]any{"numbers": []any{3, 1, 4, 1, 5, 9, 2, 6}},
			check: func(t *testing.T, r *Document) {
				doc := NewDocument(r)
				v, _ := doc.Get("numbers")
				arr := v.([]any)
				expected := []int{1, 1, 2, 3, 4, 5, 6, 9}
				for i, want := range expected {
					f, ok := types.ToFloat64(arr[i])
					if !ok || int(f) != want {
						t.Fatalf("numbers[%d] = %v, want %d", i, arr[i], want)
					}
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// APPEND PROCESSOR
// ============================================================================

func TestKibanaCompat_Append(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "append to existing array",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "append", To: "tags", Values: []any{"new_tag"}}},
			},
			input: map[string]any{"tags": []any{"existing_tag"}},
			check: func(t *testing.T, r *Document) {
				doc := NewDocument(r)
				v, _ := doc.Get("tags")
				arr := v.([]any)
				if len(arr) != 2 || arr[0] != "existing_tag" || arr[1] != "new_tag" {
					t.Fatalf("tags = %v", arr)
				}
			},
		},
		{
			name: "append multiple values to non-existent field",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "append", To: "tags", Values: []any{"tag1", "tag2"}}},
			},
			input: map[string]any{"message": "a"},
			check: func(t *testing.T, r *Document) {
				doc := NewDocument(r)
				v, _ := doc.Get("tags")
				arr := v.([]any)
				if len(arr) != 2 || arr[0] != "tag1" || arr[1] != "tag2" {
					t.Fatalf("tags = %v", arr)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// CONCAT PROCESSOR
// ============================================================================

func TestKibanaCompat_Concat(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "concat fields and literals",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "concat", To: "full_email", ConcatFrom: []ConcatSource{
					{Type: "field", Value: "first_name"},
					{Type: "literal", Value: "."},
					{Type: "field", Value: "last_name"},
					{Type: "literal", Value: "@"},
					{Type: "field", Value: "email_domain"},
				}}},
			},
			input: map[string]any{"first_name": "john", "last_name": "doe", "email_domain": "example.com"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "full_email", "john.doe@example.com")
			},
		},
		{
			name: "concat with ignore_missing",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "concat", To: "full_email", IgnoreMissing: boolP(true), ConcatFrom: []ConcatSource{
					{Type: "field", Value: "first_name"},
					{Type: "literal", Value: "."},
					{Type: "field", Value: "last_name"},
					{Type: "literal", Value: "@"},
					{Type: "field", Value: "email_domain"},
				}}},
			},
			input: map[string]any{"first_name": "jane", "last_name": "smith"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "full_email", "jane.smith@")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// MATH PROCESSOR
// ============================================================================

func TestKibanaCompat_Math(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "multiplication",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "math", Expression: "price * quantity", To: "total"}},
			},
			input: map[string]any{"price": 10, "quantity": 5},
			check: func(t *testing.T, r *Document) {
				assertFieldCloseTo(t, r, "total", 50, 0.001)
			},
		},
		{
			name: "addition",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "math", Expression: "val + small", To: "r_add"}},
			},
			input: map[string]any{"val": 4, "small": 1},
			check: func(t *testing.T, r *Document) {
				assertFieldCloseTo(t, r, "r_add", 5, 0.001)
			},
		},
		{
			name: "subtraction",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "math", Expression: "val - small", To: "r_sub"}},
			},
			input: map[string]any{"val": 4, "small": 1},
			check: func(t *testing.T, r *Document) {
				assertFieldCloseTo(t, r, "r_sub", 3, 0.001)
			},
		},
		{
			name: "division",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "math", Expression: "val / small", To: "r_div"}},
			},
			input: map[string]any{"val": 4, "small": 1},
			check: func(t *testing.T, r *Document) {
				assertFieldCloseTo(t, r, "r_div", 4, 0.001)
			},
		},
		{
			name: "log function",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "math", Expression: "log(value)", To: "result"}},
			},
			input: map[string]any{"value": math.E},
			check: func(t *testing.T, r *Document) {
				assertFieldCloseTo(t, r, "result", 1.0, 0.00001)
			},
		},
		{
			name: "nested field paths",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "math", Expression: "attributes.price * attributes.quantity", To: "attributes.total"}},
			},
			input: map[string]any{"attributes": map[string]any{"price": 25, "quantity": 4}},
			check: func(t *testing.T, r *Document) {
				assertFieldCloseTo(t, r, "attributes.total", 100, 0.001)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// JSON_EXTRACT PROCESSOR
// ============================================================================

func TestKibanaCompat_JSONExtract(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "extract simple field from JSON string",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "json_extract", Field: "message",
					Extractions: []JsonExtraction{{Selector: "user_id", TargetField: "user_id"}}}},
			},
			input: map[string]any{"message": `{"user_id": "abc123"}`},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "user_id", "abc123")
			},
		},
		{
			name: "extract multiple fields from JSON string",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "json_extract", Field: "message",
					Extractions: []JsonExtraction{
						{Selector: "user_id", TargetField: "user_id"},
						{Selector: "status", TargetField: "event_status"},
					}}},
			},
			input: map[string]any{"message": `{"user_id": "abc123", "status": "active"}`},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "user_id", "abc123")
				assertField(t, r, "event_status", "active")
			},
		},
		{
			name: "extract nested fields using dot notation",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "json_extract", Field: "message",
					Extractions: []JsonExtraction{{Selector: "metadata.client.ip", TargetField: "client_ip"}}}},
			},
			input: map[string]any{"message": `{"metadata": {"client": {"ip": "192.168.1.1"}}}`},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "client_ip", "192.168.1.1")
			},
		},
		{
			name: "extract integer with type cast",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "json_extract", Field: "message",
					Extractions: []JsonExtraction{{Selector: "count", TargetField: "count", Type: "integer"}}}},
			},
			input: map[string]any{"message": `{"count": 42}`},
			check: func(t *testing.T, r *Document) {
				doc := NewDocument(r)
				v, ok := doc.Get("count")
				if !ok {
					t.Fatal("count not found")
				}
				f, ok := types.ToFloat64(v)
				if !ok || f != 42 {
					t.Fatalf("count = %v, want 42", v)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// REDACT PROCESSOR
// ============================================================================

func TestKibanaCompat_Redact(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "redact IP address with default delimiters",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "redact", From: "message", Patterns: []string{"%{IP:client_ip}"}}},
			},
			input: map[string]any{"message": "Connection from 192.168.1.1 established"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "Connection from <client_ip> established")
			},
		},
		{
			name: "redact email address",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "redact", From: "message", Patterns: []string{"%{EMAILADDRESS:email}"}}},
			},
			input: map[string]any{"message": "Contact user at john.doe@example.com for details"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "Contact user at <email> for details")
			},
		},
		{
			name: "redact multiple patterns",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "redact", From: "message", Patterns: []string{"%{IP:ip}", "%{EMAILADDRESS:email}"}}},
			},
			input: map[string]any{"message": "User john@example.com connected from 10.0.0.1"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "User <email> connected from <ip>")
			},
		},
		{
			name: "redact with custom prefix and suffix",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "redact", From: "message",
					Patterns: []string{"%{IP:client}"}, Prefix: "[REDACTED:", Suffix: "]"}},
			},
			input: map[string]any{"message": "Request from 172.16.0.1"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "Request from [REDACTED:client]")
			},
		},
		{
			name: "redact no match leaves field unchanged",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "redact", From: "message", Patterns: []string{"%{IP:ip}"}}},
			},
			input: map[string]any{"message": "Hello world, no IP here"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "message", "Hello world, no IP here")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// REMOVE_BY_PREFIX PROCESSOR
// ============================================================================

func TestKibanaCompat_RemoveByPrefix(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "remove nested fields by prefix",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "remove_by_prefix", From: "host"}},
			},
			input: map[string]any{"host": map[string]any{"name": "server01", "ip": "192.168.1.1"}, "message": "keep-this"},
			check: func(t *testing.T, r *Document) {
				assertFieldMissing(t, r, "host")
				assertFieldMissing(t, r, "host.name")
				assertFieldMissing(t, r, "host.ip")
				assertField(t, r, "message", "keep-this")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// DROP_DOCUMENT PROCESSOR
// ============================================================================

func TestKibanaCompat_DropDocument(t *testing.T) {
	tests := []struct {
		name    string
		steps   []StreamlangStep
		input   map[string]any
		dropped bool
	}{
		{
			name: "drop document matching where condition",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "drop_document",
					Where: &Condition{Field: "logType", Eq: "info"}}},
			},
			input:   map[string]any{"logType": "info", "message": "drop-this"},
			dropped: true,
		},
		{
			name: "keep document not matching where condition",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "drop_document",
					Where: &Condition{Field: "logType", Eq: "info"}}},
			},
			input:   map[string]any{"logType": "critical", "message": "keep-this"},
			dropped: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			if tt.dropped && result != nil {
				t.Fatal("expected document to be dropped")
			}
			if !tt.dropped && result == nil {
				t.Fatal("expected document to be kept")
			}
		})
	}
}

// ============================================================================
// FILTER CONDITIONS (where clauses)
// ============================================================================

func TestKibanaCompat_FilterConditions(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "eq condition matches",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.is_active", Value: "yes",
					Where: &Condition{Field: "attributes.status", Eq: "active"}}},
			},
			input: map[string]any{"attributes": map[string]any{"status": "active"}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.is_active", "yes")
			},
		},
		{
			name: "eq condition does not match",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.is_active", Value: "yes",
					Where: &Condition{Field: "attributes.status", Eq: "active"}}},
			},
			input: map[string]any{"attributes": map[string]any{"status": "inactive"}},
			check: func(t *testing.T, r *Document) {
				assertFieldMissing(t, r, "attributes.is_active")
			},
		},
		{
			name: "neq condition",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.not_deleted", Value: "kept",
					Where: &Condition{Field: "attributes.status", Neq: "deleted"}}},
			},
			input: map[string]any{"attributes": map[string]any{"status": "active"}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.not_deleted", "kept")
			},
		},
		{
			name: "neq condition does not match deleted",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.not_deleted", Value: "kept",
					Where: &Condition{Field: "attributes.status", Neq: "deleted"}}},
			},
			input: map[string]any{"attributes": map[string]any{"status": "deleted"}},
			check: func(t *testing.T, r *Document) {
				assertFieldMissing(t, r, "attributes.not_deleted")
			},
		},
		{
			name: "gt condition",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.high_priority", Value: "high",
					Where: &Condition{Field: "attributes.priority", Gt: 5}}},
			},
			input: map[string]any{"attributes": map[string]any{"priority": 10}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.high_priority", "high")
			},
		},
		{
			name: "gt condition does not match at boundary",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.high_priority", Value: "high",
					Where: &Condition{Field: "attributes.priority", Gt: 5}}},
			},
			input: map[string]any{"attributes": map[string]any{"priority": 5}},
			check: func(t *testing.T, r *Document) {
				assertFieldMissing(t, r, "attributes.high_priority")
			},
		},
		{
			name: "gte condition at boundary",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.adult", Value: "yes",
					Where: &Condition{Field: "attributes.age", Gte: 18}}},
			},
			input: map[string]any{"attributes": map[string]any{"age": 18}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.adult", "yes")
			},
		},
		{
			name: "lt condition",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.low_stock", Value: "low",
					Where: &Condition{Field: "attributes.quantity", Lt: 10}}},
			},
			input: map[string]any{"attributes": map[string]any{"quantity": 5}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.low_stock", "low")
			},
		},
		{
			name: "lt condition does not match at boundary",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.low_stock", Value: "low",
					Where: &Condition{Field: "attributes.quantity", Lt: 10}}},
			},
			input: map[string]any{"attributes": map[string]any{"quantity": 10}},
			check: func(t *testing.T, r *Document) {
				assertFieldMissing(t, r, "attributes.low_stock")
			},
		},
		{
			name: "lte condition at boundary",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.small_file", Value: "small",
					Where: &Condition{Field: "attributes.size", Lte: 1024}}},
			},
			input: map[string]any{"attributes": map[string]any{"size": 1024}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.small_file", "small")
			},
		},
		{
			name: "exists true condition",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.has_email", Value: "yes",
					Where: &Condition{Field: "attributes.user_email", Exists: boolP(true)}}},
			},
			input: map[string]any{"attributes": map[string]any{"user_email": "test@example.com"}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.has_email", "yes")
			},
		},
		{
			name: "exists true condition - missing field",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.has_email", Value: "yes",
					Where: &Condition{Field: "attributes.user_email", Exists: boolP(true)}}},
			},
			input: map[string]any{"attributes": map[string]any{"user_name": "John"}},
			check: func(t *testing.T, r *Document) {
				assertFieldMissing(t, r, "attributes.has_email")
			},
		},
		{
			name: "range condition",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.in_range", Value: "optimal",
					Where: &Condition{Field: "attributes.temperature", Range: &RangeCond{Gte: 20, Lt: 30}}}},
			},
			input: map[string]any{"attributes": map[string]any{"temperature": 25}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.in_range", "optimal")
			},
		},
		{
			name: "range condition - at lower boundary",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.in_range", Value: "optimal",
					Where: &Condition{Field: "attributes.temperature", Range: &RangeCond{Gte: 20, Lt: 30}}}},
			},
			input: map[string]any{"attributes": map[string]any{"temperature": 20}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.in_range", "optimal")
			},
		},
		{
			name: "range condition - at upper boundary excluded",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.in_range", Value: "optimal",
					Where: &Condition{Field: "attributes.temperature", Range: &RangeCond{Gte: 20, Lt: 30}}}},
			},
			input: map[string]any{"attributes": map[string]any{"temperature": 30}},
			check: func(t *testing.T, r *Document) {
				assertFieldMissing(t, r, "attributes.in_range")
			},
		},
		{
			name: "contains condition",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "set", To: "attributes.matched", Value: "matched",
					Where: &Condition{Field: "attributes.service_name", Contains: "synth-service-2"}}},
			},
			input: map[string]any{"attributes": map[string]any{"service_name": "prefix-synth-service-2-suffix"}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.matched", "matched")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// ELSE BRANCH (condition blocks)
// ============================================================================

func TestKibanaCompat_ElseBranch(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "if-branch fires when condition matches",
			steps: []StreamlangStep{
				{
					Condition: &Condition{Field: "attributes.status", Eq: "active"},
					Steps: []StreamlangStep{
						{Action: &ProcessorDef{Action: "set", To: "attributes.outcome", Value: "success"}},
					},
					Else: []StreamlangStep{
						{Action: &ProcessorDef{Action: "set", To: "attributes.outcome", Value: "failure"}},
					},
				},
			},
			input: map[string]any{"attributes": map[string]any{"status": "active"}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.outcome", "success")
			},
		},
		{
			name: "else-branch fires when condition does not match",
			steps: []StreamlangStep{
				{
					Condition: &Condition{Field: "attributes.status", Eq: "active"},
					Steps: []StreamlangStep{
						{Action: &ProcessorDef{Action: "set", To: "attributes.outcome", Value: "success"}},
					},
					Else: []StreamlangStep{
						{Action: &ProcessorDef{Action: "set", To: "attributes.outcome", Value: "failure"}},
					},
				},
			},
			input: map[string]any{"attributes": map[string]any{"status": "inactive"}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.outcome", "failure")
			},
		},
		{
			name: "multiple steps in else branch",
			steps: []StreamlangStep{
				{
					Condition: &Condition{Field: "attributes.status", Eq: "active"},
					Steps: []StreamlangStep{
						{Action: &ProcessorDef{Action: "set", To: "attributes.outcome", Value: "success"}},
					},
					Else: []StreamlangStep{
						{Action: &ProcessorDef{Action: "set", To: "attributes.outcome", Value: "failure"}},
						{Action: &ProcessorDef{Action: "set", To: "attributes.reason", Value: "not_active"}},
					},
				},
			},
			input: map[string]any{"attributes": map[string]any{"status": "inactive"}},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "attributes.outcome", "failure")
				assertField(t, r, "attributes.reason", "not_active")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// MULTI-STEP PIPELINE (integration test)
// ============================================================================

func TestKibanaCompat_MultiStepPipeline(t *testing.T) {
	// Simplified multi-step pipeline mimicking the Kibana cross-compat test:
	// set + uppercase + replace in sequence
	steps := []StreamlangStep{
		{Action: &ProcessorDef{Action: "set", To: "status", Value: "active"}},
		{Action: &ProcessorDef{Action: "uppercase", From: "message"}},
		{Action: &ProcessorDef{Action: "replace", From: "message", Pattern: "ERROR", Replacement: "WARNING"}},
	}
	input := map[string]any{"message": "An error occurred"}
	result := runPipeline(t, steps, input)

	assertField(t, result, "status", "active")
	assertField(t, result, "message", "AN WARNING OCCURRED")
}

func TestKibanaCompat_MultiStepWithConditions(t *testing.T) {
	// Pipeline with conditions matching the Kibana multi-step test pattern:
	// Categorize status codes and performance
	steps := []StreamlangStep{
		// Categorize by status code
		{Action: &ProcessorDef{Action: "set", To: "error.type", Value: "server_error",
			Where: &Condition{Field: "status_code", Gte: 500}}},
		{Action: &ProcessorDef{Action: "set", To: "error.type", Value: "client_error",
			Where: &Condition{Field: "status_code", Range: &RangeCond{Gte: 400, Lt: 500}}}},
		{Action: &ProcessorDef{Action: "set", To: "error.type", Value: "success",
			Where: &Condition{Field: "status_code", Lt: 400}}},
		// Categorize by response time
		{Action: &ProcessorDef{Action: "set", To: "performance.category", Value: "slow",
			Where: &Condition{Field: "response_time_ms", Gt: 1000}}},
		{Action: &ProcessorDef{Action: "set", To: "performance.category", Value: "fast",
			Where: &Condition{Field: "response_time_ms", Lte: 200}}},
		{Action: &ProcessorDef{Action: "set", To: "performance.category", Value: "medium",
			Where: &Condition{Field: "response_time_ms", Range: &RangeCond{Gt: 200, Lte: 1000}}}},
	}

	// Test fast successful request
	t.Run("fast_success", func(t *testing.T) {
		result := runPipeline(t, steps, map[string]any{"status_code": 200, "response_time_ms": 150})
		assertField(t, result, "error.type", "success")
		assertField(t, result, "performance.category", "fast")
	})

	// Test slow server error
	t.Run("slow_server_error", func(t *testing.T) {
		result := runPipeline(t, steps, map[string]any{"status_code": 500, "response_time_ms": 2500})
		assertField(t, result, "error.type", "server_error")
		assertField(t, result, "performance.category", "slow")
	})

	// Test medium client error
	t.Run("medium_client_error", func(t *testing.T) {
		result := runPipeline(t, steps, map[string]any{"status_code": 404, "response_time_ms": 750})
		assertField(t, result, "error.type", "client_error")
		assertField(t, result, "performance.category", "medium")
	})
}

// ============================================================================
// NETWORK DIRECTION PROCESSOR
// ============================================================================

func TestKibanaCompat_NetworkDirection(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "inbound traffic - external to private",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "network_direction",
					SourceIP: "source_ip", DestinationIP: "destination_ip",
					InternalNetworks: []string{"private"}}},
			},
			input: map[string]any{"source_ip": "128.232.110.120", "destination_ip": "192.168.1.1"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "network.direction", "inbound")
			},
		},
		{
			name: "with custom target field",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "network_direction",
					SourceIP: "source_ip", DestinationIP: "destination_ip",
					TargetField:     "test_network_direction",
					InternalNetworks: []string{"private"}}},
			},
			input: map[string]any{"source_ip": "128.232.110.120", "destination_ip": "192.168.1.1"},
			check: func(t *testing.T, r *Document) {
				assertField(t, r, "test_network_direction", "inbound")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}

// ============================================================================
// DATE PROCESSOR (basic format tests only - no Elasticsearch needed)
// ============================================================================

func TestKibanaCompat_Date(t *testing.T) {
	tests := []struct {
		name  string
		steps []StreamlangStep
		input map[string]any
		check func(t *testing.T, result *Document)
	}{
		{
			name: "parse ISO8601 date",
			steps: []StreamlangStep{
				{Action: &ProcessorDef{Action: "date", From: "log.time", Formats: []string{"ISO8601"}}},
			},
			input: map[string]any{"log": map[string]any{"time": "2025-01-01T12:34:56.789Z"}},
			check: func(t *testing.T, r *Document) {
				doc := NewDocument(r)
				v, ok := doc.Get("@timestamp")
				if !ok {
					t.Fatal("@timestamp not found")
				}
				s := toString(v)
				if !strings.Contains(s, "2025-01-01") {
					t.Fatalf("@timestamp = %v, expected to contain 2025-01-01", s)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runPipeline(t, tt.steps, tt.input)
			tt.check(t, result)
		})
	}
}
