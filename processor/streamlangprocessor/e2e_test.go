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

package streamlangprocessor

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// helper: create a processor via the factory for a given backend and config.
func createTestProcessor(t *testing.T, cfg *Config, sink *consumertest.LogsSink) processor.Logs {
	t.Helper()
	p, err := newLogsProcessor(zap.NewNop(), cfg, sink)
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}
	return p
}

// helper: build plog.Logs with a single log record having the given attributes.
func buildLogs(attrs map[string]string) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	for k, v := range attrs {
		lr.Attributes().PutStr(k, v)
	}
	return ld
}

// helper: build plog.Logs with multiple log records.
func buildMultiLogs(records []map[string]string) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	for _, attrs := range records {
		lr := sl.LogRecords().AppendEmpty()
		for k, v := range attrs {
			lr.Attributes().PutStr(k, v)
		}
	}
	return ld
}

// helper: build plog.Logs in transport mode (body map).
func buildTransportLogs(bodyFields map[string]any) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	body := lr.Body()
	body.SetEmptyMap()
	setMapFromAny(body.Map(), bodyFields)
	return ld
}

func setMapFromAny(m pcommon.Map, fields map[string]any) {
	for k, v := range fields {
		switch val := v.(type) {
		case string:
			m.PutStr(k, val)
		case int:
			m.PutInt(k, int64(val))
		case int64:
			m.PutInt(k, val)
		case float64:
			m.PutDouble(k, val)
		case bool:
			m.PutBool(k, val)
		case map[string]any:
			nested := m.PutEmptyMap(k)
			setMapFromAny(nested, val)
		}
	}
}

// helper: extract all attributes from the first log record as a flattened map
// with dotted paths (e.g. "source.address" instead of nested maps).
func getAttrs(ld plog.Logs) map[string]any {
	lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	result := make(map[string]any)
	flattenMap(lr.Attributes(), "", result)
	return result
}

// helper: extract body map from the first log record as a flattened map.
func getBodyMap(ld plog.Logs) map[string]any {
	lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	result := make(map[string]any)
	flattenMap(lr.Body().Map(), "", result)
	return result
}

// flattenMap recursively flattens a pcommon.Map into dotted-path keys.
func flattenMap(m pcommon.Map, prefix string, out map[string]any) {
	m.Range(func(k string, v pcommon.Value) bool {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		if v.Type() == pcommon.ValueTypeMap {
			flattenMap(v.Map(), key, out)
		} else {
			out[key] = valueToGo(v)
		}
		return true
	})
}

func valueToGo(v pcommon.Value) any {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeInt:
		return v.Int()
	case pcommon.ValueTypeDouble:
		return v.Double()
	case pcommon.ValueTypeBool:
		return v.Bool()
	case pcommon.ValueTypeMap:
		m := make(map[string]any)
		v.Map().Range(func(k string, val pcommon.Value) bool {
			m[k] = valueToGo(val)
			return true
		})
		return m
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		result := make([]any, s.Len())
		for i := 0; i < s.Len(); i++ {
			result[i] = valueToGo(s.At(i))
		}
		return result
	default:
		return v.AsString()
	}
}

// TestE2E_ApacheLogPipeline tests a realistic Apache log processing pipeline
// using the full OTel processor path: factory → Config → processor → ConsumeLogs.
//
// Pipeline: grok parse Apache log → set severity from status → rename to ECS → drop health checks
func TestE2E_ApacheLogPipeline(t *testing.T) {
	trueVal := true

	cfg := &Config{
		Pipeline: DSL{
			Steps: []Step{
				// Step 1: Grok parse the Apache combined log format
				// Note: trivago/grok doesn't support dots in capture names,
				// so we use underscores and rename afterwards.
				{Processor: &ProcessorDef{
					Action: "grok",
					Field:  "message",
					Patterns: []string{
						`%{IP:source_address} - %{DATA:user_name} \[%{HTTPDATE:timestamp}\] "%{WORD:http_method} %{DATA:url_path} HTTP/%{NUMBER:http_version}" %{NUMBER:status_code} %{NUMBER:response_bytes}`,
					},
				}},
				// Step 2: Set severity based on status code (conditional)
				{Condition: &ConditionBlock{
					Condition: &Condition{
						Field:      "status_code",
						StartsWith: "5",
					},
					Steps: []Step{
						{Processor: &ProcessorDef{
							Action: "set",
							Field:  "log_level",
							Value:  "error",
						}},
					},
					Else: []Step{
						{Processor: &ProcessorDef{
							Action: "set",
							Field:  "log_level",
							Value:  "info",
						}},
					},
				}},
				// Step 3: Rename fields to ECS format
				{Processor: &ProcessorDef{
					Action: "rename",
					From:   "source_address",
					To:     "client.ip",
				}},
				{Processor: &ProcessorDef{
					Action: "rename",
					From:   "http_method",
					To:     "http.request.method",
				}},
				{Processor: &ProcessorDef{
					Action: "rename",
					From:   "url_path",
					To:     "url.path",
				}},
				// Step 4: Drop health check requests
				{Processor: &ProcessorDef{
					Action: "drop_document",
					Where: &Condition{
						Field: "url.path",
						Eq:    "/health",
					},
				}},
				// Step 5: Remove original message field
				{Processor: &ProcessorDef{
					Action:        "remove",
					Field:         "message",
					IgnoreMissing: &trueVal,
				}},
			},
		},
	}

	sink := &consumertest.LogsSink{}
	p := createTestProcessor(t, cfg, sink)

	// Test 1: Normal 200 OK request
	ld := buildLogs(map[string]string{
		"message": `192.168.1.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`,
	})

	err := p.ConsumeLogs(context.Background(), ld)
	if err != nil {
		t.Fatalf("ConsumeLogs failed: %v", err)
	}

	if sink.LogRecordCount() != 1 {
		t.Fatalf("expected 1 log record, got %d", sink.LogRecordCount())
	}

	attrs := getAttrs(sink.AllLogs()[0])

	// Verify grok extraction + rename to ECS
	if got := attrs["client.ip"]; got != "192.168.1.1" {
		t.Errorf("expected client.ip=192.168.1.1, got %v", got)
	}
	if got := attrs["http.request.method"]; got != "GET" {
		t.Errorf("expected http.request.method=GET, got %v", got)
	}
	if got := attrs["url.path"]; got != "/apache_pb.gif" {
		t.Errorf("expected url.path=/apache_pb.gif, got %v", got)
	}
	if got := attrs["status_code"]; got != "200" {
		t.Errorf("expected status_code=200, got %v", got)
	}
	// Verify severity was set to info (not 5xx)
	if got := attrs["log_level"]; got != "info" {
		t.Errorf("expected log_level=info, got %v", got)
	}
	// Verify rename happened (source_address → client.ip)
	if _, exists := attrs["source_address"]; exists {
		t.Error("expected source_address to be renamed away")
	}
	if _, exists := attrs["http_method"]; exists {
		t.Error("expected http_method to be renamed away")
	}
	// Verify message removed
	if _, exists := attrs["message"]; exists {
		t.Error("expected message field to be removed")
	}

	// Test 2: 500 error request should get severity=error
	sink.Reset()
	ld = buildLogs(map[string]string{
		"message": `10.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "POST /api/data HTTP/1.1" 500 0`,
	})
	err = p.ConsumeLogs(context.Background(), ld)
	if err != nil {
		t.Fatalf("ConsumeLogs failed: %v", err)
	}
	attrs = getAttrs(sink.AllLogs()[0])
	if got := attrs["log_level"]; got != "error" {
		t.Errorf("expected log_level=error for 500, got %v", got)
	}

	// Test 3: Health check request should be DROPPED
	sink.Reset()
	ld = buildLogs(map[string]string{
		"message": `10.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /health HTTP/1.1" 200 2`,
	})
	err = p.ConsumeLogs(context.Background(), ld)
	if err != nil {
		t.Fatalf("ConsumeLogs failed: %v", err)
	}
	// Health check should be dropped — 0 records in sink
	if sink.LogRecordCount() != 0 {
		t.Errorf("expected health check to be dropped, got %d records", sink.LogRecordCount())
	}

	// Test 4: Mixed batch — one normal, one health check
	sink.Reset()
	ld = buildMultiLogs([]map[string]string{
		{"message": `192.168.1.1 - admin [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.1" 200 1024`},
		{"message": `10.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /health HTTP/1.1" 200 2`},
		{"message": `172.16.0.5 - - [10/Oct/2000:13:55:36 -0700] "PUT /api/update HTTP/1.1" 503 0`},
	})
	err = p.ConsumeLogs(context.Background(), ld)
	if err != nil {
		t.Fatalf("ConsumeLogs failed: %v", err)
	}
	// Health check dropped, 2 remaining
	if sink.LogRecordCount() != 2 {
		t.Fatalf("expected 2 records (health dropped), got %d", sink.LogRecordCount())
	}
}

// TestE2E_TransportMode tests transport mode where the pipeline operates on the
// log body map (ES document) instead of log record attributes.
//
// Pipeline: rename fields, convert types, conditional set, remove_by_prefix
func TestE2E_TransportMode(t *testing.T) {
	cfg := &Config{
		TransportMode: true,
		Pipeline: DSL{
			Steps: []Step{
				// Step 1: Rename nested fields
				{Processor: &ProcessorDef{
					Action: "rename",
					From:   "src_ip",
					To:     "source.ip",
				}},
				{Processor: &ProcessorDef{
					Action: "rename",
					From:   "dst_ip",
					To:     "destination.ip",
				}},
				// Step 2: Convert bytes to integer
				{Processor: &ProcessorDef{
					Action: "convert",
					Field:  "bytes_sent",
					Type:   "integer",
				}},
				// Step 3: Conditional set based on value
				{Condition: &ConditionBlock{
					Condition: &Condition{
						Field: "event_type",
						Eq:    "login",
					},
					Steps: []Step{
						{Processor: &ProcessorDef{
							Action: "set",
							Field:  "event.category",
							Value:  "authentication",
						}},
					},
					Else: []Step{
						{Processor: &ProcessorDef{
							Action: "set",
							Field:  "event.category",
							Value:  "network",
						}},
					},
				}},
				// Step 4: Remove internal fields by prefix
				{Processor: &ProcessorDef{
					Action: "remove_by_prefix",
					Field:  "_internal",
				}},
			},
		},
	}

	sink := &consumertest.LogsSink{}
	p := createTestProcessor(t, cfg, sink)

	ld := buildTransportLogs(map[string]any{
		"src_ip":          "10.0.0.1",
		"dst_ip":          "192.168.1.100",
		"bytes_sent":      "4096",
		"event_type":      "login",
		"user":            "admin",
		"_internal.trace": "abc123",
		"_internal.shard": "shard-3",
	})

	err := p.ConsumeLogs(context.Background(), ld)
	if err != nil {
		t.Fatalf("ConsumeLogs failed: %v", err)
	}

	if sink.LogRecordCount() != 1 {
		t.Fatalf("expected 1 log record, got %d", sink.LogRecordCount())
	}

	body := getBodyMap(sink.AllLogs()[0])

	// Verify renames
	if got := body["source.ip"]; got != "10.0.0.1" {
		t.Errorf("expected source.ip=10.0.0.1, got %v", got)
	}
	if got := body["destination.ip"]; got != "192.168.1.100" {
		t.Errorf("expected destination.ip=192.168.1.100, got %v", got)
	}
	if _, exists := body["src_ip"]; exists {
		t.Error("expected src_ip to be renamed away")
	}
	if _, exists := body["dst_ip"]; exists {
		t.Error("expected dst_ip to be renamed away")
	}

	// Verify convert
	if got, ok := body["bytes_sent"]; !ok {
		t.Error("expected bytes_sent to exist")
	} else {
		// After convert to integer, should be int64
		if v, ok := got.(int64); !ok || v != 4096 {
			t.Errorf("expected bytes_sent=4096 (int64), got %v (%T)", got, got)
		}
	}

	// Verify conditional set
	if got := body["event.category"]; got != "authentication" {
		t.Errorf("expected event.category=authentication, got %v", got)
	}

	// Verify remove_by_prefix removed _internal fields
	for k := range body {
		if len(k) >= 9 && k[:9] == "_internal" {
			t.Errorf("expected _internal fields to be removed, found %q", k)
		}
	}

	// Verify non-prefixed field survives
	if got := body["user"]; got != "admin" {
		t.Errorf("expected user=admin, got %v", got)
	}
}

// TestE2E_CrossNamespaceAccess tests namespace-aware field access where a processor
// reads from resource attributes and writes to log record attributes.
func TestE2E_CrossNamespaceAccess(t *testing.T) {
	cfg := &Config{
		Pipeline: DSL{
			Steps: []Step{
				// Copy resource.host.name to attributes
				{Processor: &ProcessorDef{
					Action:   "set",
					Field:    "host_name",
					CopyFrom: "resource.host.name",
				}},
				// Copy resource.service.name to attributes
				{Processor: &ProcessorDef{
					Action:   "set",
					Field:    "service_name",
					CopyFrom: "resource.service.name",
				}},
				// Set severity text
				{Processor: &ProcessorDef{
					Action: "set",
					Field:  "severity",
					Value:  "ERROR",
				}},
				// Set timestamp
				{Processor: &ProcessorDef{
					Action: "set",
					Field:  "@timestamp",
					Value:  "2024-01-15T10:30:00Z",
				}},
			},
		},
	}

	sink := &consumertest.LogsSink{}
	p := createTestProcessor(t, cfg, sink)

	// Build logs with resource attributes
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("host.name", "web-server-01")
	rl.Resource().Attributes().PutStr("service.name", "my-service")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Attributes().PutStr("message", "something happened")

	err := p.ConsumeLogs(context.Background(), ld)
	if err != nil {
		t.Fatalf("ConsumeLogs failed: %v", err)
	}

	if sink.LogRecordCount() != 1 {
		t.Fatalf("expected 1 log record, got %d", sink.LogRecordCount())
	}

	result := sink.AllLogs()[0]
	lr = result.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)

	// Verify resource attribute was copied to log attributes
	v, ok := lr.Attributes().Get("host_name")
	if !ok || v.Str() != "web-server-01" {
		t.Errorf("expected host_name=web-server-01, got %v (ok=%v)", v, ok)
	}
	v, ok = lr.Attributes().Get("service_name")
	if !ok || v.Str() != "my-service" {
		t.Errorf("expected service_name=my-service, got %v (ok=%v)", v, ok)
	}

	// Verify severity was set
	if got := lr.SeverityText(); got != "ERROR" {
		t.Errorf("expected severity text=ERROR, got %v", got)
	}

	// Verify timestamp was set
	ts := lr.Timestamp().AsTime().UTC()
	expected := "2024-01-15T10:30:00Z"
	if got := ts.Format("2006-01-02T15:04:05Z"); got != expected {
		t.Errorf("expected timestamp=%s, got %s", expected, got)
	}

	// Verify original attribute still exists
	v, ok = lr.Attributes().Get("message")
	if !ok || v.Str() != "something happened" {
		t.Errorf("expected message=something happened, got %v", v)
	}
}

