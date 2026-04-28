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
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/metadata"
)

// benchPipelines is the set of representative pipelines exercised by
// BenchmarkPipeline at every batch size. Names are chosen to match the
// implementation plan's table.
var benchPipelines = []struct {
	name  string
	steps []map[string]any
}{
	{
		name: "simple_set_remove",
		steps: []map[string]any{
			{"action": "set", "to": "attributes.processed_by", "value": "streamlang"},
			{"action": "remove", "from": "attributes.unwanted", "ignore_missing": true},
		},
	},
	{
		name: "grok_then_set",
		steps: []map[string]any{
			{
				"action":   "grok",
				"from":     "attributes.message",
				"patterns": []any{"%{IP:client_ip} %{WORD:method} %{URIPATHPARAM:path}"},
			},
			{"action": "set", "to": "attributes.parsed", "value": true},
		},
	},
	{
		name: "condition_guard_skip_all",
		steps: []map[string]any{
			{
				"condition": map[string]any{
					"field": "attributes.severity",
					"eq":    "PANIC", // never matches in our seed batches
					"steps": []any{
						map[string]any{"action": "set", "to": "attributes.escalate", "value": true},
					},
				},
			},
		},
	},
	{
		name: "routing_set",
		steps: []map[string]any{
			{"action": "set", "to": "attributes.stream.name", "value": "logs.routed"},
		},
	},
	{
		name: "kitchen_sink",
		steps: []map[string]any{
			{"action": "set", "to": "attributes.processed_by", "value": "streamlang"},
			{"action": "rename", "from": "attributes.message", "to": "attributes.original", "ignore_missing": true},
			{
				"action":         "grok",
				"from":           "attributes.original",
				"patterns":       []any{"%{IP:client_ip} %{WORD:method} %{URIPATHPARAM:path}"},
				"ignore_failure": true,
			},
			{"action": "uppercase", "from": "attributes.method", "ignore_missing": true},
			{"action": "convert", "from": "attributes.status", "type": "string", "ignore_missing": true},
			{
				"condition": map[string]any{
					"field": "attributes.method",
					"eq":    "GET",
					"steps": []any{
						map[string]any{"action": "set", "to": "attributes.idempotent", "value": true},
					},
				},
			},
			{"action": "remove", "from": "attributes.original", "ignore_missing": true},
		},
	},
}

var benchSizes = []int{1, 100, 1000}

// seedLogs builds a plog.Logs with `n` records, each carrying a small set of
// realistic attributes. The same input shape is used for every pipeline so
// the only variable across benchmarks is the pipeline itself.
func seedLogs(n int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("host.name", "h1")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("io.opentelemetry.runtime")
	for i := 0; i < n; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetSeverityText("INFO")
		attrs := lr.Attributes()
		attrs.PutStr("message", "192.168.1.1 GET /api/health")
		attrs.PutInt("status", 200)
		attrs.PutStr("severity", "INFO")
		attrs.PutStr("unwanted", "remove me")
	}
	return ld
}

func BenchmarkPipeline(b *testing.B) {
	for _, pl := range benchPipelines {
		for _, n := range benchSizes {
			b.Run(fmt.Sprintf("%s/n=%d", pl.name, n), func(b *testing.B) {
				cfg := &Config{Steps: pl.steps, FailureMode: FailureModeDrop}
				if err := cfg.Validate(); err != nil {
					b.Fatalf("config: %v", err)
				}
				p, err := newLogsProcessor(processortest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
				if err != nil {
					b.Fatalf("processor: %v", err)
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					ld := seedLogs(n)
					if err := p.ConsumeLogs(ctx, ld); err != nil {
						b.Fatalf("consume: %v", err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(n), "records/op")
			})
		}
	}
}

// BenchmarkProcessor is a per-processor microbench at batch size 1000. It
// isolates the cost of a single action when running over a realistic batch.
func BenchmarkProcessor(b *testing.B) {
	microbenches := []struct {
		name string
		step map[string]any
	}{
		{"set", map[string]any{"action": "set", "to": "attributes.x", "value": "y"}},
		{"remove", map[string]any{"action": "remove", "from": "attributes.unwanted", "ignore_missing": true}},
		{"rename", map[string]any{"action": "rename", "from": "attributes.message", "to": "attributes.original"}},
		{"uppercase", map[string]any{"action": "uppercase", "from": "attributes.severity"}},
		{"replace", map[string]any{"action": "replace", "from": "attributes.message", "pattern": "GET", "replacement": "POST"}},
		{"convert_to_string", map[string]any{"action": "convert", "from": "attributes.status", "type": "string"}},
		{"grok", map[string]any{
			"action":   "grok",
			"from":     "attributes.message",
			"patterns": []any{"%{IP:client_ip} %{WORD:method} %{URIPATHPARAM:path}"},
		}},
		{"dissect", map[string]any{
			"action":  "dissect",
			"from":    "attributes.message",
			"pattern": "%{ip} %{method} %{path}",
		}},
		{"split", map[string]any{
			"action":    "split",
			"from":      "attributes.message",
			"separator": " ",
			"to":        "attributes.parts",
		}},
		{"json_extract", map[string]any{
			"action": "json_extract",
			"field":  "attributes.body_json",
			"extractions": []any{
				map[string]any{"selector": "user.id", "target_field": "attributes.uid", "type": "integer"},
			},
		}},
	}
	for _, mb := range microbenches {
		b.Run(mb.name, func(b *testing.B) {
			cfg := &Config{Steps: []map[string]any{mb.step}, FailureMode: FailureModeDrop}
			if err := cfg.Validate(); err != nil {
				b.Fatalf("config: %v", err)
			}
			p, err := newLogsProcessor(processortest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
			if err != nil {
				b.Fatalf("processor: %v", err)
			}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ld := seedLogsForMicro(1000)
				if err := p.ConsumeLogs(ctx, ld); err != nil {
					b.Fatalf("consume: %v", err)
				}
			}
		})
	}
}

func seedLogsForMicro(n int) plog.Logs {
	ld := seedLogs(n)
	// Add a JSON body so json_extract has something to chew on.
	rl := ld.ResourceLogs().At(0)
	sl := rl.ScopeLogs().At(0)
	for i := 0; i < sl.LogRecords().Len(); i++ {
		sl.LogRecords().At(i).Attributes().PutStr("body_json", `{"user":{"id":42}}`)
	}
	return ld
}
