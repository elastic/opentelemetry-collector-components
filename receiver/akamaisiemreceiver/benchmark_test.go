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

package akamaisiemreceiver

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// --- EmitEvents benchmarks: measure plog.Logs construction cost ---

func BenchmarkEmitEvents_Raw_1(b *testing.B)       { benchEmit(b, "raw", 1) }
func BenchmarkEmitEvents_Raw_1000(b *testing.B)    { benchEmit(b, "raw", 1000) }
func BenchmarkEmitEvents_Raw_100000(b *testing.B)  { benchEmit(b, "raw", 100000) }
func BenchmarkEmitEvents_Raw_600000(b *testing.B)  { benchEmit(b, "raw", 600000) }
func BenchmarkEmitEvents_OTel_1(b *testing.B)      { benchEmit(b, "otel", 1) }
func BenchmarkEmitEvents_OTel_1000(b *testing.B)   { benchEmit(b, "otel", 1000) }
func BenchmarkEmitEvents_OTel_100000(b *testing.B) { benchEmit(b, "otel", 100000) }
func BenchmarkEmitEvents_OTel_600000(b *testing.B) { benchEmit(b, "otel", 600000) }

// Dual mode benchmarks — measures parallel raw+OTel formatting cost.
func BenchmarkEmitEvents_Dual_1(b *testing.B)      { benchEmitDual(b, 1) }
func BenchmarkEmitEvents_Dual_1000(b *testing.B)   { benchEmitDual(b, 1000) }
func BenchmarkEmitEvents_Dual_100000(b *testing.B) { benchEmitDual(b, 100000) }

func benchEmitDual(b *testing.B, n int) {
	b.Helper()
	events := loadBenchEvents(b, n)
	rcv := benchReceiverDual(b)

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := rcv.emitEvents(ctx, events); err != nil {
			b.Fatal(err)
		}
	}
}

func benchEmit(b *testing.B, mapping string, n int) {
	b.Helper()
	events := loadBenchEvents(b, n)
	rcv := benchReceiver(b, mapping)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := rcv.emitEvents(context.Background(), events); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(n), "events/op")
}

// --- FullPoll benchmarks: HTTP fetch + NDJSON parse + emit ---

func BenchmarkFullPoll_Raw_100(b *testing.B)     { benchFullPoll(b, "raw", 100) }
func BenchmarkFullPoll_Raw_10000(b *testing.B)   { benchFullPoll(b, "raw", 10000) }
func BenchmarkFullPoll_Raw_100000(b *testing.B)  { benchFullPoll(b, "raw", 100000) }
func BenchmarkFullPoll_Raw_600000(b *testing.B)  { benchFullPoll(b, "raw", 600000) }
func BenchmarkFullPoll_OTel_100(b *testing.B)    { benchFullPoll(b, "otel", 100) }
func BenchmarkFullPoll_OTel_10000(b *testing.B)  { benchFullPoll(b, "otel", 10000) }
func BenchmarkFullPoll_OTel_100000(b *testing.B) { benchFullPoll(b, "otel", 100000) }
func BenchmarkFullPoll_OTel_600000(b *testing.B) { benchFullPoll(b, "otel", 600000) }

func benchFullPoll(b *testing.B, mapping string, eventCount int) {
	b.Helper()
	ndjsonBody := buildNDJSON(b, eventCount)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(ndjsonBody)
	}))
	defer server.Close()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "bench"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.OutputFormat = mapping
	cfg.PollInterval = 24 * time.Hour // don't auto-poll

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())
	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	if err != nil {
		b.Fatal(err)
	}
	if err := rcv.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		b.Fatal(err)
	}

	// Wait for first poll to complete.
	deadline := time.Now().Add(10 * time.Second)
	for sink.LogRecordCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	// Reset sink and timer for benchmark iterations.
	sink.Reset()

	// We can't re-trigger polls easily, so benchmark the emitEvents path directly.
	akRcv := rcv.(*akamaiReceiver)
	events := loadBenchEvents(b, eventCount)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink.Reset()
		if err := akRcv.emitEvents(context.Background(), events); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(eventCount), "events/op")

	_ = rcv.Shutdown(context.Background())
}

// --- helpers ---

// loadBenchEvents creates n copies of a realistic Akamai event from testdata.
func loadBenchEvents(b testing.TB, n int) []string {
	b.Helper()
	data, err := os.ReadFile("testdata/siem_response.ndjson")
	if err != nil {
		b.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	// Use only event lines (not offset context).
	var eventLines []string
	for _, line := range lines {
		if line == "" || strings.Contains(line, `"offset"`) {
			continue
		}
		eventLines = append(eventLines, line)
	}
	if len(eventLines) == 0 {
		b.Fatal("no events in testdata")
	}

	events := make([]string, n)
	for i := 0; i < n; i++ {
		events[i] = eventLines[i%len(eventLines)]
	}
	return events
}

// buildNDJSON creates NDJSON response body with n events + offset context.
func buildNDJSON(b testing.TB, n int) []byte {
	b.Helper()
	events := loadBenchEvents(b, n)
	var sb strings.Builder
	for _, e := range events {
		sb.WriteString(e)
		sb.WriteString("\n")
	}
	sb.WriteString(fmt.Sprintf(`{"offset":"bench-cursor","total":%d,"limit":%d}`, n, n+1))
	sb.WriteString("\n")
	return []byte(sb.String())
}

// benchReceiver creates a receiver wired to a nop consumer for benchmarking.
func benchReceiver(b testing.TB, mapping string) *akamaiReceiver {
	b.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = "https://bench.example.com"
	cfg.ConfigIDs = "bench"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.OutputFormat = mapping

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())
	rcv, err := newAkamaiReceiver(cfg, set)
	if err != nil {
		b.Fatal(err)
	}
	switch mapping {
	case "otel":
		rcv.setOTelConsumer(sink)
	default:
		rcv.setRawConsumer(sink)
	}
	return rcv
}

func benchReceiverDual(b *testing.B) *akamaiReceiver {
	b.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = "https://bench.example.com"
	cfg.ConfigIDs = "bench"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}

	rawSink := &consumertest.LogsSink{}
	otelSink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())
	rcv, err := newAkamaiReceiver(cfg, set)
	if err != nil {
		b.Fatal(err)
	}

	rawCfg := *cfg
	rawCfg.OutputFormat = "raw"
	rcv.setRawConsumer(rawSink)

	otelCfg := *cfg
	otelCfg.OutputFormat = "otel"
	rcv.setOTelConsumer(otelSink)

	return rcv
}
