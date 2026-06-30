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

package elasticapmintakereceiver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/elastic/opentelemetry-collector-components/internal/testutil"
	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/ndjsondecoder"
	"github.com/elastic/opentelemetry-lib/agentcfg"
)

// payloadGlobalLabelsNoShadow has 10 transactions with metadata global labels
// (tag1 string, tag2 numeric). No event shadows any global label.
var payloadGlobalLabelsNoShadow = []byte(`{"metadata": {"service": {"name": "bench-svc", "agent": {"name": "elastic-node", "version": "1.0.0"}}, "labels": {"tag1": "global_val", "tag2": 42}}}
{"transaction": {"id": "aa00000000000001", "trace_id": "aa00000000000001aa00000000000001", "name": "tx1", "type": "request", "duration": 1, "timestamp": 1000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000002", "trace_id": "aa00000000000002aa00000000000002", "name": "tx2", "type": "request", "duration": 1, "timestamp": 2000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000003", "trace_id": "aa00000000000003aa00000000000003", "name": "tx3", "type": "request", "duration": 1, "timestamp": 3000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000004", "trace_id": "aa00000000000004aa00000000000004", "name": "tx4", "type": "request", "duration": 1, "timestamp": 4000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000005", "trace_id": "aa00000000000005aa00000000000005", "name": "tx5", "type": "request", "duration": 1, "timestamp": 5000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000006", "trace_id": "aa00000000000006aa00000000000006", "name": "tx6", "type": "request", "duration": 1, "timestamp": 6000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000007", "trace_id": "aa00000000000007aa00000000000007", "name": "tx7", "type": "request", "duration": 1, "timestamp": 7000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000008", "trace_id": "aa00000000000008aa00000000000008", "name": "tx8", "type": "request", "duration": 1, "timestamp": 8000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000009", "trace_id": "aa00000000000009aa00000000000009", "name": "tx9", "type": "request", "duration": 1, "timestamp": 9000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa0000000000000a", "trace_id": "aa0000000000000aaa0000000000000a", "name": "tx10", "type": "request", "duration": 1, "timestamp": 10000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
`)

// payloadGlobalLabelsWithShadow has 10 transactions with metadata global
// labels (tag1 string, tag2 numeric). Transaction 3 shadows tag1 and
// transaction 7 shadows tag2, producing two overflow groups.
var payloadGlobalLabelsWithShadow = []byte(`{"metadata": {"service": {"name": "bench-svc", "agent": {"name": "elastic-node", "version": "1.0.0"}}, "labels": {"tag1": "global_val", "tag2": 42}}}
{"transaction": {"id": "aa00000000000001", "trace_id": "aa00000000000001aa00000000000001", "name": "tx1", "type": "request", "duration": 1, "timestamp": 1000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000002", "trace_id": "aa00000000000002aa00000000000002", "name": "tx2", "type": "request", "duration": 1, "timestamp": 2000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000003", "trace_id": "aa00000000000003aa00000000000003", "name": "tx3", "type": "request", "duration": 1, "timestamp": 3000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}, "context": {"tags": {"tag1": "event_val"}}}}
{"transaction": {"id": "aa00000000000004", "trace_id": "aa00000000000004aa00000000000004", "name": "tx4", "type": "request", "duration": 1, "timestamp": 4000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000005", "trace_id": "aa00000000000005aa00000000000005", "name": "tx5", "type": "request", "duration": 1, "timestamp": 5000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000006", "trace_id": "aa00000000000006aa00000000000006", "name": "tx6", "type": "request", "duration": 1, "timestamp": 6000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000007", "trace_id": "aa00000000000007aa00000000000007", "name": "tx7", "type": "request", "duration": 1, "timestamp": 7000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}, "context": {"tags": {"tag2": 99}}}}
{"transaction": {"id": "aa00000000000008", "trace_id": "aa00000000000008aa00000000000008", "name": "tx8", "type": "request", "duration": 1, "timestamp": 8000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000009", "trace_id": "aa00000000000009aa00000000000009", "name": "tx9", "type": "request", "duration": 1, "timestamp": 9000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa0000000000000a", "trace_id": "aa0000000000000aaa0000000000000a", "name": "tx10", "type": "request", "duration": 1, "timestamp": 10000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
`)

func BenchmarkProcessBatch(b *testing.B) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"global_labels_no_shadow", payloadGlobalLabelsNoShadow},
		{"global_labels_with_shadow", payloadGlobalLabelsWithShadow},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			factory := NewFactory()
			endpoint := testutil.GetAvailableLocalAddress(b)
			cfg := createDefaultConfig().(*Config)
			cfg.ServerConfig = confighttp.ServerConfig{
				NetAddr: confignet.AddrConfig{
					Endpoint:  endpoint,
					Transport: confignet.TransportTypeTCP,
				},
			}

			set := receivertest.NewNopSettings(metadata.Type)
			sink := new(consumertest.TracesSink)
			rcv, err := factory.CreateTraces(context.Background(), set, cfg, sink)
			if err != nil {
				b.Fatal(err)
			}
			if err := rcv.Start(context.Background(), componenttest.NewNopHost()); err != nil {
				b.Fatal(err)
			}
			defer func() { _ = rcv.Shutdown(context.Background()) }()

			url := "http://" + endpoint + intakeV2EventsPath

			// Warm up: send one request to ensure the server is ready.
			resp, err := http.Post(url, "application/x-ndjson", bytes.NewReader(tc.payload))
			if err != nil {
				b.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()

			sink.Reset()
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				resp, err := http.Post(url, "application/x-ndjson", bytes.NewReader(tc.payload))
				if err != nil {
					b.Fatal(err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		})
	}
}

// loadTestdata reads a testdata ndjson file for use as a benchmark payload.
func loadTestdata(b *testing.B, name string) []byte {
	b.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		b.Fatal(err)
	}
	return data
}

// newBenchReceiver constructs an elasticAPMIntakeReceiver wired to no-op
// consumers for every signal type. The HTTP server is intentionally not
// started — callers drive the receiver through the elasticapm processor
// directly to isolate intake processing from transport overhead.
func newBenchReceiver(b *testing.B) *elasticAPMIntakeReceiver {
	b.Helper()
	cfg := createDefaultConfig().(*Config)
	set := receivertest.NewNopSettings(metadata.Type)
	rcv, err := newElasticAPMIntakeReceiver(
		func(context.Context, component.Host) (agentcfg.Fetcher, error) { return nil, nil },
		cfg, set,
	)
	if err != nil {
		b.Fatal(err)
	}
	nop := consumertest.NewNop()
	rcv.nextLogs = nop
	rcv.nextMetrics = nop
	rcv.nextTraces = nop
	return rcv
}

func runHandleStream(b *testing.B, rcv *elasticAPMIntakeReceiver, payload []byte) {
	b.Helper()
	ctx := withECSMappingMode(context.Background(), false)
	consumer := ndjsondecoder.BatchConsumer(func(ctx context.Context, ld *plog.Logs, md *pmetric.Metrics, td *ptrace.Traces) error {
		return errors.Join(rcv.consumeOTel(ctx, ld, md, td)...)
	})

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.ReportAllocs()

	reader := bytes.NewReader(payload)
	for i := 0; i < b.N; i++ {
		reader.Reset(payload)
		_, streamErrs := ndjsondecoder.HandleStream(ctx, reader, rcv.cfg.BatchSize, rcv.cfg.MaxEventSize, rcv.settings.Logger, consumer)
		if len(streamErrs) != 0 {
			b.Fatalf("unexpected stream errors: %v", streamErrs)
		}
	}
}

// BenchmarkHandleStream measures intake throughput per event-type using the
// existing testdata files as realistic payloads. HTTP layer is bypassed; the
// bench drives the same pipeline as the HTTP handler (NDJSON decode →
// processBatch → no-op consumer).
func BenchmarkHandleStream(b *testing.B) {
	cases := []struct {
		name string
		file string
	}{
		{"transactions", "transactions.ndjson"},
		{"spans", "spans.ndjson"},
		{"transactions_spans", "transactions_spans.ndjson"},
		{"errors", "errors.ndjson"},
		{"logs", "logs.ndjson"},
		{"metricsets", "metricsets.ndjson"},
		{"histograms", "multiple_histogram_metrics_samples.ndjson"},
		{"metric_global_label_shadow", "metric_global_label_shadow.ndjson"},
	}
	for _, tc := range cases {
		payload := loadTestdata(b, tc.file)
		b.Run(tc.name, func(b *testing.B) {
			rcv := newBenchReceiver(b)
			runHandleStream(b, rcv, payload)
		})
	}
}

// BenchmarkHandleStreamHTTP measures the full HTTP handler path — HTTP
// request creation, NDJSON decode, pdata conversion, no-op consumer — using
// httptest to eliminate TCP/socket overhead. Compare with BenchmarkHandleStream
// (raw pipeline, no HTTP) to isolate handler overhead.
func BenchmarkHandleStreamHTTP(b *testing.B) {
	cases := []struct {
		name string
		file string
	}{
		{"transactions", "transactions.ndjson"},
		{"spans", "spans.ndjson"},
		{"transactions_spans", "transactions_spans.ndjson"},
		{"errors", "errors.ndjson"},
		{"logs", "logs.ndjson"},
		{"metricsets", "metricsets.ndjson"},
		{"histograms", "multiple_histogram_metrics_samples.ndjson"},
		{"metric_global_label_shadow", "metric_global_label_shadow.ndjson"},
	}
	for _, tc := range cases {
		payload := loadTestdata(b, tc.file)
		b.Run(tc.name, func(b *testing.B) {
			rcv := newBenchReceiver(b)
			handler := rcv.newElasticAPMEventsHandler(func(req *http.Request) context.Context {
				return withECSMappingMode(req.Context(), false)
			})
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				req := httptest.NewRequest(http.MethodPost, intakeV2EventsPath, bytes.NewReader(payload))
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				if rec.Code != http.StatusAccepted {
					b.Fatalf("unexpected status code: %d body: %s", rec.Code, rec.Body.String())
				}
			}
		})
	}
}

// BenchmarkHandleStreamGlobalLabels measures the cost of the global-label
// shadowing path using the in-file synthetic payloads.
func BenchmarkHandleStreamGlobalLabels(b *testing.B) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{name: "no_shadow", payload: payloadGlobalLabelsNoShadow},
		{name: "with_shadow", payload: payloadGlobalLabelsWithShadow},
		{name: "70_global_labels_with_shadow", payload: loadTestdata(b, "transactions_70_global_labels.ndjson")},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			rcv := newBenchReceiver(b)
			runHandleStream(b, rcv, tc.payload)
		})
	}
}

// BenchmarkHandleStreamSize sweeps payload size to expose per-event scaling
// behavior: how does CPU/alloc-per-event change as batches grow? The handler's
// internal batchSize is 10, so 10/100/1000 events exercise 1/10/100 batch
// flushes per request.
func BenchmarkHandleStreamSize(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		payload := generateTransactionPayload(n)
		b.Run("transactions/"+strconv.Itoa(n), func(b *testing.B) {
			rcv := newBenchReceiver(b)
			runHandleStream(b, rcv, payload)
		})
	}
}

// BenchmarkHandleStreamMixed measures a representative mixed workload
// (transactions + spans + errors + logs + metricsets) of varying sizes,
// repeated to reach the requested event count. This is closer to real
// agent traffic than the per-type benchmarks.
func BenchmarkHandleStreamMixed(b *testing.B) {
	for _, n := range []int{50, 500} {
		payload := generateMixedPayload(n)
		b.Run("mixed/"+strconv.Itoa(n), func(b *testing.B) {
			rcv := newBenchReceiver(b)
			runHandleStream(b, rcv, payload)
		})
	}
}

// generateTransactionPayload builds an NDJSON payload with a fixed metadata
// header followed by n transaction events. Each event has a unique
// id/trace_id so downstream grouping does not deduplicate.
func generateTransactionPayload(n int) []byte {
	var buf bytes.Buffer
	buf.WriteString(benchMetadataLine)
	buf.WriteByte('\n')
	for i := range n {
		fmt.Fprintf(&buf,
			`{"transaction": {"id": %q, "trace_id": %q, "name": "tx", "type": "request", "duration": 1, "timestamp": %d, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}`+"\n",
			fmt.Sprintf("aa%014x", i+1),
			fmt.Sprintf("aa%014xaa%014x", i+1, i+1),
			1_000_000+uint64(i)*1_000,
		)
	}
	return buf.Bytes()
}

// generateMixedPayload builds an NDJSON payload with a mix of event types
// (transaction, span, error, log, metricset), repeated to reach roughly n
// events total. Counts within each repeat block are kept proportional to
// typical agent traffic.
func generateMixedPayload(n int) []byte {
	// Each repeat block emits 20 events. Roughly: 40% transactions, 40%
	// spans, 5% errors, 5% logs, 10% metricsets.
	repeat := max(n/20, 1)
	var buf bytes.Buffer
	buf.WriteString(benchMetadataLine)
	buf.WriteByte('\n')
	for r := range repeat {
		base := uint64(r * 20)
		for i := range 8 {
			txSeq := base + uint64(i) + 1
			txID := fmt.Sprintf("%016x", txSeq)
			traceID := fmt.Sprintf("%032x", txSeq)
			fmt.Fprintf(&buf,
				`{"transaction": {"id": %q, "trace_id": %q, "name": "tx", "type": "request", "duration": 1, "timestamp": %d, "outcome": "success", "sampled": true, "span_count": {"started": 1}}}`+"\n",
				txID,
				traceID,
				1_000_000+txSeq*1_000,
			)
		}
		for i := range 8 {
			txSeq := base + uint64(i) + 1
			spanSeq := base + uint64(i) + 0x1001
			txID := fmt.Sprintf("%016x", txSeq)
			fmt.Fprintf(&buf,
				`{"span": {"id": %q, "trace_id": %q, "transaction_id": %q, "parent_id": %q, "name": "SELECT *", "type": "db.postgresql.query", "start": 1, "duration": 2, "timestamp": %d}}`+"\n",
				fmt.Sprintf("%016x", spanSeq),
				fmt.Sprintf("%032x", txSeq),
				txID,
				txID,
				1_000_000+txSeq*1_000+1,
			)
		}
		errSeq := base + 0x2001
		errTxSeq := base + 1
		errTxID := fmt.Sprintf("%016x", errTxSeq)
		fmt.Fprintf(&buf,
			`{"error": {"id": %q, "trace_id": %q, "transaction_id": %q, "parent_id": %q, "timestamp": %d, "log": {"message": "boom"}}}`+"\n",
			fmt.Sprintf("%032x", errSeq),
			fmt.Sprintf("%032x", errTxSeq),
			errTxID,
			errTxID,
			1_000_000+base*1_000+2,
		)
		fmt.Fprintf(&buf,
			`{"log": {"message": "log %d", "@timestamp": %d}}`+"\n",
			r, 1_000_000_000+base*1_000,
		)
		fmt.Fprintf(&buf,
			`{"metricset": {"samples": {"system.cpu.total.norm.pct": {"value": 0.5}}, "timestamp": %d}}`+"\n",
			1_000_000+base*1_000+3,
		)
		fmt.Fprintf(&buf,
			`{"metricset": {"samples": {"transaction.duration.sum.us": {"value": 1234}, "transaction.duration.count": {"value": 2}}, "transaction": {"name": "GET /", "type": "request"}, "timestamp": %d}}`+"\n",
			1_000_000+base*1_000+4,
		)
	}
	return buf.Bytes()
}

// benchMetadataLine is a minimal but realistic metadata line including a
// service block, an agent block, and a couple of global labels (one string,
// one numeric) that exercise the global-label tracking path.
const benchMetadataLine = `{"metadata": {"service": {"name": "bench-svc", "version": "1.0.0", "language": {"name": "go", "version": "1.22"}, "runtime": {"name": "gc", "version": "1.22"}, "agent": {"name": "elastic-go", "version": "1.0.0"}}, "system": {"hostname": "bench-host", "architecture": "x64", "platform": "linux"}, "labels": {"deployment": "prod", "shard": 7}}}`
