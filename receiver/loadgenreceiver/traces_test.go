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

package loadgenreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver"

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

func TestTracesGenerator_doneCh(t *testing.T) {
	const maxReplay = 2
	for _, concurrency := range []int{1, 2} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			doneCh := make(chan Stats)
			sink := &consumertest.TracesSink{}
			cfg := createDefaultReceiverConfig(nil, nil, doneCh, nil)
			cfg.(*Config).Traces.MaxReplay = maxReplay
			cfg.(*Config).Concurrency = concurrency
			r, _ := createTracesReceiver(context.Background(), receiver.Settings{
				ID: component.ID{},
				TelemetrySettings: component.TelemetrySettings{
					Logger: zap.NewNop(),
				},
				BuildInfo: component.BuildInfo{},
			}, cfg, sink)
			err := r.Start(context.Background(), componenttest.NewNopHost())
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, r.Shutdown(context.Background()))
			}()
			stats := <-doneCh
			want := maxReplay * bytes.Count(demoTraces, []byte("\n"))
			assert.Equal(t, want, stats.Requests)
			assert.Equal(t, want, len(sink.AllTraces()))
			assert.Equal(t, sink.SpanCount(), stats.Spans)
		})
	}
}

// Two JSONL lines sharing one trace: line A holds the root span and a child
// (with a link back to the root), line B holds a second child of the same
// trace. Exercises cross-line trace grouping within a replay pass.
const (
	rewriteTraceID = "0102030405060708090a0b0c0d0e0f10"
	rewriteRootID  = "0101010101010101"
	rewriteLineA   = `{"resourceSpans":[{"resource":{},"scopeSpans":[{"spans":[` +
		`{"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0101010101010101","name":"root","startTimeUnixNano":"1","endTimeUnixNano":"2","kind":2},` +
		`{"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0202020202020202","parentSpanId":"0101010101010101","name":"child1","startTimeUnixNano":"1","endTimeUnixNano":"2","kind":3,"links":[{"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0101010101010101"}]}` +
		`]}]}]}`
	rewriteLineB = `{"resourceSpans":[{"resource":{},"scopeSpans":[{"spans":[` +
		`{"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0303030303030303","parentSpanId":"0101010101010101","name":"child2","startTimeUnixNano":"1","endTimeUnixNano":"2","kind":3}` +
		`]}]}]}`
)

func writeTracesJSONL(t *testing.T, lines ...string) string {
	t.Helper()
	filePath := filepath.Join(t.TempDir(), strings.ReplaceAll(t.Name(), "/", "_")+".jsonl")
	require.NoError(t, os.WriteFile(filePath, []byte(strings.Join(lines, "\n")), 0o644))
	return filePath
}

func startTracesGenerator(t *testing.T, cfg component.Config) *consumertest.TracesSink {
	t.Helper()
	sink := &consumertest.TracesSink{}
	r, err := createTracesReceiver(context.Background(), receiver.Settings{
		ID: component.ID{},
		TelemetrySettings: component.TelemetrySettings{
			Logger: zap.NewNop(),
		},
		BuildInfo: component.BuildInfo{},
	}, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		assert.NoError(t, r.Shutdown(context.Background()))
	})
	return sink
}

func spansByName(all []ptrace.Traces) map[string]ptrace.Span {
	out := map[string]ptrace.Span{}
	for _, td := range all {
		rm := td.ResourceSpans()
		for i := 0; i < rm.Len(); i++ {
			for j := 0; j < rm.At(i).ScopeSpans().Len(); j++ {
				spans := rm.At(i).ScopeSpans().At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					out[spans.At(k).Name()] = spans.At(k)
				}
			}
		}
	}
	return out
}

func TestTracesGenerator_RewriteIDs(t *testing.T) {
	doneCh := make(chan Stats)
	cfg := createDefaultReceiverConfig(nil, nil, doneCh, nil)
	cfg.(*Config).Traces.JsonlFile = JsonlFile{Path: writeTracesJSONL(t, rewriteLineA, rewriteLineB)}
	cfg.(*Config).Traces.MaxReplay = 2
	cfg.(*Config).Traces.RewriteIDs = true
	cfg.(*Config).Concurrency = 1
	// The sink retains payloads, so the pdata-reuse optimization would let
	// later emissions overwrite them.
	cfg.(*Config).DisablePdataReuse = true

	sink := startTracesGenerator(t, cfg)
	<-doneCh

	all := sink.AllTraces()
	require.Len(t, all, 4) // 2 lines x 2 passes

	origTraceID := mustTraceID(t, rewriteTraceID)
	origRootID := mustSpanID(t, rewriteRootID)

	// Payloads arrive in order: pass 0 (line A, line B), pass 1 (line A, line B).
	for pass := 0; pass < 2; pass++ {
		byName := spansByName(all[pass*2 : pass*2+2])
		root, child1, child2 := byName["root"], byName["child1"], byName["child2"]

		assert.NotEqual(t, origTraceID, root.TraceID(), "trace ID must be rewritten")
		assert.Equal(t, root.TraceID(), child1.TraceID(), "same-line spans keep sharing a trace ID")
		assert.Equal(t, root.TraceID(), child2.TraceID(), "cross-line spans of one trace keep sharing a trace ID within a pass")

		assert.NotEqual(t, origRootID, root.SpanID(), "span ID must be rewritten")
		assert.True(t, root.ParentSpanID().IsEmpty(), "root span must stay a root span")
		assert.Equal(t, root.SpanID(), child1.ParentSpanID(), "parent-child links must be preserved")
		assert.Equal(t, root.SpanID(), child2.ParentSpanID(), "parent-child links must be preserved across lines")

		require.Equal(t, 1, child1.Links().Len())
		assert.Equal(t, root.TraceID(), child1.Links().At(0).TraceID(), "link trace ID must follow the rewrite")
		assert.Equal(t, root.SpanID(), child1.Links().At(0).SpanID(), "link span ID must follow the rewrite")
	}

	pass0 := spansByName(all[0:2])
	pass1 := spansByName(all[2:4])
	assert.NotEqual(t, pass0["root"].TraceID(), pass1["root"].TraceID(), "each pass must produce new trace IDs")
}

func TestTracesGenerator_LateSpans(t *testing.T) {
	doneCh := make(chan Stats)
	cfg := createDefaultReceiverConfig(nil, nil, doneCh, nil)
	cfg.(*Config).Traces.JsonlFile = JsonlFile{Path: writeTracesJSONL(t, rewriteLineA, rewriteLineB)}
	cfg.(*Config).Traces.MaxReplay = 1
	cfg.(*Config).Traces.RewriteIDs = true
	cfg.(*Config).Traces.LateSpans = &LateSpansConfig{
		Fraction: 1,
		Spans:    1,
		DelayMin: 10 * time.Millisecond,
		DelayMax: 20 * time.Millisecond,
	}
	cfg.(*Config).Concurrency = 1
	cfg.(*Config).DisablePdataReuse = true

	start := time.Now()
	sink := startTracesGenerator(t, cfg)
	stats := <-doneCh
	elapsed := time.Since(start)

	// Line A (2 spans) is split into main (1 span) + late (1 span). Line B has
	// a single span, so holding it back would delay the whole payload rather
	// than produce a late span; it must be emitted unsplit.
	all := sink.AllTraces()
	require.Len(t, all, 3)
	assert.Equal(t, 3, stats.Spans)
	assert.GreaterOrEqual(t, elapsed, 10*time.Millisecond, "done only after late spans are emitted")

	byName := spansByName(all)
	require.Len(t, byName, 3, "no span may be lost or duplicated by the split")
	assert.Equal(t, byName["root"].TraceID(), byName["child1"].TraceID(),
		"late span must keep the rewritten trace ID of its payload")

	var spanCounts []int
	for _, td := range all {
		spanCounts = append(spanCounts, td.SpanCount())
	}
	assert.ElementsMatch(t, []int{1, 1, 1}, spanCounts)
}

func TestTracesGenerator_LateSpansValidate(t *testing.T) {
	for name, tc := range map[string]struct {
		late    LateSpansConfig
		wantErr string
	}{
		"fraction_negative": {LateSpansConfig{Fraction: -0.1}, "fraction must be in [0, 1]"},
		"fraction_above_1":  {LateSpansConfig{Fraction: 1.1}, "fraction must be in [0, 1]"},
		"spans_negative":    {LateSpansConfig{Spans: -1}, "spans must be >= 0"},
		"delay_min_negative": {LateSpansConfig{
			DelayMin: -time.Second,
		}, "delay_min must be >= 0"},
		"delay_max_below_min": {LateSpansConfig{
			DelayMin: time.Second, DelayMax: time.Millisecond,
		}, "delay_max must be >= delay_min"},
		"valid": {LateSpansConfig{
			Fraction: 0.5, Spans: 2, DelayMin: time.Second, DelayMax: time.Minute,
		}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := createDefaultReceiverConfig(nil, nil, nil, nil)
			cfg.(*Config).Traces.LateSpans = &tc.late
			err := cfg.(*Config).Validate()
			if tc.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.wantErr)
			}
		})
	}
}

func mustTraceID(t *testing.T, hexStr string) pcommon.TraceID {
	t.Helper()
	var id pcommon.TraceID
	b, err := hex.DecodeString(hexStr)
	require.NoError(t, err)
	copy(id[:], b)
	return id
}

func mustSpanID(t *testing.T, hexStr string) pcommon.SpanID {
	t.Helper()
	var id pcommon.SpanID
	b, err := hex.DecodeString(hexStr)
	require.NoError(t, err)
	copy(id[:], b)
	return id
}

func TestTracesGenerator_MaxBufferSizeAttr(t *testing.T) {
	dummyData := `{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"my.service"}}]},"scopeSpans":[{"spans":[{"traceId":"5B8EFFF798038103D269B633813FC60C","spanId":"EEE19B7EC3C1B174","parentSpanId":"EEE19B7EC3C1B173","name":"I'm a server span","startTimeUnixNano":"1727411470107912000","endTimeUnixNano":"1727411470107912000","kind":2,"attributes":[{"key":"my.span.attr","value":{"stringValue":"some value"}}]}]}]}]}`
	for _, maxBufferSize := range []int{0, 10} {
		t.Run(fmt.Sprintf("max_buffer_size=%d", maxBufferSize), func(t *testing.T) {
			dir := t.TempDir()
			filePath := filepath.Join(dir, strings.ReplaceAll(t.Name(), "/", "_")+".jsonl")
			content := []byte(dummyData)
			require.NoError(t, os.WriteFile(filePath, content, 0o644))

			doneCh := make(chan Stats)
			cfg := createDefaultReceiverConfig(nil, nil, doneCh, nil)
			cfg.(*Config).Traces.MaxBufferSize = maxBufferSize
			cfg.(*Config).Traces.JsonlFile = JsonlFile{Path: filePath}

			_, err := createTracesReceiver(context.Background(), receiver.Settings{
				ID: component.ID{},
				TelemetrySettings: component.TelemetrySettings{
					Logger: zap.NewNop(),
				},
				BuildInfo: component.BuildInfo{},
			}, cfg, consumertest.NewNop())
			if maxBufferSize == 0 {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, bufio.ErrTooLong.Error())
			}
		})
	}
}
