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

package ndjsondecoder

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// collectOutputs runs HandleStream and returns the JSON-marshaled pdata
// produced by every batch flush, so outputs can be compared byte-for-byte
// across runs.
func collectOutputs(t *testing.T, payload []byte, batchBytes, maxLineLength int) (int, []error, []string) {
	t.Helper()
	var out []string
	tm := ptrace.JSONMarshaler{}
	lm := plog.JSONMarshaler{}
	mm := pmetric.JSONMarshaler{}
	consumer := func(_ context.Context, ld *plog.Logs, md *pmetric.Metrics, td *ptrace.Traces) error {
		if td != nil {
			b, err := tm.MarshalTraces(*td)
			require.NoError(t, err)
			out = append(out, string(b))
		}
		if ld != nil {
			b, err := lm.MarshalLogs(*ld)
			require.NoError(t, err)
			out = append(out, string(b))
		}
		if md != nil {
			b, err := mm.MarshalMetrics(*md)
			require.NoError(t, err)
			out = append(out, string(b))
		}
		return nil
	}
	accepted, errs := HandleStream(context.Background(), bytes.NewReader(payload), Config{BatchBytes: batchBytes, MaxLineLength: maxLineLength}, zap.NewNop(), consumer)
	return accepted, errs, out
}

// txStreamPayload builds a metadata line followed by n transaction lines of
// identical length (fixed-width ids), so byte thresholds map to exact event
// counts. It returns the payload and the length of one event line (without
// the trailing newline, matching how HandleStream counts batch bytes).
func txStreamPayload(n int) ([]byte, int) {
	var buf bytes.Buffer
	buf.WriteString(`{"metadata": {"service": {"name": "svc", "agent": {"name": "elastic-node", "version": "1.0.0"}}}}` + "\n")
	lineLen := 0
	for i := range n {
		start := buf.Len()
		fmt.Fprintf(&buf,
			`{"transaction": {"id": %q, "trace_id": %q, "name": "tx", "type": "request", "duration": 1, "timestamp": 1000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}`+"\n",
			fmt.Sprintf("aa%014x", i+1),
			fmt.Sprintf("aa%014xaa%014x", i+1, i+1),
		)
		lineLen = buf.Len() - start - 1
	}
	return buf.Bytes(), lineLen
}

// slowLineReader serves the stream one line at a time, sleeping before every
// line after the first to simulate a slow agent stream.
type slowLineReader struct {
	lines [][]byte
	delay time.Duration
	i     int
	off   int // read offset within lines[i]
}

func (r *slowLineReader) Read(p []byte) (int, error) {
	for r.i < len(r.lines) && r.off == len(r.lines[r.i]) {
		r.i++
		r.off = 0
	}
	if r.i >= len(r.lines) {
		return 0, io.EOF
	}
	if r.off == 0 && r.i > 0 {
		time.Sleep(r.delay)
	}
	n := copy(p, r.lines[r.i][r.off:])
	r.off += n
	return n, nil
}

// TestHandleStreamBatchBounds verifies how the byte and age bounds group
// events into batches. Exact flush shapes are asserted where timing cannot
// affect them; the age-bound case only asserts that a mid-stream flush
// happened, since scheduling delays can only add flushes, never remove them.
func TestHandleStreamBatchBounds(t *testing.T) {
	for _, tc := range []struct {
		name          string
		events        int
		batchBytes    func(lineLen int) int
		interval      time.Duration
		delay         time.Duration // >0 streams lines through slowLineReader
		maxLineLength int           // 0 means 1<<20
		wantTooLong   int           // expected number of ErrLineTooLong errors
		wantExact     []int         // exact per-flush event counts, when set
		wantMin       int           // otherwise: minimum number of flushes
	}{
		{
			// One byte over a single line: the byte bound is crossed as the
			// second event of each batch is appended, grouping events in pairs.
			name:       "byte bound groups events in pairs",
			events:     5,
			batchBytes: func(lineLen int) int { return lineLen + 1 },
			wantExact:  []int{2, 2, 1},
		},
		{
			// Events arrive far apart relative to the interval, so buffered
			// events exceed the age bound before the stream ends and must be
			// flushed mid-stream rather than in one end-of-stream batch.
			name:       "age bound flushes buffered events mid-stream",
			events:     3,
			batchBytes: func(int) int { return 1 << 20 },
			interval:   5 * time.Millisecond,
			delay:      50 * time.Millisecond,
			wantMin:    2,
		},
		{
			name:       "age bound disabled buffers slow stream until EOF",
			events:     3,
			batchBytes: func(int) int { return 1 << 20 },
			delay:      20 * time.Millisecond,
			wantExact:  []int{3},
		},
		{
			// maxLineLength sits between the metadata line and the (longer)
			// event lines, so metadata decodes but every event line comes
			// back truncated with ErrLineTooLong: each must be rejected
			// without being accepted, counted toward batch bytes, or
			// breaking the decode of subsequent lines.
			name:          "event lines over max length are rejected",
			events:        3,
			batchBytes:    func(int) int { return 1 << 20 },
			maxLineLength: 128,
			wantTooLong:   3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, lineLen := txStreamPayload(tc.events)
			maxLineLength := tc.maxLineLength
			if maxLineLength == 0 {
				maxLineLength = 1 << 20
			}
			var body io.Reader = bytes.NewReader(payload)
			if tc.delay > 0 {
				body = &slowLineReader{lines: bytes.SplitAfter(payload, []byte("\n")), delay: tc.delay}
			}

			var flushes []int
			consumer := func(_ context.Context, ld *plog.Logs, md *pmetric.Metrics, td *ptrace.Traces) error {
				n := 0
				if td != nil {
					n += td.SpanCount()
				}
				if ld != nil {
					n += ld.LogRecordCount()
				}
				if md != nil {
					n += md.DataPointCount()
				}
				flushes = append(flushes, n)
				return nil
			}

			accepted, errs := HandleStream(context.Background(), body,
				Config{BatchBytes: tc.batchBytes(lineLen), FlushInterval: tc.interval, MaxLineLength: maxLineLength},
				zap.NewNop(), consumer)

			if tc.wantTooLong > 0 {
				require.Len(t, errs, tc.wantTooLong, "errors: %v", errs)
				require.ErrorIs(t, errors.Join(errs...), ErrLineTooLong)
			} else {
				require.Empty(t, errs)
			}
			// Every too-long line is one rejected event.
			wantAccepted := tc.events - tc.wantTooLong
			require.Equal(t, wantAccepted, accepted)
			total := 0
			for _, n := range flushes {
				total += n
			}
			require.Equal(t, wantAccepted, total, "events across flushes: %v", flushes)
			if tc.wantExact != nil {
				require.Equal(t, tc.wantExact, flushes)
			} else {
				require.GreaterOrEqual(t, len(flushes), tc.wantMin, "expected a mid-stream flush from the age bound, got: %v", flushes)
			}
		})
	}
}

// TestHandleStreamAcceptedCountsOnlyFlushedEvents cancels the request context
// from the consumer after the first flush: events still buffered when the
// stream aborts must not be reported as accepted.
func TestHandleStreamAcceptedCountsOnlyFlushedEvents(t *testing.T) {
	payload, lineLen := txStreamPayload(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	flushed := 0
	consumer := func(_ context.Context, _ *plog.Logs, _ *pmetric.Metrics, td *ptrace.Traces) error {
		if td != nil {
			flushed += td.SpanCount()
		}
		cancel()
		return nil
	}

	accepted, errs := HandleStream(ctx, bytes.NewReader(payload),
		Config{BatchBytes: lineLen + 1, MaxLineLength: 1 << 20},
		zap.NewNop(), consumer)

	require.ErrorIs(t, errors.Join(errs...), context.Canceled)
	require.Equal(t, 2, accepted, "only the first flushed pair may count as accepted")
	require.Equal(t, flushed, accepted)
}

// TestHandleStreamPoolNoCrossRequestLeak verifies that reusing pooled
// decoders cannot mix data between requests. A "victim" payload is decoded
// on a cold pool, then the pool is deliberately poisoned with payloads that
// smear the reused bufio buffer with distinctive bytes and leave the
// LineReader in its dirty skip-state (request ending mid-oversized-line).
// The victim is then decoded again on the poisoned pool and the outputs
// must be identical byte-for-byte.
func TestHandleStreamPoolNoCrossRequestLeak(t *testing.T) {
	const maxLineLength = 4096
	poison := strings.Repeat("P", 3*maxLineLength)

	victim := []byte(`{"metadata": {"service": {"name": "victim-svc", "agent": {"name": "elastic-node", "version": "1.0.0"}, "environment": "prod"}, "labels": {"k": "victim-label"}}}
{"transaction": {"id": "bb00000000000001", "trace_id": "bb00000000000001bb00000000000001", "name": "victim-tx", "type": "request", "duration": 1, "timestamp": 1000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"metricset": {"timestamp": 2000000, "samples": {"victim.metric": {"value": 1}}}}
{"log": {"@timestamp": 3000000, "message": "victim-message"}}
`)
	// Poison payload: distinctive long strings that fill the pooled buffer,
	// terminated by an over-long line with no trailing newline so the
	// decoder is returned to the pool mid-skip (skip=true, non-EOF error
	// state, dirty latestLine).
	poisonPayload := []byte(`{"metadata": {"service": {"name": "` + poison[:512] + `", "agent": {"name": "elastic-node", "version": "9.9.9"}}, "labels": {"k": "` + poison[:1024] + `"}}}
{"transaction": {"id": "cc00000000000001", "trace_id": "cc00000000000001cc00000000000001", "name": "` + poison[:1024] + `", "type": "request", "duration": 1, "timestamp": 1000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"log": {"@timestamp": 3000000, "message": "` + poison + `"}}`)

	// Baseline on a cold pool. batchBytes 1 flushes every event in its own
	// batch, exercising multiple flushes per request.
	wantAccepted, wantErrs, wantOut := collectOutputs(t, victim, 1, maxLineLength)
	require.Empty(t, wantErrs)
	require.Equal(t, 3, wantAccepted)
	require.NotEmpty(t, wantOut)

	for i := 0; i < 5; i++ {
		accepted, errs, _ := collectOutputs(t, poisonPayload, 1, maxLineLength)
		require.Equal(t, 1, accepted, "poison run %d", i) // tx accepted; over-long log line rejected
		require.NotEmpty(t, errs, "poison run %d", i)
		assert.True(t, errors.Is(errors.Join(errs...), ErrLineTooLong), "poison run %d: expected ErrLineTooLong, got %v", i, errs)

		gotAccepted, gotErrs, gotOut := collectOutputs(t, victim, 1, maxLineLength)
		require.Empty(t, gotErrs, "victim run %d", i)
		require.Equal(t, wantAccepted, gotAccepted, "victim run %d", i)
		require.Equal(t, wantOut, gotOut, "victim run %d: pooled decoder leaked state between requests", i)
		for _, o := range gotOut {
			require.NotContains(t, o, "PPPP", "victim run %d: poison bytes leaked into victim output", i)
		}
	}
}

// TestHandleStreamPoolConcurrent hammers the pool from many goroutines with
// interleaved victim/poison payloads under the race detector. Each goroutine
// verifies its own outputs, catching both data races on pooled state and
// cross-request mixing under contention.
func TestHandleStreamPoolConcurrent(t *testing.T) {
	const maxLineLength = 4096

	victim := []byte(`{"metadata": {"service": {"name": "victim-svc", "agent": {"name": "elastic-node", "version": "1.0.0"}}}}
{"transaction": {"id": "bb00000000000001", "trace_id": "bb00000000000001bb00000000000001", "name": "victim-tx", "type": "request", "duration": 1, "timestamp": 1000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
`)
	wantAccepted, wantErrs, wantOut := collectOutputs(t, victim, 1<<20, maxLineLength)
	require.Empty(t, wantErrs)

	poison := strings.Repeat("Q", 2*maxLineLength)
	poisonPayload := []byte(`{"metadata": {"service": {"name": "poison-svc", "agent": {"name": "elastic-node", "version": "9.9.9"}}}}
{"log": {"@timestamp": 3000000, "message": "` + poison + `"}}`)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_, _, _ = func() (int, []error, []string) {
					consumer := func(context.Context, *plog.Logs, *pmetric.Metrics, *ptrace.Traces) error { return nil }
					a, e := HandleStream(context.Background(), bytes.NewReader(poisonPayload), Config{BatchBytes: 1 << 20, MaxLineLength: maxLineLength}, zap.NewNop(), consumer)
					return a, e, nil
				}()
				gotAccepted, gotErrs, gotOut := collectOutputs(t, victim, 1<<20, maxLineLength)
				assert.Empty(t, gotErrs)
				assert.Equal(t, wantAccepted, gotAccepted)
				assert.Equal(t, wantOut, gotOut)
			}
		}()
	}
	wg.Wait()
}
