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
	"strings"
	"sync"
	"testing"

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
func collectOutputs(t *testing.T, payload []byte, batchSize, maxLineLength int) (int, []error, []string) {
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
	accepted, errs := HandleStream(context.Background(), bytes.NewReader(payload), batchSize, maxLineLength, zap.NewNop(), consumer)
	return accepted, errs, out
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

	// Baseline on a cold pool.
	wantAccepted, wantErrs, wantOut := collectOutputs(t, victim, 2, maxLineLength)
	require.Empty(t, wantErrs)
	require.Equal(t, 3, wantAccepted)
	require.NotEmpty(t, wantOut)

	for i := 0; i < 5; i++ {
		accepted, errs, _ := collectOutputs(t, poisonPayload, 2, maxLineLength)
		require.Equal(t, 1, accepted, "poison run %d", i) // tx accepted; over-long log line rejected
		require.NotEmpty(t, errs, "poison run %d", i)
		assert.True(t, errors.Is(errors.Join(errs...), ErrLineTooLong), "poison run %d: expected ErrLineTooLong, got %v", i, errs)

		gotAccepted, gotErrs, gotOut := collectOutputs(t, victim, 2, maxLineLength)
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
	wantAccepted, wantErrs, wantOut := collectOutputs(t, victim, 10, maxLineLength)
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
					a, e := HandleStream(context.Background(), bytes.NewReader(poisonPayload), 10, maxLineLength, zap.NewNop(), consumer)
					return a, e, nil
				}()
				gotAccepted, gotErrs, gotOut := collectOutputs(t, victim, 10, maxLineLength)
				assert.Empty(t, gotErrs)
				assert.Equal(t, wantAccepted, gotAccepted)
				assert.Equal(t, wantOut, gotOut)
			}
		}()
	}
	wg.Wait()
}
