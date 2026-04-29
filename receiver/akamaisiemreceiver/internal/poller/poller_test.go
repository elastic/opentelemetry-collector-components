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

package poller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap/zaptest"

	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/akamaiclient"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/cursor"
)

func TestPoller_Poll_BasicFlow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, `{"httpMessage":{"start":"1000","host":"test.com"}}`)
		_, _ = fmt.Fprintln(w, `{"offset":"cursor-1","total":1,"limit":10000}`)
	}))
	defer server.Close()

	log := zaptest.NewLogger(t)
	client, err := akamaiclient.NewClient(&http.Client{}, server.URL, "1", log)
	require.NoError(t, err)

	store := cursor.NewCursorStore(newMemStorageClient())

	var emitted []string
	emit := func(_ context.Context, events []string) error {
		emitted = append(emitted, events...)
		return nil
	}

	poller := NewPoller(client, store, cursor.Cursor{}, PollerConfig{
		EventLimit:       10000,
		InitialLookback:  1 * time.Hour,
		BatchSize:        1000,
		StreamBufferSize: 4,
	}, emit, log, &Telemetry{})

	err = poller.Poll(context.Background())
	require.NoError(t, err)
	assert.Len(t, emitted, 1)
	assert.Contains(t, emitted[0], `"host":"test.com"`)

	// Cursor should be persisted.
	cursor, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "cursor-1", cursor.LastOffset)
	assert.True(t, cursor.CaughtUp)
}

func TestPoller_Poll_MultiPageDrain(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := int(requestCount.Add(1))
		switch page {
		case 1:
			_, _ = fmt.Fprintln(w, `{"event":"p1e1"}`)
			_, _ = fmt.Fprintln(w, `{"event":"p1e2"}`)
			_, _ = fmt.Fprintln(w, `{"offset":"page2","total":2,"limit":2}`)
		case 2:
			_, _ = fmt.Fprintln(w, `{"event":"p2e1"}`)
			_, _ = fmt.Fprintln(w, `{"offset":"done","total":1,"limit":2}`)
		default:
			w.WriteHeader(200)
		}
	}))
	defer server.Close()

	log := zaptest.NewLogger(t)
	client, err := akamaiclient.NewClient(&http.Client{}, server.URL, "1", log)
	require.NoError(t, err)

	var emitted []string
	emit := func(_ context.Context, events []string) error {
		emitted = append(emitted, events...)
		return nil
	}

	poller := NewPoller(client, nil, cursor.Cursor{}, PollerConfig{
		EventLimit:       2,
		InitialLookback:  1 * time.Hour,
		BatchSize:        100,
		StreamBufferSize: 4,
	}, emit, log, &Telemetry{})

	err = poller.Poll(context.Background())
	require.NoError(t, err)
	assert.Len(t, emitted, 3)
	assert.GreaterOrEqual(t, int(requestCount.Load()), 2)
}

func TestPoller_Poll_EmitError_NoCursorPersist(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, `{"event":"data"}`)
		_, _ = fmt.Fprintln(w, `{"offset":"x","total":1,"limit":10000}`)
	}))
	defer server.Close()

	log := zaptest.NewLogger(t)
	client, err := akamaiclient.NewClient(&http.Client{}, server.URL, "1", log)
	require.NoError(t, err)

	store := cursor.NewCursorStore(newMemStorageClient())

	emit := func(_ context.Context, _ []string) error {
		return fmt.Errorf("emit failed")
	}

	poller := NewPoller(client, store, cursor.Cursor{}, PollerConfig{
		EventLimit:       10000,
		InitialLookback:  1 * time.Hour,
		BatchSize:        100,
		StreamBufferSize: 4,
	}, emit, log, &Telemetry{})

	err = poller.Poll(context.Background())
	require.NoError(t, err) // Poll returns nil on emit error (logs it)

	// Cursor should NOT be persisted.
	cursor, err := store.Load(context.Background())
	require.NoError(t, err)
	assert.Empty(t, cursor.LastOffset)
}

func TestPoller_Poll_416Recovery(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := int(requestCount.Add(1))
		if count == 1 {
			w.WriteHeader(416)
			_, _ = fmt.Fprintln(w, `{"detail":"offset out of range"}`)
			return
		}
		_, _ = fmt.Fprintln(w, `{"event":"recovered"}`)
		_, _ = fmt.Fprintln(w, `{"offset":"new","total":1,"limit":10000}`)
	}))
	defer server.Close()

	log := zaptest.NewLogger(t)
	client, err := akamaiclient.NewClient(&http.Client{}, server.URL, "1", log)
	require.NoError(t, err)

	var emitted []string
	emit := func(_ context.Context, events []string) error {
		emitted = append(emitted, events...)
		return nil
	}

	// Start with a stale offset to trigger 416.
	cur := cursor.Cursor{
		ChainFrom:  time.Now().Add(-1 * time.Hour).Unix(),
		ChainTo:    time.Now().Add(-1 * time.Minute).Unix(),
		CaughtUp:   false,
		LastOffset: "stale-offset",
	}

	poller := NewPoller(client, nil, cur, PollerConfig{
		EventLimit:          10000,
		InitialLookback:     1 * time.Hour,
		MaxRecoveryAttempts: 3,
		BatchSize:           100,
		StreamBufferSize:    4,
	}, emit, log, &Telemetry{})

	err = poller.Poll(context.Background())
	require.NoError(t, err)
	assert.Len(t, emitted, 1)
	assert.Contains(t, emitted[0], `"recovered"`)
}

func TestPoller_Poll_MaxRecoveryAttempts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(416)
		_, _ = fmt.Fprintln(w, `{"detail":"always expired"}`)
	}))
	defer server.Close()

	log := zaptest.NewLogger(t)
	client, err := akamaiclient.NewClient(&http.Client{}, server.URL, "1", log)
	require.NoError(t, err)

	emit := func(_ context.Context, _ []string) error { return nil }

	cur := cursor.Cursor{
		ChainFrom:  time.Now().Add(-1 * time.Hour).Unix(),
		ChainTo:    time.Now().Add(-1 * time.Minute).Unix(),
		CaughtUp:   false,
		LastOffset: "stale",
	}

	poller := NewPoller(client, nil, cur, PollerConfig{
		EventLimit:          10000,
		InitialLookback:     1 * time.Hour,
		MaxRecoveryAttempts: 2,
		BatchSize:           100,
		StreamBufferSize:    4,
	}, emit, log, &Telemetry{})

	err = poller.Poll(context.Background())
	require.NoError(t, err) // Returns nil after max attempts
}

func TestPoller_Poll_ContextCanceled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, `{"event":"data"}`)
		_, _ = fmt.Fprintln(w, `{"offset":"x","total":1,"limit":10000}`)
	}))
	defer server.Close()

	log := zaptest.NewLogger(t)
	client, err := akamaiclient.NewClient(&http.Client{}, server.URL, "1", log)
	require.NoError(t, err)

	emit := func(_ context.Context, _ []string) error { return nil }

	poller := NewPoller(client, nil, cursor.Cursor{}, PollerConfig{
		EventLimit:       10000,
		InitialLookback:  1 * time.Hour,
		BatchSize:        100,
		StreamBufferSize: 4,
	}, emit, log, &Telemetry{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately
	err = poller.Poll(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestPoller_BuildFetchParams_Branch1_OffsetDrain(t *testing.T) {
	log := zaptest.NewLogger(t)
	poller := &Poller{
		cursor: cursor.Cursor{
			CaughtUp:         false,
			LastOffset:       "my-offset",
			OffsetObtainedAt: time.Now(),
			ChainFrom:        1000,
			ChainTo:          2000,
		},
		cfg: PollerConfig{
			EventLimit: 10000,
			OffsetTTL:  120 * time.Second,
		},
		log:       log,
		telemetry: nil,
	}

	params := poller.buildFetchParams()
	assert.Equal(t, "my-offset", params.Offset)
	assert.Zero(t, params.From)
	assert.Zero(t, params.To)
}

func TestPoller_BuildFetchParams_Branch2_StaleOffset(t *testing.T) {
	log := zaptest.NewLogger(t)
	poller := &Poller{
		cursor: cursor.Cursor{
			CaughtUp:         false,
			LastOffset:       "stale-offset",
			OffsetObtainedAt: time.Now().Add(-5 * time.Minute),
			ChainFrom:        time.Now().Add(-30 * time.Minute).Unix(),
			ChainTo:          time.Now().Add(-1 * time.Minute).Unix(),
		},
		cfg: PollerConfig{
			EventLimit: 10000,
			OffsetTTL:  30 * time.Second, // TTL expired
		},
		log:       log,
		telemetry: &Telemetry{},
	}

	params := poller.buildFetchParams()
	assert.Empty(t, params.Offset) // Offset cleared due to TTL
	assert.Greater(t, params.From, int64(0))
	assert.Empty(t, poller.cursor.LastOffset)
}

func TestPoller_BuildFetchParams_Branch3_ResumeFromChainTo(t *testing.T) {
	log := zaptest.NewLogger(t)
	poller := &Poller{
		cursor: cursor.Cursor{
			CaughtUp: true,
			ChainTo:  time.Now().Add(-5 * time.Minute).Unix(),
		},
		cfg: PollerConfig{
			EventLimit:      10000,
			InitialLookback: 1 * time.Hour,
		},
		log:       log,
		telemetry: &Telemetry{},
	}

	params := poller.buildFetchParams()
	// Should start from ChainTo - overlap, not InitialLookback
	assert.Greater(t, params.From, poller.cursor.ChainTo-int64((12*time.Hour).Seconds()))
}

func TestPoller_BuildFetchParams_Branch2_ChainReplay(t *testing.T) {
	log := zaptest.NewLogger(t)
	poller := &Poller{
		cursor: cursor.Cursor{
			CaughtUp:   false,
			LastOffset: "",
			ChainFrom:  time.Now().Add(-30 * time.Minute).Unix(),
			ChainTo:    time.Now().Add(-1 * time.Minute).Unix(),
		},
		cfg: PollerConfig{
			EventLimit:      10000,
			InitialLookback: 12 * time.Hour,
		},
		log:       log,
		telemetry: nil,
	}

	params := poller.buildFetchParams()
	assert.Empty(t, params.Offset)
	assert.Greater(t, params.From, int64(0))
	assert.Greater(t, params.To, int64(0))
}

func TestPoller_BuildFetchParams_Branch3_NewChain(t *testing.T) {
	log := zaptest.NewLogger(t)
	poller := &Poller{
		cursor: cursor.Cursor{
			CaughtUp: true,
		},
		cfg: PollerConfig{
			EventLimit:      10000,
			InitialLookback: 1 * time.Hour,
		},
		log:       log,
		telemetry: nil,
	}

	params := poller.buildFetchParams()
	assert.Empty(t, params.Offset)
	assert.Greater(t, params.From, int64(0))
	assert.Greater(t, params.To, int64(0))
	assert.False(t, poller.cursor.CaughtUp)
}

func TestTelemetry_WithRealInstruments(t *testing.T) {
	meter := noop.NewMeterProvider().Meter("test")
	counter, _ := meter.Int64Counter("test_counter")
	histogram, _ := meter.Float64Histogram("test_histogram")
	intHist, _ := meter.Int64Histogram("test_int_hist")

	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("test")

	tel := &Telemetry{
		Tracer:          tracer,
		Requests:        counter,
		RequestErrors:   counter,
		EventsReceived:  counter,
		EventsEmitted:   counter,
		RequestDuration: histogram,
		PollDuration:    histogram,
		EventsPerPage:   intHist,
	}

	ctx := context.Background()

	// Exercise all telemetry helpers with real instruments.
	tel.addCounter(ctx, tel.Requests, 1)
	tel.addCounter(ctx, tel.RequestErrors, 5)
	tel.recordFloat(ctx, tel.RequestDuration, 0.5)
	tel.recordInt(ctx, tel.EventsPerPage, 1000)

	// Span with real tracer.
	spanCtx, endSpan := tel.startSpan(ctx, "test.span")
	assert.NotEqual(t, ctx, spanCtx) // Should have a new span context
	endSpan(nil)

	// Span with error.
	_, endErrSpan := tel.startSpan(ctx, "test.error_span")
	endErrSpan(fmt.Errorf("test error"))

	// Nil telemetry should not panic.
	var nilTel *Telemetry
	nilTel.addCounter(ctx, nil, 1)
	nilTel.recordFloat(ctx, nil, 1.0)
	nilTel.recordInt(ctx, nil, 1)
	nilCtx, nilEnd := nilTel.startSpan(ctx, "noop")
	assert.Equal(t, ctx, nilCtx) // No-op returns same context
	nilEnd(nil)
}

func TestPoller_HandleFetchError_NonAPI(t *testing.T) {
	log := zaptest.NewLogger(t)
	poller := &Poller{log: log, telemetry: nil}
	params := akamaiclient.FetchParams{Limit: 100}
	result := poller.handleFetchError(context.Background(), fmt.Errorf("network error"), &params)
	assert.False(t, result) // Non-API errors are not recoverable
}

func TestPoller_HandleFetchError_400Fatal(t *testing.T) {
	log := zaptest.NewLogger(t)
	poller := &Poller{log: log, telemetry: nil}
	params := akamaiclient.FetchParams{Limit: 100}
	err := &akamaiclient.APIError{StatusCode: 400, Detail: "bad request"}
	result := poller.handleFetchError(context.Background(), err, &params)
	assert.False(t, result)
}

func TestPoller_Poll_InvalidTimestampRetry(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := int(requestCount.Add(1))
		if count <= 2 {
			w.WriteHeader(400)
			_, _ = fmt.Fprintln(w, `{"detail":"invalid timestamp"}`)
			return
		}
		_, _ = fmt.Fprintln(w, `{"event":"after-retry"}`)
		_, _ = fmt.Fprintln(w, `{"offset":"ok","total":1,"limit":10000}`)
	}))
	defer server.Close()

	log := zaptest.NewLogger(t)
	client, err := akamaiclient.NewClient(&http.Client{}, server.URL, "1", log)
	require.NoError(t, err)

	var emitted []string
	emit := func(_ context.Context, events []string) error {
		emitted = append(emitted, events...)
		return nil
	}

	poller := NewPoller(client, nil, cursor.Cursor{}, PollerConfig{
		EventLimit:              10000,
		InitialLookback:         1 * time.Hour,
		InvalidTimestampRetries: 3,
		MaxRecoveryAttempts:     5,
		BatchSize:               100,
		StreamBufferSize:        4,
	}, emit, log, &Telemetry{})

	err = poller.Poll(context.Background())
	require.NoError(t, err)
	assert.Len(t, emitted, 1)
	assert.GreaterOrEqual(t, int(requestCount.Load()), 3)
}

func TestPoller_HandleFetchError_InvalidTimestamp(t *testing.T) {
	log := zaptest.NewLogger(t)
	poller := &Poller{
		cursor:    cursor.Cursor{ChainFrom: 1000, ChainTo: 2000, LastOffset: "old"},
		cfg:       PollerConfig{EventLimit: 10000, InitialLookback: 1 * time.Hour},
		log:       log,
		telemetry: &Telemetry{},
	}
	params := akamaiclient.FetchParams{Limit: 100}
	err := &akamaiclient.APIError{StatusCode: 400, Detail: "invalid timestamp"}
	result := poller.handleFetchError(context.Background(), err, &params)
	assert.True(t, result)
	assert.Empty(t, poller.cursor.LastOffset) // Offset cleared
}

func TestPoller_HandleFetchError_FromTooOld(t *testing.T) {
	log := zaptest.NewLogger(t)
	poller := &Poller{
		cursor:    cursor.Cursor{ChainFrom: 1000, ChainTo: 2000},
		cfg:       PollerConfig{EventLimit: 10000, InitialLookback: 1 * time.Hour},
		log:       log,
		telemetry: nil,
	}
	params := akamaiclient.FetchParams{Limit: 100}
	err := &akamaiclient.APIError{StatusCode: 400, Detail: "from parameter is out of range"}
	result := poller.handleFetchError(context.Background(), err, &params)
	assert.True(t, result) // Recoverable
}

// memStorageClient is a simple in-memory storage.Client for tests.
type memStorageClient struct {
	data sync.Map
}

func newMemStorageClient() *memStorageClient {
	return &memStorageClient{}
}

func (m *memStorageClient) Get(_ context.Context, key string) ([]byte, error) {
	v, ok := m.data.Load(key)
	if !ok {
		return nil, nil
	}
	return v.([]byte), nil
}

func (m *memStorageClient) Set(_ context.Context, key string, value []byte) error {
	m.data.Store(key, value)
	return nil
}

func (m *memStorageClient) Delete(_ context.Context, key string) error {
	m.data.Delete(key)
	return nil
}

func (m *memStorageClient) Batch(_ context.Context, ops ...*storage.Operation) error {
	for _, op := range ops {
		switch op.Type {
		case storage.Get:
			v, _ := m.data.Load(op.Key)
			if v != nil {
				op.Value = v.([]byte)
			}
		case storage.Set:
			m.data.Store(op.Key, op.Value)
		case storage.Delete:
			m.data.Delete(op.Key)
		}
	}
	return nil
}

func (m *memStorageClient) Close(_ context.Context) error {
	return nil
}
