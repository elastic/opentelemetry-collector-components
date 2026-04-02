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

package akamaiclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// collectStreamEvents is a test helper that runs StreamEvents with a channel
// and collects all events into a slice, returning them along with the offset
// context. This mirrors the old slice-based API for test convenience.
func collectStreamEvents(ctx context.Context, body io.Reader) ([]string, OffsetContext, error) {
	ch := make(chan string, 1000)
	var pageCtx OffsetContext
	var count int
	var streamErr error

	go func() {
		defer close(ch)
		pageCtx, count, streamErr = StreamEvents(ctx, body, ch)
	}()

	var events []string
	for e := range ch {
		events = append(events, e)
	}
	_ = count
	return events, pageCtx, streamErr
}

func TestStreamEvents_WithOffsetContext(t *testing.T) {
	body := strings.NewReader(
		`{"attackData":{"rule":"950004"},"httpMessage":{"host":"example.com"}}` + "\n" +
			`{"attackData":{"rule":"990011"},"httpMessage":{"host":"example.com"}}` + "\n" +
			`{"offset":"next-cursor-abc","total":2,"limit":10000}` + "\n",
	)

	events, pageCtx, err := collectStreamEvents(context.Background(), body)
	require.NoError(t, err)
	assert.Len(t, events, 2)
	assert.Equal(t, "next-cursor-abc", pageCtx.Offset)
	assert.Equal(t, 10000, pageCtx.Limit)
	assert.Equal(t, 2, pageCtx.Total)

	for _, e := range events {
		assert.True(t, json.Valid([]byte(e)), "event should be valid JSON: %s", e)
	}
}

func TestStreamEvents_WithoutOffsetContext(t *testing.T) {
	body := strings.NewReader(
		`{"event":"one"}` + "\n" +
			`{"event":"two"}` + "\n",
	)

	events, pageCtx, err := collectStreamEvents(context.Background(), body)
	require.NoError(t, err)
	assert.Len(t, events, 2)
	assert.Empty(t, pageCtx.Offset)
	assert.Zero(t, pageCtx.Limit)
}

func TestStreamEvents_EmptyBody(t *testing.T) {
	events, pageCtx, err := collectStreamEvents(context.Background(), strings.NewReader(""))
	require.NoError(t, err)
	assert.Empty(t, events)
	assert.Empty(t, pageCtx.Offset)
}

func TestStreamEvents_OnlyOffsetContext(t *testing.T) {
	events, pageCtx, err := collectStreamEvents(context.Background(),
		strings.NewReader(`{"offset":"abc","total":0,"limit":10000}`+"\n"))
	require.NoError(t, err)
	assert.Empty(t, events)
	assert.Equal(t, "abc", pageCtx.Offset)
}

func TestStreamEvents_BlankLines(t *testing.T) {
	body := strings.NewReader("\n\n" + `{"event":"one"}` + "\n\n" + `{"offset":"x","total":1,"limit":100}` + "\n\n")
	events, pageCtx, err := collectStreamEvents(context.Background(), body)
	require.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, "x", pageCtx.Offset)
}

func TestStreamEvents_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Use a small bounded channel to trigger the context check in the select.
	ch := make(chan string, 1)
	body := strings.NewReader(`{"event":"one"}` + "\n" + `{"event":"two"}` + "\n")
	_, _, err := StreamEvents(ctx, body, ch)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestStreamEvents_BackPressure(t *testing.T) {
	// Bounded channel of size 1 — scanner must block when channel is full.
	ch := make(chan string, 1)
	body := strings.NewReader(
		`{"event":"one"}` + "\n" +
			`{"event":"two"}` + "\n" +
			`{"event":"three"}` + "\n" +
			`{"offset":"x","total":3,"limit":100}` + "\n",
	)

	done := make(chan struct{})
	var pageCtx OffsetContext
	var count int
	go func() {
		defer close(done)
		pageCtx, count, _ = StreamEvents(context.Background(), body, ch)
		close(ch)
	}()

	// Drain slowly to exercise back-pressure.
	var events []string
	for e := range ch {
		events = append(events, e)
	}
	<-done

	assert.Len(t, events, 3)
	assert.Equal(t, 3, count)
	assert.Equal(t, "x", pageCtx.Offset)
}

func TestClient_FetchResponse_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/siem/v1/configs/12345", r.URL.Path)
		assert.Equal(t, "10000", r.URL.Query().Get("limit"))
		assert.Equal(t, "test-offset", r.URL.Query().Get("offset"))
		_, _ = fmt.Fprintln(w, `{"event":"data"}`)
		_, _ = fmt.Fprintln(w, `{"offset":"next","total":1,"limit":10000}`)
	}))
	defer server.Close()

	log := zaptest.NewLogger(t)
	client, err := NewClient(server.Client(), server.URL, "12345", log)
	require.NoError(t, err)

	body, err := client.FetchResponse(context.Background(), FetchParams{
		Offset: "test-offset",
		Limit:  10000,
	})
	require.NoError(t, err)
	defer func() { _ = body.Close() }()

	events, pageCtx, err := collectStreamEvents(context.Background(), body)
	require.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, "next", pageCtx.Offset)
}

func TestClient_FetchResponse_TimeBased(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "1000", r.URL.Query().Get("from"))
		assert.Equal(t, "2000", r.URL.Query().Get("to"))
		assert.Empty(t, r.URL.Query().Get("offset"))
		w.WriteHeader(200)
	}))
	defer server.Close()

	log := zaptest.NewLogger(t)
	client, err := NewClient(server.Client(), server.URL, "99", log)
	require.NoError(t, err)

	body, err := client.FetchResponse(context.Background(), FetchParams{
		From:  1000,
		To:    2000,
		Limit: 100,
	})
	require.NoError(t, err)
	_ = body.Close()
}

func TestClient_FetchResponse_APIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(416)
		_, _ = fmt.Fprintln(w, `{"detail":"offset out of range"}`)
	}))
	defer server.Close()

	log := zaptest.NewLogger(t)
	client, err := NewClient(server.Client(), server.URL, "12345", log)
	require.NoError(t, err)

	_, err = client.FetchResponse(context.Background(), FetchParams{Offset: "stale", Limit: 100})
	require.Error(t, err)

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, 416, apiErr.StatusCode)
	assert.True(t, apiErr.IsOffsetOutOfRange())
}

func TestClient_FetchResponse_InvalidTimestamp(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_, _ = fmt.Fprintln(w, `{"detail":"invalid timestamp"}`)
	}))
	defer server.Close()

	log := zaptest.NewLogger(t)
	client, err := NewClient(server.Client(), server.URL, "12345", log)
	require.NoError(t, err)

	_, err = client.FetchResponse(context.Background(), FetchParams{Limit: 100})
	require.Error(t, err)

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.True(t, apiErr.IsInvalidTimestamp())
}

func TestAPIError_Error_NoDetail(t *testing.T) {
	err := &APIError{StatusCode: 500, Status: "500 Internal Server Error"}
	assert.Equal(t, "akamai API error: 500 Internal Server Error (500)", err.Error())
}

func TestAPIError_Error_WithDetail(t *testing.T) {
	err := &APIError{StatusCode: 400, Status: "400 Bad Request", Detail: "bad param"}
	assert.Contains(t, err.Error(), "bad param")
}

func TestNewClient_InvalidURL(t *testing.T) {
	log := zaptest.NewLogger(t)
	_, err := NewClient(&http.Client{}, "://invalid", "1", log)
	assert.Error(t, err)
}

func TestClient_Close(t *testing.T) {
	log := zaptest.NewLogger(t)
	client, err := NewClient(&http.Client{}, "https://example.com", "1", log)
	require.NoError(t, err)
	client.Close() // Should not panic
}

func TestClient_FetchResponse_NetworkError(t *testing.T) {
	log := zaptest.NewLogger(t)
	client, err := NewClient(&http.Client{}, "https://localhost:1", "1", log)
	require.NoError(t, err)
	_, err = client.FetchResponse(context.Background(), FetchParams{Limit: 100})
	assert.Error(t, err)
}

func TestAPIError_IsFromTooOld(t *testing.T) {
	assert.True(t, (&APIError{StatusCode: 400, Detail: "from parameter is out of range"}).IsFromTooOld())
	assert.True(t, (&APIError{StatusCode: 400, Detail: "timestamp is too old"}).IsFromTooOld())
	assert.False(t, (&APIError{StatusCode: 400, Detail: "invalid timestamp"}).IsFromTooOld())
	assert.False(t, (&APIError{StatusCode: 416, Detail: "out of range"}).IsFromTooOld())
}
