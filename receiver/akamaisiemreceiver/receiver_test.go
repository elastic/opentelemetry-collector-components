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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestFactory_Type(t *testing.T) {
	f := NewFactory()
	assert.Equal(t, "akamai_siem", f.Type().String())
}

func TestFactory_CreateDefaultConfig(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig()
	require.NotNil(t, cfg)
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestReceiver_StartShutdown(t *testing.T) {
	// Mock Akamai API that returns empty response.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "12345"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour // don't poll again during test

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)

	err = rcv.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Give the first poll time to execute.
	time.Sleep(500 * time.Millisecond)

	err = rcv.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestReceiver_EmitsEvents(t *testing.T) {
	// Mock Akamai API returning 2 events + offset context.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Authorization"), "EG1-HMAC-SHA256")
		_, _ = fmt.Fprintln(w, `{"attackData":{"rule":"950004"},"httpMessage":{"host":"example.com"}}`)
		_, _ = fmt.Fprintln(w, `{"attackData":{"rule":"990011"},"httpMessage":{"host":"test.com"}}`)
		_, _ = fmt.Fprintln(w, `{"offset":"cursor-abc","total":2,"limit":10000}`)
	}))
	defer server.Close()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "12345"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour
	cfg.HTTP = confighttp.ClientConfig{
		Timeout: 10 * time.Second,
	}

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)

	err = rcv.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Wait for poll to emit events.
	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 2
	}, 5*time.Second, 100*time.Millisecond, "expected at least 2 log records")

	err = rcv.Shutdown(context.Background())
	require.NoError(t, err)

	// Verify log bodies contain raw JSON.
	allLogs := sink.AllLogs()
	require.NotEmpty(t, allLogs)
	rl := allLogs[0].ResourceLogs().At(0)
	lr := rl.ScopeLogs().At(0).LogRecords().At(0)
	assert.Contains(t, bodyMessage(t, lr), `"rule":"950004"`)
}

// TestReceiver_CursorPersistAndResume verifies that:
// 1. First run fetches with time-based params (no cursor), receives events, persists cursor with offset
// 2. Cursor is written to the storage extension with correct content
// 3. Second run loads cursor and resumes with offset-based fetch (not time-based)
func TestReceiver_CursorPersistAndResume(t *testing.T) {
	var requests []map[string]string
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, map[string]string{
			"offset": r.URL.Query().Get("offset"),
			"from":   r.URL.Query().Get("from"),
			"to":     r.URL.Query().Get("to"),
		})
		mu.Unlock()

		// Return 1 event (less than limit=10000 → chain drained, caught_up=true).
		_, _ = fmt.Fprintln(w, `{"httpMessage":{"start":"1000","host":"test.com","status":"200"}}`)
		_, _ = fmt.Fprintln(w, `{"offset":"saved-cursor-abc","total":1,"limit":10000}`)
	}))
	defer server.Close()

	memClient := newMemStorageClient()
	storageID := component.MustNewID("file_storage")

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "99"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour
	cfg.StorageID = &storageID

	set := receivertest.NewNopSettings(NewFactory().Type())
	host := mockHost(memClient)

	// --- First run: should use time-based fetch (no cursor exists yet) ---
	sink1 := &consumertest.LogsSink{}
	rcv1, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink1)
	require.NoError(t, err)
	require.NoError(t, rcv1.Start(context.Background(), host))
	require.Eventually(t, func() bool { return sink1.LogRecordCount() >= 1 }, 5*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv1.Shutdown(context.Background()))

	// Verify first request was time-based (has "from" and "to", no "offset").
	mu.Lock()
	require.NotEmpty(t, requests, "first run should have made at least one request")
	firstReq := requests[0]
	mu.Unlock()
	assert.NotEmpty(t, firstReq["from"], "first run should use time-based fetch (from)")
	assert.NotEmpty(t, firstReq["to"], "first run should use time-based fetch (to)")
	assert.Empty(t, firstReq["offset"], "first run should NOT have offset (no cursor)")

	// Verify cursor was written to storage extension.
	cursorData, err := memClient.Get(context.Background(), "akamai_siem_cursor")
	require.NoError(t, err, "cursor should exist in storage after first run")
	require.NotNil(t, cursorData, "cursor data should not be nil after first run")

	var savedCursor map[string]any
	require.NoError(t, json.Unmarshal(cursorData, &savedCursor))
	assert.Equal(t, "saved-cursor-abc", savedCursor["last_offset"], "cursor should contain the offset from API response")
	assert.NotZero(t, savedCursor["chain_from"], "cursor should have chain_from")
	assert.NotZero(t, savedCursor["chain_to"], "cursor should have chain_to")
	assert.True(t, savedCursor["caught_up"].(bool), "cursor should be caught_up (events < limit)")

	// --- Second run: should resume from cursor with offset-based fetch ---
	mu.Lock()
	requests = nil // Reset request log.
	mu.Unlock()

	sink2 := &consumertest.LogsSink{}
	rcv2, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink2)
	require.NoError(t, err)
	require.NoError(t, rcv2.Start(context.Background(), host))
	require.Eventually(t, func() bool { return sink2.LogRecordCount() >= 1 }, 5*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv2.Shutdown(context.Background()))

	// Second run: cursor was caught_up=true, so it starts a NEW chain (Branch 3),
	// not an offset-based fetch. This is correct — caught_up means the previous
	// chain was fully drained, so we start fresh from chain_to with overlap.
	mu.Lock()
	require.NotEmpty(t, requests, "second run should have made at least one request")
	secondReq := requests[0]
	mu.Unlock()

	// Branch 3 (new chain) uses from/to, not offset.
	// The "from" should be based on the previous chain_to minus overlap.
	assert.NotEmpty(t, secondReq["from"], "second run should start a new chain with from")
	assert.NotEmpty(t, secondReq["to"], "second run should start a new chain with to")
}

// TestReceiver_CursorResume_OffsetDrain verifies that when the cursor has
// caught_up=false + valid offset, the second run resumes with offset-based fetch.
func TestReceiver_CursorResume_OffsetDrain(t *testing.T) {
	// Pre-seed a cursor in the mock storage that simulates an interrupted chain drain.
	memClient := newMemStorageClient()
	cursor := map[string]any{
		"chain_from":         time.Now().Add(-1 * time.Hour).Unix(),
		"chain_to":           time.Now().Add(-1 * time.Minute).Unix(),
		"caught_up":          false,
		"last_offset":        "resume-from-this-offset",
		"offset_obtained_at": time.Now().Add(-10 * time.Second).Format(time.RFC3339Nano),
	}
	cursorData, _ := json.Marshal(cursor)
	require.NoError(t, memClient.Set(context.Background(), "akamai_siem_cursor", cursorData))

	storageID := component.MustNewID("file_storage")

	var capturedOffset string
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		capturedOffset = r.URL.Query().Get("offset")
		mu.Unlock()
		// Return fewer events than limit → chain drained.
		_, _ = fmt.Fprintln(w, `{"httpMessage":{"start":"1000","host":"test.com","status":"200"}}`)
		_, _ = fmt.Fprintln(w, `{"offset":"next-offset","total":1,"limit":10000}`)
	}))
	defer server.Close()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "99"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour
	cfg.StorageID = &storageID

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), mockHost(memClient)))
	require.Eventually(t, func() bool { return sink.LogRecordCount() >= 1 }, 5*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))

	// The receiver should have resumed with the pre-seeded offset (Branch 1: drain).
	mu.Lock()
	assert.Equal(t, "resume-from-this-offset", capturedOffset,
		"receiver should resume from the persisted offset")
	mu.Unlock()
}

func TestReceiver_TestdataResponse(t *testing.T) {
	// Serve the realistic NDJSON testdata file through a mock server.
	ndjson, err := os.ReadFile("testdata/siem_response.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "67217"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))

	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 2
	}, 5*time.Second, 100*time.Millisecond, "expected 2 events from testdata")

	require.NoError(t, rcv.Shutdown(context.Background()))

	// Verify the events contain real Akamai SIEM fields.
	allLogs := sink.AllLogs()
	require.NotEmpty(t, allLogs)
	records := allLogs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	assert.Equal(t, 2, records.Len())

	body0 := bodyMessage(t, records.At(0))
	assert.Contains(t, body0, `"clientIP":"198.51.100.1"`)
	assert.Contains(t, body0, `"configId":"67217"`)
	assert.Contains(t, body0, `"host":"example.com"`)

	body1 := bodyMessage(t, records.At(1))
	assert.Contains(t, body1, `"clientIP":"203.0.113.42"`)
	assert.Contains(t, body1, `"status":"403"`)
}

func TestReceiver_OTelMappingMode(t *testing.T) {
	ndjson, err := os.ReadFile("testdata/siem_response.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "67217"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour
	cfg.OutputFormat = "otel" // OTel semantic conventions mode

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))

	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 2
	}, 5*time.Second, 100*time.Millisecond, "expected 2 events in otel mode")

	require.NoError(t, rcv.Shutdown(context.Background()))

	allLogs := sink.AllLogs()
	require.NotEmpty(t, allLogs)
	rl := allLogs[0].ResourceLogs().At(0)

	// Should have OTel semantic convention attributes on log records.
	lr := rl.ScopeLogs().At(0).LogRecords().At(0)
	attrs := lr.Attributes()

	// HTTP semantic conventions present.
	method, ok := attrs.Get("http.request.method")
	require.True(t, ok, "http.request.method missing in otel mode")
	assert.Equal(t, "GET", method.Str())

	statusCode, ok := attrs.Get("http.response.status_code")
	require.True(t, ok, "http.response.status_code missing in otel mode")
	assert.Equal(t, int64(200), statusCode.Int())

	domain, ok := attrs.Get("url.domain")
	require.True(t, ok, "url.domain missing in otel mode")
	assert.Equal(t, "example.com", domain.Str())

	// Source IP mapped.
	srcIP, ok := attrs.Get("source.ip")
	require.True(t, ok, "source.ip missing in otel mode")
	assert.Equal(t, "198.51.100.1", srcIP.Str())

	// Geo mapped.
	country, ok := attrs.Get("source.geo.country_iso_code")
	require.True(t, ok, "source.geo.country_iso_code missing")
	assert.Equal(t, "BR", country.Str())

	// Akamai-specific fields in namespace.
	configID, ok := attrs.Get("akamai.siem.config_id")
	require.True(t, ok, "akamai.siem.config_id missing")
	assert.Equal(t, "67217", configID.Str())

	// Body still contains raw JSON.
	assert.Contains(t, lr.Body().Str(), `"clientIP"`)

	// Timestamp set from httpMessage.start.
	assert.Greater(t, int64(lr.Timestamp()), int64(0), "timestamp should be set from event")
}

func TestReceiver_RawMode_BodyIsRawJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, `{"httpMessage":{"start":"1000","host":"test.com","status":"200"}}`)
		_, _ = fmt.Fprintln(w, `{"offset":"x","total":1,"limit":10000}`)
	}))
	defer server.Close()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "1"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour
	cfg.OutputFormat = "raw"

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))

	assert.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 1
	}, 5*time.Second, 100*time.Millisecond)

	require.NoError(t, rcv.Shutdown(context.Background()))

	allLogs := sink.AllLogs()
	require.NotEmpty(t, allLogs)
	rl := allLogs[0].ResourceLogs().At(0)

	// Raw mode should NOT have OTel semantic convention attrs on log records.
	lr := rl.ScopeLogs().At(0).LogRecords().At(0)
	_, hasMethod := lr.Attributes().Get("http.request.method")
	assert.False(t, hasMethod, "raw mode should not parse into OTel attrs")

	// Body is a map with raw JSON in "message" key.
	assert.Contains(t, bodyMessage(t, lr), `"host":"test.com"`)
}

func TestReceiver_InvalidMappingMode(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = "https://test.example.com"
	cfg.ConfigIDs = "1"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.OutputFormat = "invalid"
	assert.ErrorContains(t, cfg.Validate(), `output_format must be "raw" or "otel"`)
}

// TestReceiver_RawMode_FullFlow tests the complete flow in raw mode:
// Mock Akamai API → EdgeGrid auth → NDJSON parse → cursor persist
// → plog.Logs with body map {message: rawJSON}, no OTel semantic attributes.
func TestReceiver_RawMode_FullFlow(t *testing.T) {
	var requestCount atomic.Int32
	ndjson, err := os.ReadFile("testdata/siem_response.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		auth := r.Header.Get("Authorization")
		require.Contains(t, auth, "EG1-HMAC-SHA256", "request missing EdgeGrid auth")
		require.Contains(t, auth, "client_token=ct")
		require.Contains(t, auth, "access_token=at")
		require.Contains(t, auth, "signature=")
		assert.Equal(t, "/siem/v1/configs/12345", r.URL.Path)
		assert.NotEmpty(t, r.URL.Query().Get("limit"))
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "12345"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.OutputFormat = "raw"
	cfg.PollInterval = 24 * time.Hour

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 2
	}, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))

	allLogs := sink.AllLogs()
	require.NotEmpty(t, allLogs)
	require.Equal(t, 1, allLogs[0].ResourceLogs().Len())
	rl := allLogs[0].ResourceLogs().At(0)

	require.Equal(t, 1, rl.ScopeLogs().Len())
	records := rl.ScopeLogs().At(0).LogRecords()
	assert.Equal(t, 2, records.Len(), "expected 2 events (offset context excluded)")

	for i := 0; i < records.Len(); i++ {
		lr := records.At(i)
		body := bodyMessage(t, lr)
		assert.True(t, json.Valid([]byte(body)), "record %d body should be valid JSON", i)
		assert.Contains(t, body, `"attackData"`, "record %d missing attackData", i)
		assert.Contains(t, body, `"httpMessage"`, "record %d missing httpMessage", i)
		assert.Greater(t, int64(lr.Timestamp()), int64(0), "record %d timestamp not set", i)
		assert.Greater(t, int64(lr.ObservedTimestamp()), int64(0), "record %d observed timestamp not set", i)
		// Raw mode: no OTel semantic convention attributes on log records.
		_, hasMethod := lr.Attributes().Get("http.request.method")
		assert.False(t, hasMethod, "record %d should not have http.request.method in raw mode", i)
	}

	assert.Contains(t, bodyMessage(t, records.At(0)), `"clientIP":"198.51.100.1"`)
	assert.Contains(t, bodyMessage(t, records.At(1)), `"clientIP":"203.0.113.42"`)

	// Offset context should not appear as an event.
	for i := 0; i < records.Len(); i++ {
		assert.NotContains(t, bodyMessage(t, records.At(i)), `"offset"`)
	}

	assert.GreaterOrEqual(t, int(requestCount.Load()), 1)
}

// TestReceiver_OTelMode_FullFlow tests the complete flow in OTel mode:
// Mock Akamai API → EdgeGrid auth → NDJSON parse → OTel mapper
// → plog.Logs with structured semantic convention attributes.
func TestReceiver_OTelMode_FullFlow(t *testing.T) {
	ndjson, err := os.ReadFile("testdata/siem_response.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Contains(t, r.Header.Get("Authorization"), "EG1-HMAC-SHA256")
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "12345"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.OutputFormat = "otel"
	cfg.PollInterval = 24 * time.Hour

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 2
	}, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))

	allLogs := sink.AllLogs()
	require.NotEmpty(t, allLogs)
	rl := allLogs[0].ResourceLogs().At(0)

	records := rl.ScopeLogs().At(0).LogRecords()
	assert.Equal(t, 2, records.Len())

	// Record 0: tarpit action, BR geo, GET /api/test.
	lr0 := records.At(0)
	a0 := lr0.Attributes()
	assertAttr(t, a0, "http.request.method", "GET")
	assertAttrI(t, a0, "http.response.status_code", 200)
	assertAttr(t, a0, "url.domain", "example.com")
	assertAttr(t, a0, "url.path", "/api/test")
	assertAttrI(t, a0, "server.port", 443)
	assertAttr(t, a0, "network.protocol.name", "http")
	assertAttr(t, a0, "network.protocol.version", "1.1")
	assertAttr(t, a0, "network.transport", "tcp")
	assertAttr(t, a0, "tls.protocol.name", "tls")
	assertAttr(t, a0, "tls.protocol.version", "1.3")
	assertAttr(t, a0, "source.ip", "198.51.100.1")
	assertAttr(t, a0, "source.geo.country_iso_code", "BR")
	assertAttr(t, a0, "source.geo.city_name", "SOROCABA")
	assertAttr(t, a0, "source.geo.region_iso_code", "BR-SP")
	assertAttrI(t, a0, "source.as.number", 28573)
	assertAttr(t, a0, "akamai.siem.config_id", "67217")
	assertAttr(t, a0, "akamai.siem.applied_action", "tarpit")
	assert.Equal(t, plog.SeverityNumberWarn, lr0.SeverityNumber())
	assert.Equal(t, "WARN", lr0.SeverityText())

	// Rules decoded.
	rules0, ok := a0.Get("akamai.siem.rules")
	require.True(t, ok)
	assert.Equal(t, 2, rules0.Slice().Len())
	assert.Equal(t, "3904006", rules0.Slice().At(0).Str())

	assertAttr(t, a0, "observer.vendor", "akamai")
	assertAttr(t, a0, "event.kind", "event")
	assert.Greater(t, int64(lr0.Timestamp()), int64(0))
	assert.Contains(t, lr0.Body().Str(), `"clientIP"`)

	// Record 1: monitor action, US geo, POST /login.
	lr1 := records.At(1)
	a1 := lr1.Attributes()
	assertAttr(t, a1, "http.request.method", "POST")
	assertAttrI(t, a1, "http.response.status_code", 403)
	assertAttr(t, a1, "source.ip", "203.0.113.42")
	assertAttr(t, a1, "source.geo.country_iso_code", "US")
	assert.Equal(t, plog.SeverityNumberInfo, lr1.SeverityNumber())

	// Offset context not emitted as event.
	for i := 0; i < records.Len(); i++ {
		var parsed map[string]any
		require.NoError(t, json.Unmarshal([]byte(records.At(i).Body().Str()), &parsed))
		_, hasOffset := parsed["offset"]
		assert.False(t, hasOffset, "record %d looks like offset context", i)
	}
}

// TestReceiver_OTelMode_SkipsInvalidJSON verifies that invalid JSON events
// are skipped in OTel mode without polluting the output.
func TestReceiver_OTelMode_SkipsInvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, `{"httpMessage":{"start":"1000","host":"good.com","status":"200"},"attackData":{"clientIP":"1.2.3.4"}}`)
		_, _ = fmt.Fprintln(w, `{invalid json that will fail mapping}`)
		_, _ = fmt.Fprintln(w, `{"offset":"x","total":2,"limit":10000}`)
	}))
	defer server.Close()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "1"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.OutputFormat = "otel"
	cfg.PollInterval = 24 * time.Hour

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	require.Eventually(t, func() bool { return sink.LogRecordCount() >= 1 }, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))

	records := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	assert.Equal(t, 1, records.Len(), "invalid event should be skipped")
	assertAttr(t, records.At(0).Attributes(), "url.domain", "good.com")
}

// TestReceiver_ChainDrain_MultiPage verifies multi-page chain draining:
// page 1 returns events == limit → fetches page 2 → fewer events → drained.
func TestReceiver_ChainDrain_MultiPage(t *testing.T) {
	var pageRequests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := int(pageRequests.Add(1))
		switch page {
		case 1:
			_, _ = fmt.Fprintln(w, `{"httpMessage":{"start":"1000","host":"page1.com","status":"200"}}`)
			_, _ = fmt.Fprintln(w, `{"httpMessage":{"start":"1001","host":"page1.com","status":"200"}}`)
			_, _ = fmt.Fprintln(w, `{"offset":"page2-cursor","total":2,"limit":2}`)
		case 2:
			_, _ = fmt.Fprintln(w, `{"httpMessage":{"start":"1002","host":"page2.com","status":"200"}}`)
			_, _ = fmt.Fprintln(w, `{"offset":"final","total":1,"limit":2}`)
		default:
			w.WriteHeader(200)
		}
	}))
	defer server.Close()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "1"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.OutputFormat = "raw"
	cfg.EventLimit = 2
	cfg.PollInterval = 24 * time.Hour

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	require.Eventually(t, func() bool { return sink.LogRecordCount() >= 3 }, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))

	assert.GreaterOrEqual(t, int(pageRequests.Load()), 2, "should fetch at least 2 pages")
	assert.Equal(t, 3, sink.LogRecordCount())
}

// TestReceiver_BatchedEmission verifies that a page with more events than
// batch_size is split into multiple ConsumeLogs calls.
func TestReceiver_BatchedEmission(t *testing.T) {
	// Generate 5 events. With batch_size=2, expect 3 ConsumeLogs calls (2+2+1).
	var sb strings.Builder
	for i := 0; i < 5; i++ {
		_, _ = fmt.Fprintf(&sb, `{"httpMessage":{"start":"%d","host":"batch.com","status":"200"}}%s`, 1000+i, "\n")
	}
	_, _ = fmt.Fprintln(&sb, `{"offset":"x","total":5,"limit":10000}`)
	ndjson := sb.String()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, ndjson)
	}))
	defer server.Close()

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "1"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour
	cfg.BatchSize = 2
	cfg.StreamBufferSize = 2

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 5
	}, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))

	// Total events should be 5.
	assert.Equal(t, 5, sink.LogRecordCount())

	// With batch_size=2, we expect 3 separate ConsumeLogs calls → 3 plog.Logs entries.
	allLogs := sink.AllLogs()
	assert.GreaterOrEqual(t, len(allLogs), 3,
		"5 events with batch_size=2 should produce at least 3 ConsumeLogs calls, got %d", len(allLogs))
}

// TestReceiver_EmitFailure_NoCursorPersist verifies that if ConsumeLogs fails
// mid-page, the cursor is NOT persisted (events not confirmed).
func TestReceiver_EmitFailure_NoCursorPersist(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, `{"httpMessage":{"start":"1000","host":"test.com","status":"200"}}`)
		_, _ = fmt.Fprintln(w, `{"offset":"should-not-persist","total":1,"limit":10000}`)
	}))
	defer server.Close()

	memClient := newMemStorageClient()
	storageID := component.MustNewID("file_storage")

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "1"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour
	cfg.StorageID = &storageID

	// Use a consumer that always fails.
	failConsumer := &failingConsumer{}
	set := receivertest.NewNopSettings(NewFactory().Type())

	rcv, err := newAkamaiReceiver(cfg, set, failConsumer)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), mockHost(memClient)))

	// Give time for one poll to execute and fail.
	time.Sleep(1 * time.Second)
	require.NoError(t, rcv.Shutdown(context.Background()))

	// Cursor should NOT have been persisted because ConsumeLogs failed.
	cursorData, err := memClient.Get(context.Background(), "akamai_siem_cursor")
	require.NoError(t, err)
	assert.Nil(t, cursorData, "cursor should NOT be persisted when ConsumeLogs fails")
}

// failingConsumer is a consumer.Logs that always returns an error.
type failingConsumer struct{}

func (f *failingConsumer) ConsumeLogs(_ context.Context, _ plog.Logs) error {
	return fmt.Errorf("simulated ConsumeLogs failure")
}
func (f *failingConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}

// --- test helpers ---

func assertAttr(t *testing.T, m pcommon.Map, key, want string) {
	t.Helper()
	v, ok := m.Get(key)
	require.True(t, ok, "attribute %q missing", key)
	assert.Equal(t, want, v.Str(), "attribute %q", key)
}

func assertAttrI(t *testing.T, m pcommon.Map, key string, want int64) {
	t.Helper()
	v, ok := m.Get(key)
	require.True(t, ok, "attribute %q missing", key)
	assert.Equal(t, want, v.Int(), "attribute %q", key)
}


// TestEmitRaw_BodyIsMap verifies that emitRaw produces a body map with
// a "message" key containing the raw JSON string. The downstream pipeline
// sets elastic.mapping.mode: bodymap via a transform processor.
func TestEmitRaw_BodyIsMap(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.OutputFormat = "raw"
	sink := &consumertest.LogsSink{}
	rcv, err := newAkamaiReceiver(cfg, receivertest.NewNopSettings(NewFactory().Type()), sink)
	require.NoError(t, err)

	err = rcv.emitEvents(context.Background(), []string{`{"test":"event"}`})
	require.NoError(t, err)

	allLogs := sink.AllLogs()
	require.NotEmpty(t, allLogs)
	lr := allLogs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)

	// Body should be a map with "message" key.
	require.Equal(t, pcommon.ValueTypeMap, lr.Body().Type(), "raw mode body should be a map")
	v, ok := lr.Body().Map().Get("message")
	require.True(t, ok, "body map should have 'message' key")
	assert.Equal(t, `{"test":"event"}`, v.Str())
}

// bodyMessage extracts the "message" string from a raw-mode LogRecord body map.
func bodyMessage(t *testing.T, lr plog.LogRecord) string {
	t.Helper()
	require.Equal(t, pcommon.ValueTypeMap, lr.Body().Type(), "raw mode body should be a map")
	v, ok := lr.Body().Map().Get("message")
	require.True(t, ok, "raw mode body map should have 'message' key")
	return v.Str()
}

// mockStorageExtension implements storage.Extension for tests.
type mockStorageExtension struct {
	component.StartFunc
	component.ShutdownFunc
	client *memStorageClient
}

func (m *mockStorageExtension) GetClient(_ context.Context, _ component.Kind, _ component.ID, _ string) (storage.Client, error) {
	return m.client, nil
}

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
func (m *memStorageClient) Close(_ context.Context) error { return nil }

// mockHost returns a component.Host with a mock storage extension registered.
func mockHost(storageClient *memStorageClient) component.Host {
	ext := &mockStorageExtension{client: storageClient}
	return &mockHostImpl{extensions: map[component.ID]component.Component{
		component.MustNewID("file_storage"): ext,
	}}
}

type mockHostImpl struct {
	component.Host
	extensions map[component.ID]component.Component
}

func (h *mockHostImpl) GetExtensions() map[component.ID]component.Component {
	return h.extensions
}
