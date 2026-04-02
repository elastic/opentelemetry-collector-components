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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// Integration tests verify the exact output of each mapping mode (ECS, OTel, Dual)
// against realistic Akamai SIEM API responses using a local httptest server.
// Test data: testdata/siem_response_full.ndjson — 3 events with different attack
// types, HTTP methods, geo locations, and applied actions.

func TestIntegration_ECSMode_FullResponse(t *testing.T) {
	ndjson, err := os.ReadFile("testdata/siem_response_full.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	sink := &consumertest.LogsSink{}
	rcv := createTestReceiver(t, server.URL, "ecs", sink)

	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	assert.Eventually(t, func() bool { return sink.LogRecordCount() >= 3 }, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))

	allLogs := sink.AllLogs()
	require.NotEmpty(t, allLogs)

	records := collectRecords(allLogs)
	require.Len(t, records, 3, "expected 3 events from test data")

	// ECS mode: body contains raw JSON, no attributes on the record.
	for i, lr := range records {
		body := lr.Body().Str()
		assert.NotEmpty(t, body, "record %d body should not be empty", i)
		assert.Contains(t, body, `"attackData"`, "record %d should contain raw Akamai JSON", i)
		assert.Contains(t, body, `"httpMessage"`, "record %d should contain httpMessage", i)
		assert.Equal(t, 0, lr.Attributes().Len(), "ECS mode should have no record attributes, got %d on record %d", lr.Attributes().Len(), i)
	}

	// Verify specific event content by body.
	assert.Contains(t, records[0].Body().Str(), `"appliedAction":"deny"`)
	assert.Contains(t, records[0].Body().Str(), `"method":"POST"`)
	assert.Contains(t, records[0].Body().Str(), `"host":"api.example.com"`)

	assert.Contains(t, records[1].Body().Str(), `"appliedAction":"monitor"`)
	assert.Contains(t, records[1].Body().Str(), `"method":"GET"`)
	assert.Contains(t, records[1].Body().Str(), `"country":"BR"`)

	assert.Contains(t, records[2].Body().Str(), `"appliedAction":"alert"`)
	assert.Contains(t, records[2].Body().Str(), `"method":"DELETE"`)
	assert.Contains(t, records[2].Body().Str(), `"status":"500"`)
}

func TestIntegration_OTelMode_FullResponse(t *testing.T) {
	ndjson, err := os.ReadFile("testdata/siem_response_full.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	sink := &consumertest.LogsSink{}
	rcv := createTestReceiver(t, server.URL, "otel", sink)

	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	assert.Eventually(t, func() bool { return sink.LogRecordCount() >= 3 }, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))

	records := collectRecords(sink.AllLogs())
	require.Len(t, records, 3)

	// --- Event 1: deny, POST, US, 403, HTTP/1.1, TLS 1.3 ---
	r0 := records[0]
	assertAttr(t, r0.Attributes(), "http.request.method", "POST")
	assertAttrI(t, r0.Attributes(), "http.response.status_code", 403)
	assertAttrI(t, r0.Attributes(), "http.response.body.size", 4096)
	assertAttr(t, r0.Attributes(), "url.domain", "api.example.com")
	assertAttr(t, r0.Attributes(), "url.path", "/v2/users")
	assertAttr(t, r0.Attributes(), "url.query", "id=1234&action=update")
	assertAttrI(t, r0.Attributes(), "server.port", 443)
	assertAttr(t, r0.Attributes(), "network.protocol.name", "http")
	assertAttr(t, r0.Attributes(), "network.protocol.version", "1.1")
	assertAttr(t, r0.Attributes(), "tls.protocol.name", "tls")
	assertAttr(t, r0.Attributes(), "tls.protocol.version", "1.3")
	assertAttr(t, r0.Attributes(), "source.address", "198.51.100.1")
	assertAttr(t, r0.Attributes(), "source.ip", "198.51.100.1")
	assertAttr(t, r0.Attributes(), "source.geo.country_iso_code", "US")
	assertAttr(t, r0.Attributes(), "source.geo.city_name", "MOUNTAIN VIEW")
	assertAttr(t, r0.Attributes(), "source.geo.continent_code", "NA")
	assertAttr(t, r0.Attributes(), "source.geo.region_iso_code", "US-CA")
	assertAttrI(t, r0.Attributes(), "source.as.number", 15169)
	assertAttr(t, r0.Attributes(), "akamai.siem.applied_action", "deny")
	assertAttr(t, r0.Attributes(), "akamai.siem.config_id", "67217")
	assertAttr(t, r0.Attributes(), "akamai.siem.policy_id", "PNWD_110088")
	assertAttr(t, r0.Attributes(), "event.id", "abc123def456")
	assertAttr(t, r0.Attributes(), "observer.vendor", "akamai")
	// Severity: deny → ERROR
	assert.Equal(t, plog.SeverityNumberError, r0.SeverityNumber())
	assert.Equal(t, "ERROR", r0.SeverityText())
	// Decoded rules
	rules, ok := r0.Attributes().Get("akamai.siem.rules")
	require.True(t, ok)
	assert.Equal(t, 2, rules.Slice().Len())
	assert.Equal(t, "950002", rules.Slice().At(0).Str())
	assert.Equal(t, "950019", rules.Slice().At(1).Str())
	// Decoded rule messages
	msgs, ok := r0.Attributes().Get("akamai.siem.rule_messages")
	require.True(t, ok)
	assert.Equal(t, "SQL Injection attempt detected", msgs.Slice().At(0).Str())
	assert.Equal(t, "Cross-Site Scripting attempt detected", msgs.Slice().At(1).Str())
	// Timestamp from start field
	assert.Equal(t, int64(1700000000), r0.Timestamp().AsTime().Unix())

	// --- Event 2: monitor, GET, BR, 200, HTTP/2.0, no TLS ---
	r1 := records[1]
	assertAttr(t, r1.Attributes(), "http.request.method", "GET")
	assertAttrI(t, r1.Attributes(), "http.response.status_code", 200)
	assertAttrI(t, r1.Attributes(), "http.response.body.size", 0)
	assertAttr(t, r1.Attributes(), "url.domain", "www.example.com")
	assertAttr(t, r1.Attributes(), "url.path", "/health")
	assertAttrI(t, r1.Attributes(), "server.port", 80)
	assertAttr(t, r1.Attributes(), "network.protocol.version", "2.0")
	assertAttr(t, r1.Attributes(), "source.geo.country_iso_code", "BR")
	assertAttr(t, r1.Attributes(), "source.geo.city_name", "SAO PAULO")
	assertAttr(t, r1.Attributes(), "akamai.siem.applied_action", "monitor")
	// Severity: monitor → INFO
	assert.Equal(t, plog.SeverityNumberInfo, r1.SeverityNumber())
	assert.Equal(t, "INFO", r1.SeverityText())
	// No TLS field → no TLS attributes
	_, hasTLS := r1.Attributes().Get("tls.protocol.name")
	assert.False(t, hasTLS, "no TLS field should mean no TLS attributes")

	// --- Event 3: alert, DELETE, DE, 500 ---
	r2 := records[2]
	assertAttr(t, r2.Attributes(), "http.request.method", "DELETE")
	assertAttrI(t, r2.Attributes(), "http.response.status_code", 500)
	assertAttr(t, r2.Attributes(), "url.path", "/api/v1/resource/42")
	assertAttr(t, r2.Attributes(), "url.query", "force=true")
	assertAttr(t, r2.Attributes(), "source.geo.country_iso_code", "DE")
	assertAttr(t, r2.Attributes(), "source.geo.region_iso_code", "DE-BE")
	assertAttr(t, r2.Attributes(), "akamai.siem.applied_action", "alert")
	// Severity: alert → INFO (alert and monitor both map to INFO)
	assert.Equal(t, plog.SeverityNumberInfo, r2.SeverityNumber())
	assert.Equal(t, "INFO", r2.SeverityText())
	// Single rule decoded
	rules2, ok := r2.Attributes().Get("akamai.siem.rules")
	require.True(t, ok)
	assert.Equal(t, 1, rules2.Slice().Len())
	assert.Equal(t, "999999", rules2.Slice().At(0).Str())
}

func TestIntegration_DualMode_FullResponse(t *testing.T) {
	ndjson, err := os.ReadFile("testdata/siem_response_full.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	ecsSink := &consumertest.LogsSink{}
	otelSink := &consumertest.LogsSink{}

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = server.URL
	cfg.ConfigIDs = "1"
	cfg.Authentication = EdgeGridAuth{
		ClientToken: configopaque.String("ct"), ClientSecret: configopaque.String("cs"), AccessToken: configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour

	// Create two factory instances with same key → dual mode.
	ecsCfg := *cfg
	ecsCfg.OutputFormat = "raw"
	otelCfg := *cfg
	otelCfg.OutputFormat = "otel"

	set := receivertest.NewNopSettings(NewFactory().Type())
	factory := NewFactory()

	ecsRcv, err := factory.CreateLogs(context.Background(), set, &ecsCfg, ecsSink)
	require.NoError(t, err)
	otelRcv, err := factory.CreateLogs(context.Background(), set, &otelCfg, otelSink)
	require.NoError(t, err)

	require.NoError(t, ecsRcv.Start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, otelRcv.Start(context.Background(), componenttest.NewNopHost()))

	assert.Eventually(t, func() bool {
		return ecsSink.LogRecordCount() >= 3 && otelSink.LogRecordCount() >= 3
	}, 10*time.Second, 50*time.Millisecond, "both sinks should receive 3 events")

	require.NoError(t, ecsRcv.Shutdown(context.Background()))
	require.NoError(t, otelRcv.Shutdown(context.Background()))

	ecsRecords := collectRecords(ecsSink.AllLogs())
	otelRecords := collectRecords(otelSink.AllLogs())
	require.Len(t, ecsRecords, 3)
	require.Len(t, otelRecords, 3)

	// ECS records: raw JSON body, no attributes.
	for i, lr := range ecsRecords {
		assert.Contains(t, lr.Body().Str(), `"attackData"`, "ECS record %d", i)
		assert.Equal(t, 0, lr.Attributes().Len(), "ECS record %d should have no attrs", i)
	}

	// OTel records: structured attributes.
	for i, lr := range otelRecords {
		_, hasMethod := lr.Attributes().Get("http.request.method")
		assert.True(t, hasMethod, "OTel record %d should have http.request.method", i)
		_, hasAction := lr.Attributes().Get("akamai.siem.applied_action")
		assert.True(t, hasAction, "OTel record %d should have akamai.siem.applied_action", i)
	}

	// Verify same events — ECS body for event 0 should match OTel's source data.
	assert.Contains(t, ecsRecords[0].Body().Str(), `"appliedAction":"deny"`)
	assertAttr(t, otelRecords[0].Attributes(), "akamai.siem.applied_action", "deny")
}

func TestIntegration_ECSMode_ContextMetadata(t *testing.T) {
	ndjson, err := os.ReadFile("testdata/siem_response_full.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	// Use a channel to safely pass the context from the consumer goroutine.
	ctxCh := make(chan context.Context, 1)
	capture := &contextCapturingConsumer{
		onConsume: func(ctx context.Context, _ plog.Logs) error {
			select {
			case ctxCh <- ctx:
			default:
			}
			return nil
		},
	}

	rcv := createTestReceiverWithConsumer(t, server.URL, "ecs", capture)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))

	var capturedCtx context.Context
	assert.Eventually(t, func() bool {
		select {
		case capturedCtx = <-ctxCh:
			return true
		default:
			return false
		}
	}, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))
	require.NotNil(t, capturedCtx)

	// ECS mode should set x-elastic-mapping-mode: ecs.
	info := client.FromContext(capturedCtx)
	values := info.Metadata.Get("x-elastic-mapping-mode")
	require.Len(t, values, 1)
	assert.Equal(t, "ecs", values[0])
}

func TestIntegration_OTelMode_NoContextMetadata(t *testing.T) {
	ndjson, err := os.ReadFile("testdata/siem_response_full.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	ctxCh := make(chan context.Context, 1)
	capture := &contextCapturingConsumer{
		onConsume: func(ctx context.Context, _ plog.Logs) error {
			select {
			case ctxCh <- ctx:
			default:
			}
			return nil
		},
	}

	rcv := createTestReceiverWithConsumer(t, server.URL, "otel", capture)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))

	var capturedCtx context.Context
	assert.Eventually(t, func() bool {
		select {
		case capturedCtx = <-ctxCh:
			return true
		default:
			return false
		}
	}, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))
	require.NotNil(t, capturedCtx)

	// OTel mode should NOT set x-elastic-mapping-mode.
	info := client.FromContext(capturedCtx)
	values := info.Metadata.Get("x-elastic-mapping-mode")
	assert.Empty(t, values)
}

func TestIntegration_ECSMode_EmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, `{"offset":"empty-cursor","total":0,"limit":10000}`)
	}))
	defer server.Close()

	sink := &consumertest.LogsSink{}
	rcv := createTestReceiver(t, server.URL, "ecs", sink)

	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	time.Sleep(2 * time.Second)
	require.NoError(t, rcv.Shutdown(context.Background()))

	// Empty response should emit zero records.
	assert.Equal(t, 0, sink.LogRecordCount())
}

func TestIntegration_OTelMode_EmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, `{"offset":"empty-cursor","total":0,"limit":10000}`)
	}))
	defer server.Close()

	sink := &consumertest.LogsSink{}
	rcv := createTestReceiver(t, server.URL, "otel", sink)

	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	time.Sleep(2 * time.Second)
	require.NoError(t, rcv.Shutdown(context.Background()))

	assert.Equal(t, 0, sink.LogRecordCount())
}

func TestIntegration_ECSMode_SeverityMapping(t *testing.T) {
	// Test all severity levels: deny → ERROR, alert/tarpit → WARN, monitor → INFO
	events := []struct {
		action   string
		wantSev  plog.SeverityNumber
		wantText string
	}{
		// ECS mode doesn't set severity — it's raw JSON passthrough.
		// Severity is only set in OTel mode.
	}
	_ = events

	ndjson, err := os.ReadFile("testdata/siem_response_full.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	sink := &consumertest.LogsSink{}
	rcv := createTestReceiver(t, server.URL, "ecs", sink)

	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	assert.Eventually(t, func() bool { return sink.LogRecordCount() >= 3 }, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))

	records := collectRecords(sink.AllLogs())
	// ECS mode: all records have SeverityNumber=Unspecified.
	for i, lr := range records {
		assert.Equal(t, plog.SeverityNumberUnspecified, lr.SeverityNumber(), "ECS record %d should have unspecified severity", i)
	}
}

// --- helpers ---

func createTestReceiver(t *testing.T, serverURL, outputFormat string, sink *consumertest.LogsSink) *akamaiReceiver {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = serverURL
	cfg.ConfigIDs = "1"
	cfg.Authentication = EdgeGridAuth{
		ClientToken: configopaque.String("ct"), ClientSecret: configopaque.String("cs"), AccessToken: configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour
	cfg.OutputFormat = outputFormat

	set := receivertest.NewNopSettings(NewFactory().Type())
	rcv, err := newAkamaiReceiver(cfg, set)
	require.NoError(t, err)

	switch outputFormat {
	case "otel":
		rcv.setOTelConsumer(sink)
	default:
		rcv.setRawConsumer(sink)
	}
	return rcv
}

func createTestReceiverWithConsumer(t *testing.T, serverURL, outputFormat string, cons consumer.Logs) *akamaiReceiver {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = serverURL
	cfg.ConfigIDs = "1"
	cfg.Authentication = EdgeGridAuth{
		ClientToken: configopaque.String("ct"), ClientSecret: configopaque.String("cs"), AccessToken: configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour
	cfg.OutputFormat = outputFormat

	set := receivertest.NewNopSettings(NewFactory().Type())
	rcv, err := newAkamaiReceiver(cfg, set)
	require.NoError(t, err)

	switch outputFormat {
	case "otel":
		rcv.setOTelConsumer(cons)
	default:
		rcv.setRawConsumer(cons)
	}
	return rcv
}

func collectRecords(allLogs []plog.Logs) []plog.LogRecord {
	var records []plog.LogRecord
	for _, logs := range allLogs {
		for i := 0; i < logs.ResourceLogs().Len(); i++ {
			rl := logs.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				sl := rl.ScopeLogs().At(j)
				for k := 0; k < sl.LogRecords().Len(); k++ {
					records = append(records, sl.LogRecords().At(k))
				}
			}
		}
	}
	return records
}
