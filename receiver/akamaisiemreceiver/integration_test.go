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
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// Integration tests verify the exact output of the receiver against realistic
// Akamai SIEM API responses using a local httptest server.
// Test data: testdata/siem_response_full.ndjson — 3 events with different attack
// types, HTTP methods, geo locations, and applied actions.

func TestIntegration_FullResponse(t *testing.T) {
	ndjson, err := os.ReadFile("testdata/siem_response_full.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	sink := &consumertest.LogsSink{}
	rcv := createTestReceiver(t, server.URL, sink)

	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	assert.Eventually(t, func() bool { return sink.LogRecordCount() >= 3 }, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))

	allLogs := sink.AllLogs()
	require.NotEmpty(t, allLogs)

	records := collectRecords(allLogs)
	require.Len(t, records, 3, "expected 3 events from test data")

	for i, lr := range records {
		body := intBodyMessage(t, lr)
		assert.NotEmpty(t, body, "record %d body should not be empty", i)
		assert.Contains(t, body, `"attackData"`, "record %d should contain raw Akamai JSON", i)
		assert.Contains(t, body, `"httpMessage"`, "record %d should contain httpMessage", i)
		assert.Equal(t, 0, lr.Attributes().Len(), "record %d should have no record attributes, got %d", i, lr.Attributes().Len())
	}

	assert.Contains(t, intBodyMessage(t, records[0]), `"appliedAction":"deny"`)
	assert.Contains(t, intBodyMessage(t, records[0]), `"method":"POST"`)
	assert.Contains(t, intBodyMessage(t, records[0]), `"host":"api.example.com"`)

	assert.Contains(t, intBodyMessage(t, records[1]), `"appliedAction":"monitor"`)
	assert.Contains(t, intBodyMessage(t, records[1]), `"method":"GET"`)
	assert.Contains(t, intBodyMessage(t, records[1]), `"country":"BR"`)

	assert.Contains(t, intBodyMessage(t, records[2]), `"appliedAction":"alert"`)
	assert.Contains(t, intBodyMessage(t, records[2]), `"method":"DELETE"`)
	assert.Contains(t, intBodyMessage(t, records[2]), `"status":"500"`)
}

func TestIntegration_EmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, `{"offset":"empty-cursor","total":0,"limit":10000}`)
	}))
	defer server.Close()

	sink := &consumertest.LogsSink{}
	rcv := createTestReceiver(t, server.URL, sink)

	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	time.Sleep(2 * time.Second)
	require.NoError(t, rcv.Shutdown(context.Background()))

	assert.Equal(t, 0, sink.LogRecordCount())
}

func TestIntegration_SeverityUnset(t *testing.T) {
	// The receiver does not interpret event content — severity stays unspecified.
	ndjson, err := os.ReadFile("testdata/siem_response_full.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	sink := &consumertest.LogsSink{}
	rcv := createTestReceiver(t, server.URL, sink)

	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	assert.Eventually(t, func() bool { return sink.LogRecordCount() >= 3 }, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))

	records := collectRecords(sink.AllLogs())
	for i, lr := range records {
		assert.Equal(t, plog.SeverityNumberUnspecified, lr.SeverityNumber(), "record %d should have unspecified severity", i)
	}
}

// --- helpers ---

func createTestReceiver(t *testing.T, serverURL string, sink *consumertest.LogsSink) *akamaiReceiver {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.HTTP.Endpoint = serverURL
	cfg.ConfigIDs = "1"
	cfg.Authentication = EdgeGridAuth{
		ClientToken: configopaque.String("ct"), ClientSecret: configopaque.String("cs"), AccessToken: configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour

	set := receivertest.NewNopSettings(NewFactory().Type())
	rcv, err := newAkamaiReceiver(cfg, set, sink)
	require.NoError(t, err)
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

// intBodyMessage extracts the "message" string from a LogRecord body map.
func intBodyMessage(t *testing.T, lr plog.LogRecord) string {
	t.Helper()
	require.Equal(t, pcommon.ValueTypeMap, lr.Body().Type(), "body should be a map")
	v, ok := lr.Body().Map().Get("message")
	require.True(t, ok, "body map should have 'message' key")
	return v.Str()
}
