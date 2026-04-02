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
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestTracing_SpansCreated verifies that the receiver creates the expected
// trace spans when a TracerProvider is configured.
func TestTracing_SpansCreated(t *testing.T) {
	ndjson, err := os.ReadFile("testdata/siem_response.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	// Set up in-memory span exporter to capture spans.
	spanExporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(spanExporter),
	)
	defer func() { _ = tp.Shutdown(context.Background()) }()

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
	storageID := component.MustNewID("file_storage")
	cfg.StorageID = &storageID

	memClient := newMemStorageClient()

	sink := &consumertest.LogsSink{}
	set := receivertest.NewNopSettings(NewFactory().Type())
	// Inject our test TracerProvider.
	set.TracerProvider = tp

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), mockHost(memClient)))

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 2
	}, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))

	// Force flush to ensure all spans are exported.
	_ = tp.ForceFlush(context.Background())

	spans := spanExporter.GetSpans()
	require.NotEmpty(t, spans, "expected trace spans to be created")

	// Collect span names.
	spanNames := make(map[string]int)
	for _, s := range spans {
		spanNames[s.Name]++
	}

	// Verify expected spans exist.
	assert.Contains(t, spanNames, "akamai_siem.Poll", "Poll span missing")
	assert.Contains(t, spanNames, "akamai_siem.FetchPage", "FetchPage span missing")
	assert.Contains(t, spanNames, "akamai_siem.ProcessPage", "ProcessPage span missing")
	assert.Contains(t, spanNames, "akamai_siem.MapEvents", "MapEvents span missing")
	assert.Contains(t, spanNames, "akamai_siem.PersistCursor", "PersistCursor span missing")

	// Verify parent-child: FetchPage and ProcessPage should be children of Poll.
	var pollSpanID string
	for _, s := range spans {
		if s.Name == "akamai_siem.Poll" {
			pollSpanID = s.SpanContext.SpanID().String()
			break
		}
	}
	require.NotEmpty(t, pollSpanID, "Poll span not found")

	for _, s := range spans {
		if s.Name == "akamai_siem.FetchPage" || s.Name == "akamai_siem.ProcessPage" || s.Name == "akamai_siem.PersistCursor" {
			assert.Equal(t, pollSpanID, s.Parent.SpanID().String(),
				"span %q should be a child of Poll", s.Name)
		}
	}
}

// TestTracing_OTelMode_SpansCreated verifies spans in OTel mapping mode.
func TestTracing_OTelMode_SpansCreated(t *testing.T) {
	ndjson, err := os.ReadFile("testdata/siem_response.ndjson")
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(ndjson)
	}))
	defer server.Close()

	spanExporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spanExporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

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
	set.TracerProvider = tp

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	require.Eventually(t, func() bool { return sink.LogRecordCount() >= 2 }, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))
	_ = tp.ForceFlush(context.Background())

	spans := spanExporter.GetSpans()

	// MapEvents span should have output_format=otel attribute.
	var foundMapSpan bool
	for _, s := range spans {
		if s.Name == "akamai_siem.MapEvents" {
			foundMapSpan = true
			for _, a := range s.Attributes {
				if string(a.Key) == "output_format" {
					assert.Equal(t, "otel", a.Value.AsString())
				}
				if string(a.Key) == "event_count" {
					assert.Equal(t, int64(2), a.Value.AsInt64())
				}
			}
		}
	}
	assert.True(t, foundMapSpan, "MapEvents span missing in otel mode")
}

// TestTracing_MappingError_SpanStatus verifies that mapping errors set
// span status to Error.
func TestTracing_MappingError_SpanStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, `{"httpMessage":{"start":"1000","host":"good.com","status":"200"}}`)
		_, _ = fmt.Fprintln(w, `{broken json}`)
		_, _ = fmt.Fprintln(w, `{"offset":"x","total":2,"limit":10000}`)
	}))
	defer server.Close()

	spanExporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spanExporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

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
	set.TracerProvider = tp

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	require.Eventually(t, func() bool { return sink.LogRecordCount() >= 1 }, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))
	_ = tp.ForceFlush(context.Background())

	spans := spanExporter.GetSpans()

	// MapEvents span should have error status due to mapping failure.
	for _, s := range spans {
		if s.Name == "akamai_siem.MapEvents" {
			assert.Equal(t, sdktrace.Status{
				Code:        2, // codes.Error
				Description: "1/2 events failed mapping",
			}.Description, s.Status.Description,
				"MapEvents span should have error status with mapping failure count")
			// Should have mapping_errors attribute.
			for _, a := range s.Attributes {
				if string(a.Key) == "mapping_errors" {
					assert.Equal(t, int64(1), a.Value.AsInt64())
				}
			}
		}
	}
}

// TestTracing_NoSpans_WhenDisabled verifies zero overhead when no
// TracerProvider is configured (default).
func TestTracing_NoSpans_WhenDisabled(t *testing.T) {
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

	sink := &consumertest.LogsSink{}
	// Default NopSettings — no TracerProvider configured.
	set := receivertest.NewNopSettings(NewFactory().Type())

	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	require.Eventually(t, func() bool { return sink.LogRecordCount() >= 1 }, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))

	// No way to capture spans from nop provider — this test verifies no panics
	// or errors occur when tracing is disabled (the default production path).
}
