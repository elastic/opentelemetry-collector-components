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

package vercelencodingextension

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/elastic/opentelemetry-collector-components/internal/vercel"
)

func TestExtensionFactoryType(t *testing.T) {
	require.Equal(t, component.MustNewType("vercel_encoding"), NewFactory().Type())
}

func TestDetectsSignalAndDecodesPayload(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		filename         string
		expectedFilename string
		metrics          bool
	}{
		"speed_insights_json": {
			filename:         "speed-insights/speed_insights.json",
			expectedFilename: "speed-insights/expected.yaml",
			metrics:          true,
		},
		"speed_insights_ndjson": {
			filename:         "speed-insights/speed_insights.ndjson",
			expectedFilename: "speed-insights/expected.yaml",
			metrics:          true,
		},
		"speed_insights_full": {
			filename:         "speed-insights/full.json",
			expectedFilename: "speed-insights/full_expected.yaml",
			metrics:          true,
		},
		"speed_insights_multi_project": {
			filename:         "speed-insights/multi_project.json",
			expectedFilename: "speed-insights/multi_project_expected.yaml",
			metrics:          true,
		},
		"web_analytics_json": {
			filename:         "web-analytics/web_analytics.json",
			expectedFilename: "web-analytics/expected.yaml",
		},
		"web_analytics_ndjson": {
			filename:         "web-analytics/web_analytics.ndjson",
			expectedFilename: "web-analytics/expected.yaml",
		},
		"web_analytics_full": {
			filename:         "web-analytics/full.json",
			expectedFilename: "web-analytics/full_expected.yaml",
		},
		"web_analytics_multi_project": {
			filename:         "web-analytics/multi_project.json",
			expectedFilename: "web-analytics/multi_project_expected.yaml",
		},
		"audit_logs_json": {
			filename:         "audit-logs/audit_logs.json",
			expectedFilename: "audit-logs/expected.yaml",
		},
		"audit_logs_ndjson": {
			filename:         "audit-logs/audit_logs.ndjson",
			expectedFilename: "audit-logs/expected.yaml",
		},
		"audit_logs_full": {
			filename:         "audit-logs/full.json",
			expectedFilename: "audit-logs/full_expected.yaml",
		},
		"audit_logs_multi_project": {
			filename:         "audit-logs/multi_project.json",
			expectedFilename: "audit-logs/multi_project_expected.yaml",
		},
		"logs_json_objects": {
			filename:         "logs/logs.json",
			expectedFilename: "logs/expected.yaml",
		},
		"logs_full": {
			filename:         "logs/full.json",
			expectedFilename: "logs/full_expected.yaml",
		},
		"logs_ndjson": {
			filename:         "logs/logs.ndjson",
			expectedFilename: "logs/expected.yaml",
		},
	}

	ext := createTestExtension(t)
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", tc.filename))
			require.NoError(t, err)

			parser, err := ext.NewVercelParser(bytes.NewReader(data))
			require.NoError(t, err)
			parsedPayloads := drainPayloads(t, parser)
			require.NotEmpty(t, parsedPayloads)

			if tc.metrics {
				expectedMetrics, readErr := golden.ReadMetrics(filepath.Join("testdata", tc.expectedFilename))
				require.NoError(t, readErr)
				for _, parsed := range parsedPayloads {
					require.Equal(t, vercel.SignalMetrics, parsed.Signal)
					require.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, parsed.Metrics))
				}
				return
			}

			expected, readErr := golden.ReadLogs(filepath.Join("testdata", tc.expectedFilename))
			require.NoError(t, readErr)
			for _, parsed := range parsedPayloads {
				require.Equal(t, vercel.SignalLogs, parsed.Signal)
				require.NoError(t, plogtest.CompareLogs(expected, parsed.Logs))
			}
		})
	}
}

func TestIntegerPrecision(t *testing.T) {
	t.Parallel()

	const body = `{"schema":"vercel.audit_log.v1","id":"first","teamId":"team_123","action":"first","timestamp":1779444000123,"payload":{"bigId":9007199254740993,"nested":{"nestedId":9007199254740995},"items":[{"itemId":9007199254740997}]}}
{"schema":"vercel.audit_log.v1","id":"second","teamId":"team_123","action":"second","timestamp":1779444000456,"payload":{"bigId":9007199254740999,"nested":{"nestedId":9007199254741001},"items":[{"itemId":9007199254741003}]}}`

	ext := createTestExtension(t)
	parser, err := ext.NewVercelParser(bytes.NewReader([]byte(body)), encoding.WithFlushItems(0))
	require.NoError(t, err)

	payloads := drainPayloads(t, parser)
	require.Len(t, payloads, 1)
	require.Equal(t, vercel.SignalLogs, payloads[0].Signal)

	records := payloads[0].Logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	require.Equal(t, 2, records.Len())

	cases := []struct {
		name         string
		recordIndex  int
		wantBigID    int64
		wantNestedID int64
		wantItemID   int64
	}{
		{
			name:         "raw first record",
			recordIndex:  0,
			wantBigID:    9007199254740993,
			wantNestedID: 9007199254740995,
			wantItemID:   9007199254740997,
		},
		{
			name:         "streamed second record",
			recordIndex:  1,
			wantBigID:    9007199254740999,
			wantNestedID: 9007199254741001,
			wantItemID:   9007199254741003,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payloadAttr, ok := records.At(tc.recordIndex).Attributes().Get(attrVercelAuditLogPayload)
			require.True(t, ok)

			auditPayload := payloadAttr.Map()
			requireIntValue(t, auditPayload, "big_id", tc.wantBigID)

			nested, ok := auditPayload.Get("nested")
			require.True(t, ok)
			requireIntValue(t, nested.Map(), "nested_id", tc.wantNestedID)

			items, ok := auditPayload.Get("items")
			require.True(t, ok)
			requireIntValue(t, items.Slice().At(0).Map(), "item_id", tc.wantItemID)
		})
	}
}

func requireIntValue(t *testing.T, attrs pcommon.Map, key string, want int64) {
	t.Helper()

	value, ok := attrs.Get(key)
	require.True(t, ok)
	require.Equal(t, pcommon.ValueTypeInt, value.Type())
	require.Equal(t, want, value.Int())
}

func TestRawRecordSchema(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		raw     string
		want    schema
		wantErr error
	}{
		{
			name: "speed insights schema",
			raw:  `{"schema":"vercel.speed_insights.v1"}`,
			want: schemaSpeedInsights,
		},
		{
			name: "web analytics schema",
			raw:  `{"schema":"vercel.analytics.v2"}`,
			want: schemaWebAnalytics,
		},
		{
			name: "audit logs schema",
			raw:  `{"schema":"vercel.audit_log.v1"}`,
			want: schemaAuditLogs,
		},
		{
			name: "missing schema",
			raw:  `{"message":"log"}`,
			want: schemaLogs,
		},
		{
			name:    "unknown schema",
			raw:     `{"schema":"vercel.unknown.v1"}`,
			wantErr: errUnsupportedVercelSchema,
		},
		{
			name:    "empty schema",
			raw:     `{"schema":""}`,
			wantErr: errUnsupportedVercelSchema,
		},
		{
			name:    "non-string schema",
			raw:     `{"schema":123}`,
			wantErr: errUnsupportedVercelSchema,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := (rawRecord{raw: []byte(tc.raw)}).schema()
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestParserBatchesRecordsByFlushItems verifies flush item batching.
func TestParserBatchesRecordsByFlushItems(t *testing.T) {
	t.Parallel()

	ext := createTestExtension(t)
	body := []byte(`{"message":"one"}
{"message":"two"}
{"message":"three"}`)

	cases := []struct {
		name       string
		flushItems int64
		want       int
	}{
		{
			name:       "flush all",
			flushItems: 0,
			want:       1,
		},
		{
			name:       "one per payload",
			flushItems: 1,
			want:       3,
		},
		{
			name:       "two per payload",
			flushItems: 2,
			want:       2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parser, err := ext.NewVercelParser(bytes.NewReader(body), encoding.WithFlushItems(tc.flushItems))
			require.NoError(t, err)

			payloads := drainPayloads(t, parser)
			require.Len(t, payloads, tc.want)
			for _, payload := range payloads {
				require.Equal(t, vercel.SignalLogs, payload.Signal)
			}
		})
	}
}

// TestParserRoutesRequestByFirstRecordSchema verifies later schemas do not
// reroute a request.
func TestParserRoutesRequestByFirstRecordSchema(t *testing.T) {
	t.Parallel()

	ext := createTestExtension(t)

	cases := []struct {
		name       string
		body       string
		wantSignal vercel.Signal
	}{
		{
			name:       "trailing record with different signal is ignored for routing",
			body:       `[{"message":"log one"},{"schema":"vercel.speed_insights.v1"}]`,
			wantSignal: vercel.SignalLogs,
		},
		{
			name:       "trailing record with different schema is ignored for routing",
			body:       `[{"message":"log one"},{"schema":"vercel.audit_log.v1"}]`,
			wantSignal: vercel.SignalLogs,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parser, err := ext.NewVercelParser(bytes.NewReader([]byte(tc.body)), encoding.WithFlushItems(0))
			require.NoError(t, err)

			payloads := drainPayloads(t, parser)
			require.Len(t, payloads, 1)
			require.Equal(t, tc.wantSignal, payloads[0].Signal)
		})
	}
}

func createTestExtension(t *testing.T) *vercelEncodingExtension {
	t.Helper()

	factory := NewFactory()
	ext, err := factory.Create(t.Context(), extensiontest.NewNopSettings(factory.Type()), factory.CreateDefaultConfig())
	require.NoError(t, err)

	vercelExt, ok := ext.(vercel.EncodingExtension)
	require.True(t, ok)

	encodingExt, ok := ext.(*vercelEncodingExtension)
	require.True(t, ok)

	var _ extension.Extension = vercelExt

	return encodingExt
}

func drainPayloads(t *testing.T, parser vercel.PayloadParser) []vercel.Payload {
	t.Helper()

	var payloads []vercel.Payload
	for {
		payload, err := parser.Next()
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			return payloads
		}
		payloads = append(payloads, payload)
	}
}
