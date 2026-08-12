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
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// benchLogRecord is a nested Vercel lambda log line covering string, numeric,
// array, and nested-object decoding.
const benchLogRecord = `{"id":"1573817250283254651097202070","deploymentId":"dpl_233NRGRjVZX1caZrXWtz5g1TAksD","source":"lambda","host":"my-app-abc123.vercel.app","timestamp":1573817250283,"projectId":"gdufoJxB6b9b1fEqr1jUtFkyavUU","level":"info","message":"API request processed","entrypoint":"api/index.js","requestId":"643af4e3-975a-4cc7-9e7a-1eda11539d90","statusCode":200,"path":"/api/users","executionRegion":"sfo1","environment":"production","traceId":"1b02cd14bb8642fd092bc23f54c7ffcd","spanId":"f24e8631bd11faa7","trace.id":"1b02cd14bb8642fd092bc23f54c7ffcd","span.id":"f24e8631bd11faa7","proxy":{"timestamp":1573817250172,"method":"GET","host":"my-app.vercel.app","path":"/api/users?page=1","userAgent":["Mozilla/5.0..."],"referer":"https://my-app.vercel.app","region":"sfo1","statusCode":200,"clientIp":"120.75.16.101","scheme":"https","vercelCache":"MISS"}}`

// benchSpeedInsightsRecordTmpl is a full speed insights record with project id
// and metric type as format verbs, so variants can vary resource identity.
const benchSpeedInsightsRecordTmpl = `{"schema":"vercel.speed_insights.v1","timestamp":"2023-09-14T15:30:00.000Z","projectId":"proj_%d","ownerId":"team_full","deviceId":999,"metricType":"%s","value":2.5,"origin":"https://example.com","path":"/dashboard","route":"/dashboard/[id]","country":"US","region":"CA","city":"San Francisco","osName":"macOS","osVersion":"13.4","clientName":"Chrome","clientType":"browser","clientVersion":"114.0","deviceType":"desktop","deviceBrand":"Apple","connectionSpeed":"4g","browserEngine":"Blink","browserEngineVersion":"114.0","scriptVersion":"1.0.0","sdkVersion":"2.1.0","sdkName":"@vercel/speed-insights","vercelEnvironment":"production","vercelUrl":"example.vercel.app","deploymentId":"dpl_123","attribution":"attr-data"}`

// benchAuditLogRecordTmpl is a full audit log record with project id and action
// as format verbs. Its payload has camelCase keys, nested objects, and arrays,
// so it also exercises the recursive snake_case conversion.
const benchAuditLogRecordTmpl = `{"schema":"vercel.audit_log.v1","id":"uev_O0Sn1S6VHTDuKJ6sNs3hLIEy","teamId":"team_123","projectId":"proj_%d","action":"%s","timestamp":1779444000123,"actor":{"type":"user","id":"user_123","name":"Test User","email":"test@example.com"},"via":[{"type":"app","id":"app_123","name":"Test App"}],"requestId":"req_123","payload":{"drainUrl":"https://example.com/webhook","drainName":"prod-audit-drain","deliveryFormat":"ndjson","httpStatusCode":200,"samplingRate":0.25,"isEnabled":true,"filter":{"environmentTypes":["production","preview"],"sources":["lambda","edge","external"],"maxRetryCount":5,"includeMetadata":true},"headers":[{"headerName":"X-Signature","headerValue":"abc"},{"headerName":"X-Trace-Id","headerValue":"def"}]}}`

// benchWebAnalyticsRecordTmpl is a full web analytics record with project id
// and event type as format verbs, so variants can vary resource identity.
const benchWebAnalyticsRecordTmpl = `{"schema":"vercel.analytics.v2","eventType":"%s","eventName":"button_click","eventData":"{\"button\":\"signup\"}","timestamp":1694723400000,"projectId":"proj_%d","ownerId":"team_full","deviceId":999,"origin":"https://example.com","path":"/dashboard","referrer":"https://google.com","queryParams":"utm_source=newsletter","route":"/dashboard/[id]","country":"US","region":"CA","city":"San Francisco","osName":"macOS","osVersion":"13.4","clientName":"Chrome","clientType":"browser","clientVersion":"114.0","deviceType":"desktop","deviceBrand":"Apple","deviceModel":"MacBook Pro","browserEngine":"Blink","browserEngineVersion":"114.0","sdkVersion":"1.5.0","sdkName":"@vercel/analytics","sdkVersionFull":"1.5.0-canary.1","vercelEnvironment":"production","vercelUrl":"example.vercel.app","flags":"my-flag","deployment":"dpl_123"}`

// repeatRecord builds n identical records, collapsing to one group to isolate
// per-record decode cost.
func repeatRecord(record string) func(int) []string {
	return func(n int) []string {
		records := make([]string, n)
		for i := range records {
			records[i] = record
		}
		return records
	}
}

// benchSpeedInsightsRecord is the single-identity speed insights record.
func benchSpeedInsightsRecord() string {
	return fmt.Sprintf(benchSpeedInsightsRecordTmpl, 0, "LCP")
}

// benchWebAnalyticsRecord is the single-identity web analytics record.
func benchWebAnalyticsRecord() string {
	return fmt.Sprintf(benchWebAnalyticsRecordTmpl, "pageview", 0)
}

// benchAuditLogRecord is the single-identity audit log record.
func benchAuditLogRecord() string {
	return fmt.Sprintf(benchAuditLogRecordTmpl, 0, "drain-created")
}

// webAnalyticsMultiProject varies project id and event type per record to spread
// the batch across resource groups, exercising the grouping in logsBatch.
func webAnalyticsMultiProject(n int) []string {
	const projects = 16
	eventTypes := []string{"pageview", "event"}
	records := make([]string, n)
	for i := range records {
		records[i] = fmt.Sprintf(
			benchWebAnalyticsRecordTmpl,
			eventTypes[i%len(eventTypes)],
			i%projects,
		)
	}
	return records
}

// auditLogsMultiProject varies project id and action per record to spread the
// batch across resource groups, exercising the grouping in logsBatch.
func auditLogsMultiProject(n int) []string {
	const projects = 16
	actions := []string{"drain-created", "project.env.created", "deployment.promoted"}
	records := make([]string, n)
	for i := range records {
		records[i] = fmt.Sprintf(
			benchAuditLogRecordTmpl,
			i%projects,
			actions[i%len(actions)],
		)
	}
	return records
}

// speedInsightsMultiProject varies project id and metric type per record to
// spread the batch across resource, scope, and metric groups in metricsBatch.
func speedInsightsMultiProject(n int) []string {
	const projects = 16
	// Core Web Vitals metric types a real drain mixes within one batch.
	metricTypes := []string{"LCP", "CLS", "FCP", "INP", "TTFB", "FID"}
	records := make([]string, n)
	for i := range records {
		records[i] = fmt.Sprintf(
			benchSpeedInsightsRecordTmpl,
			i%projects,
			metricTypes[i%len(metricTypes)],
		)
	}
	return records
}

// arrayFraming wraps records as a single JSON array.
func arrayFraming(records []string) []byte {
	return []byte("[" + strings.Join(records, ",") + "]")
}

// objectsFraming wraps records as newline-delimited JSON objects.
func objectsFraming(records []string) []byte {
	return []byte(strings.Join(records, "\n"))
}

// BenchmarkParse measures the production parse path: NewVercelParser plus
// draining Next() to io.EOF. It reports B/op and allocs/op across each schema
// and both wire framings (JSON array and NDJSON) at a range of record counts.
func BenchmarkParse(b *testing.B) {
	ext := &vercelEncodingExtension{}

	sizes := []int{100, 1000}

	recordSets := []struct {
		name  string
		build func(int) []string
	}{
		{
			name:  "logs",
			build: repeatRecord(benchLogRecord),
		},
		{
			name:  "speedinsights",
			build: repeatRecord(benchSpeedInsightsRecord()),
		},
		{
			name:  "speedinsights_multiproject",
			build: speedInsightsMultiProject,
		},
		{
			name:  "webanalytics",
			build: repeatRecord(benchWebAnalyticsRecord()),
		},
		{
			name:  "webanalytics_multiproject",
			build: webAnalyticsMultiProject,
		},
		{
			name:  "auditlogs",
			build: repeatRecord(benchAuditLogRecord()),
		},
		{
			name:  "auditlogs_multiproject",
			build: auditLogsMultiProject,
		},
	}

	framings := []struct {
		name string
		wrap func([]string) []byte
	}{
		{
			name: "array",
			wrap: arrayFraming,
		},
		{
			name: "objects",
			wrap: objectsFraming,
		},
	}

	for _, rs := range recordSets {
		for _, framing := range framings {
			for _, n := range sizes {
				payload := framing.wrap(rs.build(n))
				b.Run(fmt.Sprintf("%s/%s/records=%d", rs.name, framing.name, n), func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(payload)))
					for b.Loop() {
						parser, err := ext.NewVercelParser(bytes.NewReader(payload))
						if err != nil {
							b.Fatal(err)
						}
						for {
							if _, err := parser.Next(); err != nil {
								if !errors.Is(err, io.EOF) {
									b.Fatal(err)
								}
								break
							}
						}
					}
				})
			}
		}
	}
}

// snakeCaseSink prevents the compiler from eliminating the toSnakeCase result.
var snakeCaseSink string

// BenchmarkToSnakeCase isolates the key-transform loop per key shape:
// camelCase, acronyms, already snake_case (no-op), and short keys.
func BenchmarkToSnakeCase(b *testing.B) {
	cases := []struct {
		name string
		key  string
	}{
		{
			name: "camelcase",
			key:  "drainUrl",
		},
		{
			name: "acronym_run",
			key:  "drainURL",
		},
		{
			name: "acronym_then_word",
			key:  "HTTPStatusCode",
		},
		{
			name: "already_snake",
			key:  "already_snake_case",
		},
		{
			name: "short",
			key:  "id",
		},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				snakeCaseSink = toSnakeCase(tc.key)
			}
		})
	}
}
