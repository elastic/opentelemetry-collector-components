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

package beatsencodingextension

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// BenchmarkAppendLogRecord isolates the per-record allocation cost:
// one pcommon.Map creation, 6+ body field writes, and 3 attribute writes.
func BenchmarkAppendLogRecord(b *testing.B) {
	ext, err := newBeatsEncodingExtension(&Config{
		Format:     FormatText,
		DataStream: DataStreamConfig{Dataset: "aws.vpcflow", Namespace: "default"},
	}, zap.NewNop())
	if err != nil {
		b.Fatal(err)
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	eventCreated := now.AsTime().UTC().Format(time.RFC3339Nano)
	const record = `2 123456789010 eni-abc123 192.168.1.1 10.0.0.1 12345 80 6 10 840 1620000000 1620000060 ACCEPT OK`

	b.ReportAllocs()
	for b.Loop() {
		logs := plog.NewLogs()
		sl := newScopeLogs(logs)
		ext.appendLogRecord(sl, now, eventCreated, record)
	}
}

// BenchmarkWriteFields isolates the recursive type-switch that maps
// map[string]any values onto a pcommon.Map, covering strings, float64,
// a nested map, and a slice.
func BenchmarkWriteFields(b *testing.B) {
	logger := zap.NewNop()
	fields := map[string]any{
		"environment": "production",
		"version":     float64(1),
		"nested":      map[string]any{"region": "us-east-1", "az": "a"},
		"tags":        []any{"forwarded", "aws-s3"},
	}

	b.ReportAllocs()
	for b.Loop() {
		m := pcommon.NewMap()
		writeFields(logger, m, fields)
	}
}

// BenchmarkUnmarshalLogs_Text covers the end-to-end text path:
// newline scanning → appendLogRecord for each line.
func BenchmarkUnmarshalLogs_Text(b *testing.B) {
	ext, err := newBeatsEncodingExtension(&Config{
		Format:     FormatText,
		DataStream: DataStreamConfig{Dataset: "aws.vpcflow", Namespace: "default"},
	}, zap.NewNop())
	if err != nil {
		b.Fatal(err)
	}

	input, err := os.ReadFile(filepath.Join(testDataDir, "aws_vpcflow.txt"))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(input)))
	for b.Loop() {
		_, _ = ext.UnmarshalLogs(input)
	}
}

// BenchmarkUnmarshalLogs_JSONWithUnwrap covers the end-to-end JSON path:
// navigateToArray → streaming json.Decode per element → appendLogRecord.
func BenchmarkUnmarshalLogs_JSONWithUnwrap(b *testing.B) {
	ext, err := newBeatsEncodingExtension(&Config{
		Format:     FormatJSON,
		Unwrap:     []string{"records"},
		DataStream: DataStreamConfig{Dataset: "azure.events", Namespace: "default"},
	}, zap.NewNop())
	if err != nil {
		b.Fatal(err)
	}

	input, err := os.ReadFile(filepath.Join(testDataDir, "azure_diagnostic_settings.json"))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(input)))
	for b.Loop() {
		_, _ = ext.UnmarshalLogs(input)
	}
}
