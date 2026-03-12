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
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

const testDataDir = "testdata"

// Set to true to regenerate golden files, then set back to false.
var updateGoldenFiles = false

func TestUnmarshalLogs(t *testing.T) {
	tests := []struct {
		name       string
		config     Config
		inputFile  string
		goldenFile string
		wantLogs   int
	}{
		{
			name: "azure diagnostic settings (json + unwrap)",
			config: Config{
				Format:      FormatJSON,
				Unwrap:      "$.records[*]",
				TargetField: "message",
				Routing:     RoutingConfig{Dataset: "azure.events", Namespace: "default"},
			},
			inputFile:  "azure_diagnostic_settings.json",
			goldenFile: "azure_diagnostic_settings_expected.yaml",
			wantLogs:   2,
		},
		{
			name: "aws cloudtrail (json + unwrap)",
			config: Config{
				Format:      FormatJSON,
				Unwrap:      "$.Records[*]",
				TargetField: "message",
				Routing:     RoutingConfig{Dataset: "aws.cloudtrail", Namespace: "default"},
			},
			inputFile:  "aws_cloudtrail.json",
			goldenFile: "aws_cloudtrail_expected.yaml",
			wantLogs:   2,
		},
		{
			name: "aws vpc flow logs (text)",
			config: Config{
				Format:      FormatText,
				TargetField: "message",
				Routing:     RoutingConfig{Dataset: "aws.vpcflow", Namespace: "default"},
			},
			inputFile:  "aws_vpcflow.txt",
			goldenFile: "aws_vpcflow_expected.yaml",
			wantLogs:   3,
		},
		{
			name: "aws elb access logs (text)",
			config: Config{
				Format:      FormatText,
				TargetField: "message",
				Routing:     RoutingConfig{Dataset: "aws.elb_logs", Namespace: "default"},
			},
			inputFile:  "aws_elb.txt",
			goldenFile: "aws_elb_expected.yaml",
			wantLogs:   2,
		},
		{
			name: "json without unwrap (single record)",
			config: Config{
				Format:      FormatJSON,
				TargetField: "message",
				Routing:     RoutingConfig{Dataset: "generic", Namespace: "default"},
			},
			inputFile:  "json_single.json",
			goldenFile: "json_single_expected.yaml",
			wantLogs:   1,
		},
		{
			name: "json nested path unwrap",
			config: Config{
				Format:      FormatJSON,
				Unwrap:      "$.data.items[*]",
				TargetField: "message",
				Routing:     RoutingConfig{Dataset: "custom.nested", Namespace: "default"},
			},
			inputFile:  "json_nested.json",
			goldenFile: "json_nested_expected.yaml",
			wantLogs:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ext, err := newBeatsEncodingExtension(&tt.config, zap.NewNop())
			require.NoError(t, err)

			input, err := os.ReadFile(filepath.Join(testDataDir, tt.inputFile))
			require.NoError(t, err)

			logs, err := ext.UnmarshalLogs(input)
			require.NoError(t, err)
			require.Equal(t, tt.wantLogs, logs.LogRecordCount())

			goldenPath := filepath.Join(testDataDir, tt.goldenFile)

			stripEventCreated(logs)

			if updateGoldenFiles {
				require.NoError(t, golden.WriteLogsToFile(goldenPath, logs))
				t.Log("Golden file written to", goldenPath)
			}

			expected, err := golden.ReadLogs(goldenPath)
			require.NoError(t, err)

			require.NoError(t, plogtest.CompareLogs(
				expected, logs,
				plogtest.IgnoreObservedTimestamp(),
				plogtest.IgnoreTimestamp(),
			))
		})
	}
}

func TestUnmarshalLogs_EmptyInput(t *testing.T) {
	tests := []struct {
		name   string
		format Format
		input  []byte
	}{
		{name: "empty json", format: FormatJSON, input: []byte("")},
		{name: "whitespace json", format: FormatJSON, input: []byte("   ")},
		{name: "empty text", format: FormatText, input: []byte("")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ext, err := newBeatsEncodingExtension(&Config{
				Format:      tt.format,
				TargetField: "message",
				Routing:     RoutingConfig{Dataset: "test", Namespace: "default"},
			}, zap.NewNop())
			require.NoError(t, err)

			logs, err := ext.UnmarshalLogs(tt.input)
			require.NoError(t, err)
			assert.Equal(t, 0, logs.LogRecordCount())
		})
	}
}

func TestUnmarshalLogs_UnwrapFieldMissing(t *testing.T) {
	ext, err := newBeatsEncodingExtension(&Config{
		Format:      FormatJSON,
		Unwrap:      "$.records[*]",
		TargetField: "message",
		Routing:     RoutingConfig{Dataset: "test", Namespace: "default"},
	}, zap.NewNop())
	require.NoError(t, err)

	input, err := os.ReadFile(filepath.Join(testDataDir, "json_no_records_field.json"))
	require.NoError(t, err)

	_, err = ext.UnmarshalLogs(input)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `key "records" not found`)
}

func TestUnmarshalLogs_StructuralChecks(t *testing.T) {
	ext, err := newBeatsEncodingExtension(&Config{
		Format:      FormatJSON,
		Unwrap:      "$.records[*]",
		TargetField: "message",
		Routing:     RoutingConfig{Dataset: "azure.events", Namespace: "default"},
	}, zap.NewNop())
	require.NoError(t, err)

	input, err := os.ReadFile(filepath.Join(testDataDir, "azure_diagnostic_settings.json"))
	require.NoError(t, err)

	logs, err := ext.UnmarshalLogs(input)
	require.NoError(t, err)

	require.Equal(t, 1, logs.ResourceLogs().Len())
	sl := logs.ResourceLogs().At(0).ScopeLogs()
	require.Equal(t, 1, sl.Len())

	scopeAttrs := sl.At(0).Scope().Attributes()
	mappingMode, ok := scopeAttrs.Get("elastic.mapping.mode")
	require.True(t, ok)
	assert.Equal(t, "bodymap", mappingMode.Str())

	logRecords := sl.At(0).LogRecords()
	require.Equal(t, 2, logRecords.Len())

	for i := 0; i < logRecords.Len(); i++ {
		lr := logRecords.At(i)

		msgVal, ok := lr.Body().Map().Get("message")
		require.True(t, ok, "log record %d: body should have 'message' key", i)
		assert.NotEmpty(t, msgVal.Str(), "log record %d: message should not be empty", i)

		eventCreated, ok := lr.Body().Map().Get("event.created")
		require.True(t, ok, "log record %d: body should have 'event.created' key", i)
		assert.NotEmpty(t, eventCreated.Str(), "log record %d: event.created should not be empty", i)

		assert.NotZero(t, lr.Timestamp())
		assert.NotZero(t, lr.ObservedTimestamp())

		attrs := lr.Attributes()
		v, ok := attrs.Get("data_stream.type")
		require.True(t, ok)
		assert.Equal(t, "logs", v.Str())

		v, ok = attrs.Get("data_stream.dataset")
		require.True(t, ok)
		assert.Equal(t, "azure.events", v.Str())

		v, ok = attrs.Get("data_stream.namespace")
		require.True(t, ok)
		assert.Equal(t, "default", v.Str())
	}
}

func TestNewLogsDecoder_StreamingBatches(t *testing.T) {
	ext, err := newBeatsEncodingExtension(&Config{
		Format:      FormatJSON,
		Unwrap:      "$.records[*]",
		TargetField: "message",
		Routing:     RoutingConfig{Dataset: "test", Namespace: "default"},
	}, zap.NewNop())
	require.NoError(t, err)

	input, err := os.ReadFile(filepath.Join(testDataDir, "azure_diagnostic_settings.json"))
	require.NoError(t, err)

	decoder, err := ext.NewLogsDecoder(
		bytes.NewReader(input),
		encoding.WithFlushItems(1),
	)
	require.NoError(t, err)

	// First call: should return exactly 1 record (flushed after 1 item)
	logs1, err := decoder.DecodeLogs()
	require.NoError(t, err)
	assert.Equal(t, 1, logs1.LogRecordCount())

	// Second call: should return the remaining 1 record
	logs2, err := decoder.DecodeLogs()
	require.NoError(t, err)
	assert.Equal(t, 1, logs2.LogRecordCount())

	// Third call: should return io.EOF (no more records)
	_, err = decoder.DecodeLogs()
	assert.ErrorIs(t, err, io.EOF)

	// Offset should be positive (bytes consumed)
	assert.Greater(t, decoder.Offset(), int64(0))
}

func TestNewLogsDecoder_TextStreamingBatches(t *testing.T) {
	ext, err := newBeatsEncodingExtension(&Config{
		Format:      FormatText,
		TargetField: "message",
		Routing:     RoutingConfig{Dataset: "test", Namespace: "default"},
	}, zap.NewNop())
	require.NoError(t, err)

	input, err := os.ReadFile(filepath.Join(testDataDir, "aws_vpcflow.txt"))
	require.NoError(t, err)

	decoder, err := ext.NewLogsDecoder(
		bytes.NewReader(input),
		encoding.WithFlushItems(1),
	)
	require.NoError(t, err)

	var totalRecords int
	for {
		logs, err := decoder.DecodeLogs()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		totalRecords += logs.LogRecordCount()
	}

	assert.Equal(t, 3, totalRecords)
}

// stripEventCreated removes the "event.created" key from all log record
// body maps so golden file comparison is not affected by dynamic timestamps.
func stripEventCreated(logs plog.Logs) {
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rl := logs.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				lr.Body().Map().Remove("event.created")
			}
		}
	}
}
