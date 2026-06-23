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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

// space-separated CSV mirroring how Netskope Log Streaming delivers to S3:
// a header row, space separator, quoted values that contain spaces, and "-"
// for empty fields.
const netskopeCSV = `date time x-cs-app cs-username
2024-08-05 16:24:20 "Microsoft Bing" john.doe@gmail.com
2024-08-05 16:25:00 - jane.doe@gmail.com
`

// allMessages returns the "message" body field of every log record, in order.
func allMessages(t *testing.T, logs plog.Logs) []string {
	t.Helper()
	var out []string
	rls := logs.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		sls := rls.At(i).ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				body := lrs.At(k).Body().Map()
				msg, ok := body.Get("message")
				require.True(t, ok, "log record body has no message field")
				out = append(out, msg.Str())
			}
		}
	}
	return out
}

func TestCSVDecoder(t *testing.T) {
	ext := newTestExtension(t, &Config{
		Format:     FormatCSV,
		CSV:        CSVConfig{Comma: " "},
		DataStream: DataStreamConfig{Dataset: "netskope.transaction", Namespace: "default"},
		InputType:  "aws-s3",
		Tags:       []string{"forwarded", "netskope-transaction"},
	})

	logs, err := ext.UnmarshalLogs([]byte(netskopeCSV))
	require.NoError(t, err)
	require.Equal(t, 2, logs.LogRecordCount())

	messages := allMessages(t, logs)
	require.Len(t, messages, 2)

	// The header keys the JSON object, quoted values are unquoted, and the
	// "-" placeholder is preserved verbatim (the ingest pipeline drops it).
	want := []map[string]string{
		{"date": "2024-08-05", "time": "16:24:20", "x-cs-app": "Microsoft Bing", "cs-username": "john.doe@gmail.com"},
		{"date": "2024-08-05", "time": "16:25:00", "x-cs-app": "-", "cs-username": "jane.doe@gmail.com"},
	}
	for i, msg := range messages {
		var got map[string]string
		require.NoError(t, json.Unmarshal([]byte(msg), &got), "message is not valid JSON: %s", msg)
		assert.Equal(t, want[i], got)
	}

	// Routing attributes must be present so the document lands in the
	// configured data stream and the integration pipeline runs.
	body := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Map()
	ds, ok := body.Get("data_stream.dataset")
	require.True(t, ok)
	assert.Equal(t, "netskope.transaction", ds.Str())

	// Every record must carry a baseline @timestamp (like a Beats doc) so it
	// indexes into a data stream even when the integration pipeline does not
	// derive one.
	_, ok = body.Get("@timestamp")
	assert.True(t, ok, "body is missing @timestamp")
}

// A value containing the separator and a double quote must be encoded as
// valid, escaped JSON.
func TestCSVDecoder_QuotingAndEscaping(t *testing.T) {
	const in = `a b
"x y" "he said ""hi"""
`
	ext := newTestExtension(t, &Config{
		Format:     FormatCSV,
		CSV:        CSVConfig{Comma: " "},
		DataStream: DataStreamConfig{Dataset: "test.ds", Namespace: "default"},
	})

	logs, err := ext.UnmarshalLogs([]byte(in))
	require.NoError(t, err)
	require.Equal(t, 1, logs.LogRecordCount())

	var got map[string]string
	require.NoError(t, json.Unmarshal([]byte(allMessages(t, logs)[0]), &got))
	assert.Equal(t, map[string]string{"a": "x y", "b": `he said "hi"`}, got)
}

// FieldNames overrides the header, so the first record is treated as data.
func TestCSVDecoder_FieldNames(t *testing.T) {
	const in = "1 2\n3 4\n"
	ext := newTestExtension(t, &Config{
		Format:     FormatCSV,
		CSV:        CSVConfig{Comma: " ", FieldNames: []string{"a", "b"}},
		DataStream: DataStreamConfig{Dataset: "test.ds", Namespace: "default"},
	})

	logs, err := ext.UnmarshalLogs([]byte(in))
	require.NoError(t, err)
	require.Equal(t, 2, logs.LogRecordCount())

	messages := allMessages(t, logs)
	var first map[string]string
	require.NoError(t, json.Unmarshal([]byte(messages[0]), &first))
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, first)
}

func TestCSVDecoder_EmptyInput(t *testing.T) {
	for _, in := range [][]byte{[]byte(""), []byte("   ")} {
		ext := newTestExtension(t, &Config{
			Format:     FormatCSV,
			CSV:        CSVConfig{Comma: " "},
			DataStream: DataStreamConfig{Dataset: "test.ds", Namespace: "default"},
		})
		logs, err := ext.UnmarshalLogs(in)
		require.NoError(t, err)
		assert.Equal(t, 0, logs.LogRecordCount())
	}
}

// Header only, no data rows.
func TestCSVDecoder_HeaderOnly(t *testing.T) {
	ext := newTestExtension(t, &Config{
		Format:     FormatCSV,
		CSV:        CSVConfig{Comma: " "},
		DataStream: DataStreamConfig{Dataset: "test.ds", Namespace: "default"},
	})
	logs, err := ext.UnmarshalLogs([]byte("a b c\n"))
	require.NoError(t, err)
	assert.Equal(t, 0, logs.LogRecordCount())
}

func TestConfigValidate_CSV(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name: "valid csv",
			config: Config{
				Format:     FormatCSV,
				CSV:        CSVConfig{Comma: " "},
				DataStream: DataStreamConfig{Dataset: "netskope.transaction", Namespace: "default"},
			},
		},
		{
			name: "csv default comma",
			config: Config{
				Format:     FormatCSV,
				DataStream: DataStreamConfig{Dataset: "test.ds", Namespace: "default"},
			},
		},
		{
			name: "comma too long",
			config: Config{
				Format:     FormatCSV,
				CSV:        CSVConfig{Comma: ", "},
				DataStream: DataStreamConfig{Dataset: "test.ds", Namespace: "default"},
			},
			wantErr: "csv.comma must be a single character",
		},
		{
			name: "csv options with wrong format",
			config: Config{
				Format:     FormatText,
				CSV:        CSVConfig{Comma: " "},
				DataStream: DataStreamConfig{Dataset: "test.ds", Namespace: "default"},
			},
			wantErr: `csv options are only supported when format is "csv"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
