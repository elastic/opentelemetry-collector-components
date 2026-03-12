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

package beatsencodingextension // import "github.com/elastic/opentelemetry-collector-components/extension/beatsencodingextension"

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/goccy/go-json"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/xstreamencoding"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type beatsEncodingExtension struct {
	config     *Config
	unwrapPath *json.Path
	logger     *zap.Logger
}

func newBeatsEncodingExtension(config *Config, logger *zap.Logger) (*beatsEncodingExtension, error) {
	ext := &beatsEncodingExtension{config: config, logger: logger}

	if config.Unwrap != "" {
		path, err := json.CreatePath(config.Unwrap)
		if err != nil {
			return nil, fmt.Errorf("invalid JSONPath unwrap expression %q: %w", config.Unwrap, err)
		}
		ext.unwrapPath = path
	}

	return ext, nil
}

func (e *beatsEncodingExtension) Start(context.Context, component.Host) error {
	return nil
}

func (e *beatsEncodingExtension) Shutdown(context.Context) error {
	return nil
}

// UnmarshalLogs converts raw bytes into OTel log records by delegating to
// the streaming decoder with flushing disabled, so all records are returned
// in a single batch.
func (e *beatsEncodingExtension) UnmarshalLogs(buf []byte) (plog.Logs, error) {
	decoder, err := e.NewLogsDecoder(
		bytes.NewReader(buf),
		encoding.WithFlushBytes(0),
		encoding.WithFlushItems(0),
	)
	if err != nil {
		return plog.Logs{}, err
	}
	logs, err := decoder.DecodeLogs()
	if err == io.EOF {
		return plog.NewLogs(), nil
	}
	return logs, err
}

// NewLogsDecoder creates a streaming decoder for the configured format.
// For text/ndjson it streams line-by-line; for json it reads the full
// document (required for JSONPath) and batches extracted records.
func (e *beatsEncodingExtension) NewLogsDecoder(reader io.Reader, options ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	switch e.config.Format {
	case FormatText:
		return e.newLineDecoder(reader, options...)
	case FormatJSON:
		return e.newJSONDecoder(reader, options...)
	default:
		return nil, fmt.Errorf("unsupported format: %q", e.config.Format)
	}
}

// newLineDecoder returns a streaming decoder that reads newline-delimited
// records (text or ndjson) using ScannerHelper for offset tracking and
// batch flushing.
func (e *beatsEncodingExtension) newLineDecoder(reader io.Reader, options ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	scanner, err := xstreamencoding.NewScannerHelper(reader, options...)
	if err != nil {
		return nil, err
	}

	decodeF := func() (plog.Logs, error) {
		logs := plog.NewLogs()
		sl := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
		sl.Scope().Attributes().PutStr("elastic.mapping.mode", "bodymap")
		now := pcommon.NewTimestampFromTime(time.Now())

		for {
			line, flush, err := scanner.ScanString()

			if line != "" {
				e.appendLogRecord(sl, now, line)
			}

			if err == io.EOF {
				break
			}
			if err != nil {
				return logs, err
			}

			if flush {
				return logs, nil
			}
		}

		if logs.LogRecordCount() == 0 {
			return logs, io.EOF
		}
		return logs, nil
	}

	return xstreamencoding.NewLogsDecoderAdapter(decodeF, scanner.Offset), nil
}

// newJSONDecoder returns a decoder that reads the entire JSON document
// from the stream (necessary for JSONPath extraction), then batches the
// extracted records according to flush options.
func (e *beatsEncodingExtension) newJSONDecoder(reader io.Reader, options ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	opts := encoding.NewDecoderOptions(options...)

	if opts.Offset > 0 {
		if _, err := io.CopyN(io.Discard, reader, opts.Offset); err != nil {
			return nil, fmt.Errorf("discarding offset %d: %w", opts.Offset, err)
		}
	}

	buf, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading JSON input: %w", err)
	}

	offset := opts.Offset + int64(len(buf))

	records, err := e.extractJSONRecords(buf)
	if err != nil {
		return nil, err
	}

	batchHelper := xstreamencoding.NewBatchHelper(options...)
	idx := 0

	decodeF := func() (plog.Logs, error) {
		if idx >= len(records) {
			return plog.Logs{}, io.EOF
		}

		logs := plog.NewLogs()
		sl := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
		sl.Scope().Attributes().PutStr("elastic.mapping.mode", "bodymap")
		now := pcommon.NewTimestampFromTime(time.Now())

		for idx < len(records) {
			e.appendLogRecord(sl, now, records[idx])
			batchHelper.IncrementItems(1)
			batchHelper.IncrementBytes(int64(len(records[idx])))
			idx++

			if batchHelper.ShouldFlush() {
				batchHelper.Reset()
				return logs, nil
			}
		}

		return logs, nil
	}

	offsetF := func() int64 {
		return offset
	}

	return xstreamencoding.NewLogsDecoderAdapter(decodeF, offsetF), nil
}

func (e *beatsEncodingExtension) appendLogRecord(sl plog.ScopeLogs, ts pcommon.Timestamp, record string) {
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(ts)
	lr.SetObservedTimestamp(ts)

	lr.Body().SetEmptyMap().PutStr(e.config.TargetField, record)

	attrs := lr.Attributes()
	attrs.PutStr("data_stream.type", "logs")
	attrs.PutStr("data_stream.dataset", e.config.Routing.Dataset)
	attrs.PutStr("data_stream.namespace", e.config.Routing.Namespace)
}

// extractJSONRecords extracts records from JSON input. When an unwrap
// JSONPath expression is configured, it uses the compiled path to extract
// matching elements. Otherwise, the entire input is treated as a single record.
func (e *beatsEncodingExtension) extractJSONRecords(buf []byte) ([]string, error) {
	buf = bytes.TrimSpace(buf)
	if len(buf) == 0 {
		return nil, nil
	}

	if e.unwrapPath == nil {
		return []string{string(buf)}, nil
	}

	return e.unwrapJSON(buf)
}

// unwrapJSON extracts individual records from a JSON structure using
// the compiled JSONPath expression.
func (e *beatsEncodingExtension) unwrapJSON(buf []byte) ([]string, error) {
	extracted, err := e.unwrapPath.Extract(buf)
	if err != nil {
		return nil, fmt.Errorf("unwrapping JSON with path %q: %w", e.config.Unwrap, err)
	}
	if len(extracted) == 0 {
		return []string{string(buf)}, nil
	}

	records := make([]string, 0, len(extracted))
	for _, r := range extracted {
		trimmed := bytes.TrimSpace(r)
		if len(trimmed) > 0 {
			records = append(records, string(trimmed))
		}
	}

	if len(records) == 0 {
		return []string{string(buf)}, nil
	}

	return records, nil
}
