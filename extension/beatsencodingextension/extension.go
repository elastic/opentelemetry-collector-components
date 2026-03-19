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
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/xstreamencoding"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

var (
	_ encoding.LogsUnmarshalerExtension = (*beatsEncodingExtension)(nil)
	_ encoding.LogsDecoderExtension     = (*beatsEncodingExtension)(nil)
)

type beatsEncodingExtension struct {
	config     *Config
	unwrapKeys []string
	logger     *zap.Logger
}

func newBeatsEncodingExtension(config *Config, logger *zap.Logger) (*beatsEncodingExtension, error) {
	ext := &beatsEncodingExtension{config: config, logger: logger}

	if config.Unwrap != "" {
		keys, err := parseUnwrapPath(config.Unwrap)
		if err != nil {
			return nil, fmt.Errorf("invalid unwrap expression %q: %w", config.Unwrap, err)
		}
		ext.unwrapKeys = keys
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
// For text it streams line-by-line; for json it streams array elements
// from the unwrap path using a tokenizing decoder.
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
// records using ScannerHelper for offset tracking and batch flushing.
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
		eventCreated := now.AsTime().UTC().Format(time.RFC3339Nano)

		for {
			line, flush, err := scanner.ScanString()

			if line != "" {
				e.appendLogRecord(sl, now, eventCreated, line)
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

// newJSONDecoder returns a streaming decoder for JSON input.
// Without unwrap, it reads the entire stream as a single record.
// With unwrap, it uses a streaming json.Decoder to navigate to the
// target array and yield elements one by one.
func (e *beatsEncodingExtension) newJSONDecoder(reader io.Reader, options ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	opts := encoding.NewDecoderOptions(options...)

	if opts.Offset > 0 {
		if _, err := io.CopyN(io.Discard, reader, opts.Offset); err != nil {
			return nil, fmt.Errorf("discarding offset %d: %w", opts.Offset, err)
		}
	}

	if len(e.unwrapKeys) == 0 {
		return e.newSingleRecordDecoder(reader, opts)
	}

	return e.newStreamingJSONDecoder(reader, opts, options...)
}

// newSingleRecordDecoder reads the entire stream and returns it as a
// single record. Used when no unwrap path is configured.
func (e *beatsEncodingExtension) newSingleRecordDecoder(reader io.Reader, opts encoding.DecoderOptions) (encoding.LogsDecoder, error) {
	buf, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading JSON input: %w", err)
	}

	offset := opts.Offset + int64(len(buf))
	trimmed := bytes.TrimSpace(buf)
	done := len(trimmed) == 0

	decodeF := func() (plog.Logs, error) {
		if done {
			return plog.NewLogs(), io.EOF
		}
		done = true

		logs := plog.NewLogs()
		sl := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
		sl.Scope().Attributes().PutStr("elastic.mapping.mode", "bodymap")
		now := pcommon.NewTimestampFromTime(time.Now())
		e.appendLogRecord(sl, now, now.AsTime().UTC().Format(time.RFC3339Nano), string(trimmed))
		return logs, nil
	}

	offsetF := func() int64 { return offset }
	return xstreamencoding.NewLogsDecoderAdapter(decodeF, offsetF), nil
}

// newStreamingJSONDecoder creates a decoder that uses json.Decoder to
// navigate the token stream to the target array (specified by unwrapKeys)
// and yields elements one by one.
func (e *beatsEncodingExtension) newStreamingJSONDecoder(reader io.Reader, opts encoding.DecoderOptions, options ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	dec := json.NewDecoder(reader)
	batchHelper := xstreamencoding.NewBatchHelper(options...)

	if err := navigateToArray(dec, e.unwrapKeys); err != nil {
		return nil, fmt.Errorf("navigating to unwrap path %q: %w", e.config.Unwrap, err)
	}

	decodeF := func() (plog.Logs, error) {
		if !dec.More() {
			return plog.NewLogs(), io.EOF
		}

		logs := plog.NewLogs()
		sl := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
		sl.Scope().Attributes().PutStr("elastic.mapping.mode", "bodymap")
		now := pcommon.NewTimestampFromTime(time.Now())
		eventCreated := now.AsTime().UTC().Format(time.RFC3339Nano)

		for dec.More() {
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return logs, fmt.Errorf("decoding array element: %w", err)
			}

			trimmed := bytes.TrimSpace(raw)
			if len(trimmed) == 0 {
				continue
			}

			e.appendLogRecord(sl, now, eventCreated, string(trimmed))
			batchHelper.IncrementItems(1)
			batchHelper.IncrementBytes(int64(len(raw)))

			if batchHelper.ShouldFlush() {
				batchHelper.Reset()
				return logs, nil
			}
		}

		if logs.LogRecordCount() == 0 {
			return logs, io.EOF
		}
		return logs, nil
	}

	offsetF := func() int64 {
		return opts.Offset + dec.InputOffset()
	}

	return xstreamencoding.NewLogsDecoderAdapter(decodeF, offsetF), nil
}

// navigateToArray walks the JSON token stream to find and enter the array
// at the path specified by keys. For example, keys ["data", "items"]
// navigates into {"data": {"items": [...]}} and positions the decoder
// right after the opening '[' of the target array.
func navigateToArray(dec *json.Decoder, keys []string) error {
	for i, key := range keys {
		// Expect opening '{' of the current object
		tok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("expected object at key %q: %w", key, err)
		}
		delim, ok := tok.(json.Delim)
		if !ok || delim != '{' {
			return fmt.Errorf("expected '{' before key %q, got %v", key, tok)
		}

		// Scan keys at this level to find the target key
		found := false
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return fmt.Errorf("reading key: %w", err)
			}
			name, ok := keyTok.(string)
			if !ok {
				return fmt.Errorf("expected string key, got %T", keyTok)
			}

			if name == key {
				found = true
				break
			}

			// Not our key — skip its value entirely
			if err := skipValue(dec); err != nil {
				return fmt.Errorf("skipping value for key %q: %w", name, err)
			}
		}

		if !found {
			return fmt.Errorf("key %q not found in object", key)
		}

		// For the last key, expect '[' (the target array)
		if i == len(keys)-1 {
			tok, err := dec.Token()
			if err != nil {
				return fmt.Errorf("expected array at key %q: %w", key, err)
			}
			delim, ok := tok.(json.Delim)
			if !ok || delim != '[' {
				return fmt.Errorf("expected '[' at key %q, got %v", key, tok)
			}
		}
		// For intermediate keys, the next iteration will consume '{'
	}

	return nil
}

// skipValue skips a single JSON value from the decoder. This handles
// nested objects, arrays, and scalar values.
func skipValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}

	delim, ok := tok.(json.Delim)
	if !ok {
		return nil // scalar value — already consumed
	}

	switch delim {
	case '{':
		for dec.More() {
			// skip key
			if _, err := dec.Token(); err != nil {
				return err
			}
			// skip value
			if err := skipValue(dec); err != nil {
				return err
			}
		}
		// consume closing '}'
		_, err = dec.Token()
		return err
	case '[':
		for dec.More() {
			if err := skipValue(dec); err != nil {
				return err
			}
		}
		// consume closing ']'
		_, err = dec.Token()
		return err
	default:
		return fmt.Errorf("unexpected delimiter %v", delim)
	}
}

func (e *beatsEncodingExtension) appendLogRecord(sl plog.ScopeLogs, ts pcommon.Timestamp, eventCreated string, record string) {
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(ts)
	lr.SetObservedTimestamp(ts)

	body := lr.Body().SetEmptyMap()
	body.PutStr("message", record)
	body.PutStr("event.created", eventCreated)

	attrs := lr.Attributes()
	attrs.PutStr("data_stream.type", "logs")
	attrs.PutStr("data_stream.dataset", e.config.DataStream.Dataset)
	attrs.PutStr("data_stream.namespace", e.config.DataStream.Namespace)
}
