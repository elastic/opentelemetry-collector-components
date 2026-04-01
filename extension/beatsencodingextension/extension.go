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
	"errors"
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

const (
	elasticMappingMode = "elastic.mapping.mode"
	bodymap            = "bodymap"
)

var (
	_ encoding.LogsUnmarshalerExtension = (*beatsEncodingExtension)(nil)
	_ encoding.LogsDecoderExtension     = (*beatsEncodingExtension)(nil)
)

type beatsEncodingExtension struct {
	config      *Config
	logger      *zap.Logger
	fieldWriter func(pcommon.Map)
}

func newBeatsEncodingExtension(config *Config, logger *zap.Logger) (*beatsEncodingExtension, error) {
	return &beatsEncodingExtension{
		config:      config,
		logger:      logger,
		fieldWriter: compileMapWriter(logger, config.Fields),
	}, nil
}

// compileMapWriter converts static config fields into a reusable map writer.
// This shifts type-switch and recursion work to startup so per-record writes
// in appendLogRecord stay cheaper on hot paths.
func compileMapWriter(logger *zap.Logger, fields map[string]any) func(pcommon.Map) {
	if len(fields) == 0 {
		return nil
	}

	writers := make([]func(pcommon.Map), 0, len(fields))
	for k, v := range fields {
		switch val := v.(type) {
		case string:
			key := k
			value := val
			writers = append(writers, func(m pcommon.Map) {
				m.PutStr(key, value)
			})
		case bool:
			key := k
			value := val
			writers = append(writers, func(m pcommon.Map) {
				m.PutBool(key, value)
			})
		case int:
			key := k
			value := int64(val)
			writers = append(writers, func(m pcommon.Map) {
				m.PutInt(key, value)
			})
		case int64:
			key := k
			value := val
			writers = append(writers, func(m pcommon.Map) {
				m.PutInt(key, value)
			})
		case float64:
			key := k
			value := val
			writers = append(writers, func(m pcommon.Map) {
				m.PutDouble(key, value)
			})
		case map[string]any:
			key := k
			nestedLen := len(val)
			nestedWriter := compileMapWriter(logger, val)
			writers = append(writers, func(m pcommon.Map) {
				nested := m.PutEmptyMap(key)
				nested.EnsureCapacity(nestedLen)
				if nestedWriter != nil {
					nestedWriter(nested)
				}
			})
		case []any:
			key := k
			sliceWriter := compileSliceWriter(logger, key, val)
			writers = append(writers, func(m pcommon.Map) {
				sl := m.PutEmptySlice(key)
				if sliceWriter != nil {
					sliceWriter(sl)
				}
			})
		default:
			key := k
			value := val
			writers = append(writers, func(pcommon.Map) {
				logger.Warn("unsupported field type, skipping", zap.String("key", key), zap.Any("value", value))
			})
		}
	}

	return func(m pcommon.Map) {
		for _, writer := range writers {
			writer(m)
		}
	}
}

// compileSliceWriter converts static slice values into reusable appenders.
// This avoids repeating per-element type checks for every log record and keeps
// field enrichment overhead lower in steady-state decoding.
func compileSliceWriter(logger *zap.Logger, key string, values []any) func(pcommon.Slice) {
	if len(values) == 0 {
		return nil
	}

	appenders := make([]func(pcommon.Slice), 0, len(values))
	for _, item := range values {
		switch elem := item.(type) {
		case string:
			value := elem
			appenders = append(appenders, func(sl pcommon.Slice) {
				sl.AppendEmpty().SetStr(value)
			})
		case bool:
			value := elem
			appenders = append(appenders, func(sl pcommon.Slice) {
				sl.AppendEmpty().SetBool(value)
			})
		case int:
			value := int64(elem)
			appenders = append(appenders, func(sl pcommon.Slice) {
				sl.AppendEmpty().SetInt(value)
			})
		case int64:
			value := elem
			appenders = append(appenders, func(sl pcommon.Slice) {
				sl.AppendEmpty().SetInt(value)
			})
		case float64:
			value := elem
			appenders = append(appenders, func(sl pcommon.Slice) {
				sl.AppendEmpty().SetDouble(value)
			})
		case map[string]any:
			nestedLen := len(elem)
			nestedWriter := compileMapWriter(logger, elem)
			appenders = append(appenders, func(sl pcommon.Slice) {
				nested := sl.AppendEmpty().SetEmptyMap()
				nested.EnsureCapacity(nestedLen)
				if nestedWriter != nil {
					nestedWriter(nested)
				}
			})
		default:
			value := elem
			appenders = append(appenders, func(pcommon.Slice) {
				logger.Warn("unsupported field type in slice, skipping", zap.String("key", key), zap.Any("value", value))
			})
		}
	}

	return func(sl pcommon.Slice) {
		sl.EnsureCapacity(len(appenders))
		for _, appender := range appenders {
			appender(sl)
		}
	}
}

func (*beatsEncodingExtension) Start(context.Context, component.Host) error {
	return nil
}

func (*beatsEncodingExtension) Shutdown(context.Context) error {
	return nil
}

// UnmarshalLogs converts raw bytes into OTel log records by delegating to
// the streaming decoder with flushing disabled, so all records are returned
// in a single batch.
func (e *beatsEncodingExtension) UnmarshalLogs(buf []byte) (plog.Logs, error) {
	switch e.config.Format {
	case FormatText:
		return e.unmarshalText(buf)
	case FormatJSON:
		return e.unmarshalJSON(buf)
	default:
		return plog.NewLogs(), fmt.Errorf("unsupported format: %q", e.config.Format)
	}
}

func (e *beatsEncodingExtension) unmarshalText(buf []byte) (plog.Logs, error) {
	scanner, err := xstreamencoding.NewScannerHelper(bytes.NewReader(buf))
	if err != nil {
		return plog.NewLogs(), err
	}

	logs := plog.NewLogs()
	sl := newScopeLogs(logs)
	now := pcommon.NewTimestampFromTime(time.Now())
	eventCreated := now.AsTime().UTC().Format(time.RFC3339Nano)

	for {
		line, _, err := scanner.ScanString()
		if line != "" {
			e.appendLogRecord(sl, now, eventCreated, line)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return logs, err
		}
	}

	return logs, nil
}

func (e *beatsEncodingExtension) unmarshalJSON(buf []byte) (plog.Logs, error) {
	if len(e.config.Unwrap) == 0 {
		trimmed := bytes.TrimSpace(buf)
		if len(trimmed) == 0 {
			return plog.NewLogs(), nil
		}

		logs := plog.NewLogs()
		sl := newScopeLogs(logs)
		now := pcommon.NewTimestampFromTime(time.Now())
		e.appendLogRecord(sl, now, now.AsTime().UTC().Format(time.RFC3339Nano), string(trimmed))
		return logs, nil
	}

	dec := json.NewDecoder(bytes.NewReader(buf))
	if err := navigateToArray(dec, e.config.Unwrap); err != nil {
		return plog.NewLogs(), fmt.Errorf("navigating to unwrap %v: %w", e.config.Unwrap, err)
	}

	logs := plog.NewLogs()
	sl := newScopeLogs(logs)
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
	}

	if logs.LogRecordCount() == 0 {
		return plog.NewLogs(), nil
	}
	return logs, nil
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
		sl := newScopeLogs(logs)
		now := pcommon.NewTimestampFromTime(time.Now())
		eventCreated := now.AsTime().UTC().Format(time.RFC3339Nano)

		for {
			line, flush, err := scanner.ScanString()

			if line != "" {
				e.appendLogRecord(sl, now, eventCreated, line)
			}

			if errors.Is(err, io.EOF) {
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

	if len(e.config.Unwrap) == 0 {
		return e.newSingleRecordDecoder(reader, opts)
	}

	return e.newStreamingJSONDecoder(reader, opts, options...)
}

// newSingleRecordDecoder reads the entire stream and returns it as a
// single record. Used when no unwrap path is configured.
// Offset semantics: 0 = not yet consumed, 1 = already consumed.
func (e *beatsEncodingExtension) newSingleRecordDecoder(reader io.Reader, opts encoding.DecoderOptions) (encoding.LogsDecoder, error) {
	// If offset >= 1, the single record was already consumed.
	if opts.Offset >= 1 {
		decodeF := func() (plog.Logs, error) {
			return plog.NewLogs(), io.EOF
		}
		offsetF := func() int64 { return opts.Offset }
		return xstreamencoding.NewLogsDecoderAdapter(decodeF, offsetF), nil
	}

	buf, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading JSON input: %w", err)
	}

	trimmed := bytes.TrimSpace(buf)
	done := len(trimmed) == 0

	decodeF := func() (plog.Logs, error) {
		if done {
			return plog.NewLogs(), io.EOF
		}
		done = true

		logs := plog.NewLogs()
		sl := newScopeLogs(logs)
		now := pcommon.NewTimestampFromTime(time.Now())
		e.appendLogRecord(sl, now, now.AsTime().UTC().Format(time.RFC3339Nano), string(trimmed))
		return logs, nil
	}

	offsetF := func() int64 {
		if done {
			return 1
		}
		return 0
	}
	return xstreamencoding.NewLogsDecoderAdapter(decodeF, offsetF), nil
}

// newStreamingJSONDecoder creates a decoder that uses json.Decoder to
// navigate the token stream to the target array (specified by Unwrap)
// and yields elements one by one.
func (e *beatsEncodingExtension) newStreamingJSONDecoder(reader io.Reader, opts encoding.DecoderOptions, options ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	dec := json.NewDecoder(reader)
	batchHelper := xstreamencoding.NewBatchHelper(options...)

	if err := navigateToArray(dec, e.config.Unwrap); err != nil {
		return nil, fmt.Errorf("navigating to unwrap %v: %w", e.config.Unwrap, err)
	}

	// Skip records that were already processed in a previous decoder session.
	recordCount := int64(0)
	for recordCount < opts.Offset {
		if !dec.More() {
			return nil, fmt.Errorf("EOF reached before skipping %d records (only found %d)", opts.Offset, recordCount)
		}
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return nil, fmt.Errorf("skipping record %d: %w", recordCount, err)
		}
		recordCount++
	}

	decodeF := func() (plog.Logs, error) {
		if !dec.More() {
			return plog.NewLogs(), io.EOF
		}

		logs := plog.NewLogs()
		sl := newScopeLogs(logs)
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
			recordCount++
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
		return recordCount
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
	var raw json.RawMessage
	return dec.Decode(&raw)
}

// writeFields recursively writes a map[string]any into a pcommon.Map.
// Strings, bools, ints, and floats are written as scalar values.
// Nested maps are written as nested pcommon.Map entries.
// Slices are written as pcommon.Slice entries.
func writeFields(logger *zap.Logger, m pcommon.Map, fields map[string]any) {
	for k, v := range fields {
		switch val := v.(type) {
		case string:
			m.PutStr(k, val)
		case bool:
			m.PutBool(k, val)
		case int:
			m.PutInt(k, int64(val))
		case int64:
			m.PutInt(k, val)
		case float64:
			m.PutDouble(k, val)
		case map[string]any:
			nested := m.PutEmptyMap(k)
			nested.EnsureCapacity(len(val))
			writeFields(logger, nested, val)
		case []any:
			sl := m.PutEmptySlice(k)
			sl.EnsureCapacity(len(val))
			for _, item := range val {
				switch elem := item.(type) {
				case string:
					sl.AppendEmpty().SetStr(elem)
				case bool:
					sl.AppendEmpty().SetBool(elem)
				case int:
					sl.AppendEmpty().SetInt(int64(elem))
				case int64:
					sl.AppendEmpty().SetInt(elem)
				case float64:
					sl.AppendEmpty().SetDouble(elem)
				case map[string]any:
					writeFields(logger, sl.AppendEmpty().SetEmptyMap(), elem)
				default:
					logger.Warn("unsupported field type in slice, skipping", zap.String("key", k), zap.Any("value", elem))
				}
			}
		default:
			logger.Warn("unsupported field type, skipping", zap.String("key", k), zap.Any("value", val))
		}
	}
}

// newScopeLogs creates a new ResourceLogs → ScopeLogs inside logs and sets
// the elastic.mapping.mode scope attribute to bodymap.
func newScopeLogs(logs plog.Logs) plog.ScopeLogs {
	sl := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	sl.Scope().Attributes().PutStr(elasticMappingMode, bodymap)
	return sl
}

func (e *beatsEncodingExtension) appendLogRecord(sl plog.ScopeLogs, ts pcommon.Timestamp, eventCreated, record string) {
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(ts)
	lr.SetObservedTimestamp(ts)

	body := lr.Body().SetEmptyMap()
	body.EnsureCapacity(7) // we know we'll be adding at least 7 entries to the body
	body.PutStr("message", record)
	body.PutStr("event.created", eventCreated)

	// The data_stream.* should be also set on the body as some
	// integrations expect them.
	body.PutStr("data_stream.type", "logs")
	body.PutStr("data_stream.dataset", e.config.DataStream.Dataset)
	body.PutStr("data_stream.namespace", e.config.DataStream.Namespace)
	body.PutStr("event.dataset", e.config.DataStream.Dataset)

	if e.config.InputType != "" {
		body.PutStr("input.type", e.config.InputType)
	}

	if len(e.config.Tags) > 0 {
		tags := body.PutEmptySlice("tags")
		tags.EnsureCapacity(len(e.config.Tags))
		for _, tag := range e.config.Tags {
			tags.AppendEmpty().SetStr(tag)
		}
	}

	if e.fieldWriter != nil {
		e.fieldWriter(body)
	}

	// We need to set these attributes on the record itself for the
	// Elasticsearch exporter to route the record to the correct
	// data stream.
	attrs := lr.Attributes()
	attrs.EnsureCapacity(3)
	attrs.PutStr("data_stream.type", "logs")
	attrs.PutStr("data_stream.dataset", e.config.DataStream.Dataset)
	attrs.PutStr("data_stream.namespace", e.config.DataStream.Namespace)
}
