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
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"slices"
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

// MappedField pairs a FieldMapping with its extracted value.
type MappedField struct {
	Mapping FieldMapping
	Value   any
}

type beatsEncodingExtension struct {
	config *Config
	logger *zap.Logger
}

func newBeatsEncodingExtension(config *Config, logger *zap.Logger) (*beatsEncodingExtension, error) {
	return &beatsEncodingExtension{config: config, logger: logger}, nil
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
		return plog.NewLogs(), err
	}
	logs, err := decoder.DecodeLogs()
	if errors.Is(err, io.EOF) {
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
	case FormatCSV:
		return e.newCSVDecoder(reader, options...)
	default:
		return nil, fmt.Errorf("unsupported format: %q", e.config.Format)
	}
}

// newCSVDecoder returns a streaming decoder that reads CSV records. The first
// record is the header (unless CSV.FieldsNames is set); each subsequent record
// becomes a log record whose "message" is a JSON object keyed by the header.
// This mirrors the Beats aws-s3 input's decoding.codec.csv behaviour so the
// documents match what an Elastic Agent would produce.
func (e *beatsEncodingExtension) newCSVDecoder(reader io.Reader, options ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	opts := encoding.NewDecoderOptions(options...)
	batchHelper := xstreamencoding.NewBatchHelper(options...)

	r := csv.NewReader(reader)
	r.ReuseRecord = true
	r.LazyQuotes = e.config.CSV.LazyQuotes
	r.TrimLeadingSpace = e.config.CSV.TrimLeadingSpace
	if e.config.CSV.Comma != "" {
		r.Comma = []rune(e.config.CSV.Comma)[0]
	}
	if e.config.CSV.Comment != "" {
		r.Comment = []rune(e.config.CSV.Comment)[0]
	}

	// Establish the header. With FieldsNames set, lock the field count to it;
	// otherwise the first non-comment record is the header and csv.Reader
	// enforces that subsequent records have the same field count.
	var header []string
	if len(e.config.CSV.FieldsNames) != 0 {
		header = e.config.CSV.FieldsNames
		r.FieldsPerRecord = len(header)
	} else {
		h, err := r.Read()
		if errors.Is(err, io.EOF) {
			// Empty input: nothing to decode.
			return newEOFDecoder(opts.Offset), nil
		}
		if err != nil {
			return nil, fmt.Errorf("reading CSV header: %w", err)
		}
		header = slices.Clone(h)
	}

	// Skip records already processed in a previous decoder session.
	recordCount := int64(0)
	for recordCount < opts.Offset {
		if _, err := r.Read(); err != nil {
			return nil, fmt.Errorf("skipping CSV record %d: %w", recordCount, err)
		}
		recordCount++
	}

	decodeF := func() (plog.Logs, error) {
		logs := plog.NewLogs()
		sl := newScopeLogs(logs)
		now := pcommon.NewTimestampFromTime(time.Now())
		eventCreated := now.AsTime().UTC().Format(time.RFC3339Nano)

		for {
			record, err := r.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return logs, fmt.Errorf("reading CSV record: %w", err)
			}

			msg, err := csvRecordToJSON(header, record)
			if err != nil {
				return plog.NewLogs(), err
			}

			data := []MappedField{{Mapping: FieldMapping{Type: FieldTypeString, Destination: "message"}, Value: msg}}
			if err := e.appendLogRecord(sl, now, eventCreated, data); err != nil {
				return plog.NewLogs(), err
			}

			recordCount++
			batchHelper.IncrementItems(1)
			batchHelper.IncrementBytes(int64(len(msg)))

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

	offsetF := func() int64 { return recordCount }
	return xstreamencoding.NewLogsDecoderAdapter(decodeF, offsetF), nil
}

// newEOFDecoder returns a decoder that yields no records (io.EOF). Used when
// the CSV input is empty so callers get empty logs rather than an error.
func newEOFDecoder(offset int64) encoding.LogsDecoder {
	decodeF := func() (plog.Logs, error) { return plog.NewLogs(), io.EOF }
	offsetF := func() int64 { return offset }
	return xstreamencoding.NewLogsDecoderAdapter(decodeF, offsetF)
}

// csvRecordToJSON encodes a CSV record as a JSON object keyed by header.
// Values are kept as strings (matching the Beats csv codec), and JSON
// encoding handles escaping.
//
// csv.Reader enforces that every record has the same field count as the header
// (FieldsPerRecord), so a row with too few or too many fields fails the read
// with ErrFieldCount before reaching here — matching the Beats codec's strict
// behaviour. The length guard below is therefore defensive only (it never pads
// in practice) and exists solely to avoid an index panic if that invariant
// ever changes.
func csvRecordToJSON(header, record []string) (string, error) {
	m := make(map[string]string, len(header))
	for i, name := range header {
		if i < len(record) {
			m[name] = record[i]
		} else {
			m[name] = ""
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("encoding CSV record to JSON: %w", err)
	}
	return string(b), nil
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
				// Simply add to message as a string
				data := []MappedField{{Mapping: FieldMapping{Type: FieldTypeString, Destination: "message"}, Value: line}}
				if err := e.appendLogRecord(sl, now, eventCreated, data); err != nil {
					return plog.NewLogs(), err
				}
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
			writeFields(logger, m.PutEmptyMap(k), val)
		case []any:
			sl := m.PutEmptySlice(k)
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

func (e *beatsEncodingExtension) appendLogRecord(sl plog.ScopeLogs, ts pcommon.Timestamp, eventCreated string, data []MappedField) error {
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(ts)
	lr.SetObservedTimestamp(ts)

	body := lr.Body().SetEmptyMap()
	body.EnsureCapacity(7)

	for _, d := range data {
		switch d.Mapping.Type {
		case FieldTypeString:
			s, ok := d.Value.(string)
			if !ok {
				return fmt.Errorf("expected string field type, got %T", d.Value)
			}
			body.PutStr(d.Mapping.Destination, s)
		case FieldTypeInteger:
			num, ok := d.Value.(json.Number)
			if !ok {
				return fmt.Errorf("field %q: expected numeric (json.Number), got %T", d.Mapping.Destination, d.Value)
			}
			base, err := num.Int64()
			if err != nil {
				return fmt.Errorf("field %q: expected integer, got %q: %w", d.Mapping.Destination, num.String(), err)
			}

			if d.Mapping.Multiplier != 0 {
				base *= d.Mapping.Multiplier
			}
			body.PutInt(d.Mapping.Destination, base)
		}
	}

	// Store event and data_stream content as maps

	eventMap := body.PutEmptyMap("event")
	eventMap.PutStr("created", eventCreated)
	eventMap.PutStr("dataset", e.config.DataStream.Dataset)

	// Always set @timestamp on the body, like a Beats document. Some
	// integration ingest pipelines only derive @timestamp on certain code
	// paths (e.g. Netskope sets it inside per-record-type sub-pipelines) and
	// otherwise rely on the input having stamped it. Without a baseline here,
	// such documents reach a data stream with no @timestamp and are rejected.
	// When the pipeline does derive @timestamp from the event it overrides
	// this value.
	body.PutStr("@timestamp", eventCreated)

	// The data_stream.* should be also set on the body as some
	// integrations expect them.
	dStreamMap := body.PutEmptyMap("data_stream")
	dStreamMap.PutStr("type", "logs")
	dStreamMap.PutStr("dataset", e.config.DataStream.Dataset)
	dStreamMap.PutStr("namespace", e.config.DataStream.Namespace)

	if e.config.InputType != "" {
		inputMap := body.PutEmptyMap("input")
		inputMap.PutStr("type", e.config.InputType)
	}

	if len(e.config.Tags) > 0 {
		tags := body.PutEmptySlice("tags")
		tags.EnsureCapacity(len(e.config.Tags))
		for _, tag := range e.config.Tags {
			tags.AppendEmpty().SetStr(tag)
		}
	}

	writeFields(e.logger, body, e.config.Fields)

	// We need to set these attributes on the record itself for the
	// Elasticsearch exporter to route the record to the correct
	// data stream.
	attrs := lr.Attributes()
	attrs.PutStr("data_stream.type", "logs")
	attrs.PutStr("data_stream.dataset", e.config.DataStream.Dataset)
	attrs.PutStr("data_stream.namespace", e.config.DataStream.Namespace)

	return nil
}
