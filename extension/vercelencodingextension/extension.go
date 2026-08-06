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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/goccy/go-json"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/internal/vercel"
)

type signal string

const (
	signalLogs    signal = "logs"
	signalMetrics signal = "metrics"
)

type schema string

const (
	schemaLogs          schema = ""
	schemaSpeedInsights schema = "vercel.speed_insights.v1"
	schemaWebAnalytics  schema = "vercel.analytics.v2"
	schemaAuditLogs     schema = "vercel.audit_log.v1"
)

var errUnsupportedVercelSchema = errors.New("unsupported vercel schema")

type schemaRoute struct {
	signal signal
	format format
}

var schemaRoutes = map[schema]schemaRoute{
	schemaLogs:          {signal: signalLogs, format: formatLogs},
	schemaAuditLogs:     {signal: signalLogs, format: formatAuditLogs},
	schemaSpeedInsights: {signal: signalMetrics, format: formatSpeedInsights},
	schemaWebAnalytics:  {signal: signalLogs, format: formatWebAnalytics},
}

type format string

const (
	formatLogs          format = "vercel.logs"
	formatSpeedInsights format = "vercel.speedinsights.v1"
	formatWebAnalytics  format = "vercel.analytics.v2"
	formatAuditLogs     format = "vercel.auditlog.v1"

	formatIdentificationTag = "encoding.format"
)

type vercelEncodingExtension struct{}

var _ vercel.EncodingExtension = (*vercelEncodingExtension)(nil)

func (*vercelEncodingExtension) Start(context.Context, component.Host) error {
	return nil
}

func (*vercelEncodingExtension) Shutdown(context.Context) error {
	return nil
}

func (*vercelEncodingExtension) NewVercelParser(reader io.Reader, opts ...encoding.DecoderOption) (vercel.PayloadParser, error) {
	parser := &vercelPayloadParser{
		decoder: newVercelStreamDecoder(reader, encoding.NewDecoderOptions(opts...)),
	}
	payload, err := parser.readPayload()
	if err != nil {
		return nil, err
	}
	parser.next = &payload
	return parser, nil
}

type vercelPayloadParser struct {
	decoder *vercelStreamDecoder
	next    *vercel.Payload

	// Routing is resolved once because Vercel drains use one schema per request.
	routingResolved bool
	batchSignal     signal
	batchFormat     format
	batchSchema     schema
}

func (p *vercelPayloadParser) Next() (vercel.Payload, error) {
	if p.next != nil {
		payload := *p.next
		p.next = nil
		return payload, nil
	}
	return p.readPayload()
}

// readPayload assembles one payload up to the configured flush threshold.
func (p *vercelPayloadParser) readPayload() (vercel.Payload, error) {
	startOffset := p.decoder.inputOffset()

	record, err := p.nextRecord()
	if err != nil {
		return vercel.Payload{}, err
	}

	if !p.routingResolved {
		batchSchema, schemaErr := record.schema()
		if schemaErr != nil {
			return vercel.Payload{}, schemaErr
		}
		route := schemaRoutes[batchSchema]
		p.batchSignal = route.signal
		p.batchFormat = route.format
		p.batchSchema = batchSchema
		p.routingResolved = true
	}

	var metrics pmetric.Metrics
	var logs plog.Logs
	var appendRecord func(recordSource) error
	if p.batchSignal == signalMetrics {
		metrics, appendRecord = newMetricsDecoder()
	} else {
		logs, appendRecord = newLogsDecoder(p.batchSchema)
	}

	// The first record's raw bytes were read to resolve routing; decode them
	// from the buffer. Later records decode straight from the stream.
	if err := appendRecord(rawRecordSource(record)); err != nil {
		return vercel.Payload{}, err
	}

	records := int64(1)
	for !p.decoder.flushReached(records, startOffset) {
		if err := appendRecord(p.decoder); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return vercel.Payload{}, err
		}
		records++
	}

	if p.batchSignal == signalMetrics {
		return vercel.Payload{Signal: vercel.SignalMetrics, Metrics: metrics}, nil
	}
	return vercel.Payload{Signal: vercel.SignalLogs, Logs: logs}, nil
}

func (p *vercelPayloadParser) nextRecord() (rawRecord, error) {
	return p.decoder.readRecord()
}

type vercelStreamDecoder struct {
	reader      *bufio.Reader
	decoder     *json.Decoder
	initialized bool
	inArray     bool
	arrayDone   bool
	flushItems  int64
	flushBytes  int64
}

func newVercelStreamDecoder(reader io.Reader, options encoding.DecoderOptions) *vercelStreamDecoder {
	buffered := bufio.NewReader(reader)
	return &vercelStreamDecoder{
		reader:     buffered,
		decoder:    newVercelJSONDecoder(buffered),
		flushItems: options.FlushItems,
		flushBytes: options.FlushBytes,
	}
}

// newVercelJSONDecoder preserves JSON numbers for opaque fields like audit log
// payloads, where decoding into map[string]any would otherwise round large
// integer identifiers to float64 before they are written as OTEL attributes.
func newVercelJSONDecoder(reader io.Reader) *json.Decoder {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	return decoder
}

// inputOffset reports bytes consumed from the stream.
func (d *vercelStreamDecoder) inputOffset() int64 {
	return d.decoder.InputOffset()
}

// flushReached reports whether the current batch hit a flush threshold.
func (d *vercelStreamDecoder) flushReached(records, startOffset int64) bool {
	if d.flushItems > 0 && records >= d.flushItems {
		return true
	}
	if d.flushBytes > 0 && d.inputOffset()-startOffset >= d.flushBytes {
		return true
	}
	return false
}

// decode reads the next record from the stream into v, handling both the JSON
// array and NDJSON framings. It returns io.EOF once the stream is drained.
func (d *vercelStreamDecoder) decode(v any) error {
	if err := d.init(); err != nil {
		return err
	}
	if d.inArray {
		if d.arrayDone {
			return io.EOF
		}
		if !d.decoder.More() {
			if _, err := d.decoder.Token(); err != nil {
				return err
			}
			d.arrayDone = true
			return io.EOF
		}
	}
	return d.decoder.Decode(v)
}

// readRecord reads the next record as raw bytes. Only the first record of a
// request needs this to resolve routing before the schema struct is known.
func (d *vercelStreamDecoder) readRecord() (rawRecord, error) {
	var raw json.RawMessage
	if err := d.decode(&raw); err != nil {
		return rawRecord{}, err
	}
	return rawRecord{raw: raw}, nil
}

func (d *vercelStreamDecoder) init() error {
	if d.initialized {
		return nil
	}
	d.initialized = true

	for {
		next, err := d.reader.ReadByte()
		if err != nil {
			return err
		}
		if isJSONWhitespace(next) {
			continue
		}
		unreadErr := d.reader.UnreadByte()
		if unreadErr != nil {
			return unreadErr
		}
		if next != '[' {
			return nil
		}

		token, err := d.decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok || delim != '[' {
			return errors.New("expected vercel payload array")
		}
		d.inArray = true
		return nil
	}
}

func isJSONWhitespace(value byte) bool {
	return value == ' ' || value == '\n' || value == '\r' || value == '\t'
}

// rawRecord holds the undecoded bytes of one Vercel record.
type rawRecord struct {
	raw json.RawMessage
}

// schema returns the validated routing schema. Missing schema means logs.
func (r rawRecord) schema() (schema, error) {
	var probe struct {
		Schema json.RawMessage `json:"schema"`
	}
	if err := json.Unmarshal(r.raw, &probe); err != nil {
		return "", err
	}
	if len(probe.Schema) == 0 {
		// Logs have no schema field.
		// See: https://vercel.com/docs/drains/reference/logs#logs-schema.
		return schemaLogs, nil
	}

	var recordSchema string
	if err := json.Unmarshal(probe.Schema, &recordSchema); err != nil {
		return "", fmt.Errorf("%w %q", errUnsupportedVercelSchema, string(probe.Schema))
	}
	parsedSchema := schema(recordSchema)
	if parsedSchema == schemaLogs {
		return "", fmt.Errorf("%w %q", errUnsupportedVercelSchema, recordSchema)
	}
	if _, ok := schemaRoutes[parsedSchema]; !ok {
		return "", fmt.Errorf("%w %q", errUnsupportedVercelSchema, recordSchema)
	}
	return parsedSchema, nil
}

// recordSource decodes a single Vercel record into a schema-specific value. The
// first record of a batch decodes from raw bytes already read to resolve
// routing; the rest decodes straight from the stream, avoiding a second parse.
type recordSource interface {
	decode(v any) error
}

// rawRecordSource decodes the buffered bytes of the already-read first record.
type rawRecordSource struct {
	raw json.RawMessage
}

func (s rawRecordSource) decode(v any) error {
	return newVercelJSONDecoder(bytes.NewReader(s.raw)).Decode(v)
}
