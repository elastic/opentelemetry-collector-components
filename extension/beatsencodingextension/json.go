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
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/goccy/go-json"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/xstreamencoding"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// newJSONDecoder returns a streaming decoder over the records of a JSON
// stream, as jsonElements defines them. Mappings, when configured, are
// applied to every record when unwrap is set.
func (e *beatsEncodingExtension) newJSONDecoder(reader io.Reader, options ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	opts := encoding.NewDecoderOptions(options...)
	batchHelper := xstreamencoding.NewBatchHelper(options...)
	elements := newJSONElements(reader, e.config.Unwrap)

	// Skip records that were already processed in a previous decoder session.
	recordCount := int64(0)
	for recordCount < opts.Offset {
		if _, err := elements.next(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("EOF reached before skipping %d records (only found %d)", opts.Offset, recordCount)
			}
			return nil, fmt.Errorf("skipping record %d: %w", recordCount, err)
		}
		recordCount++
	}

	decodeF := func() (plog.Logs, error) {
		logs := plog.NewLogs()
		sl := newScopeLogs(logs)
		now := pcommon.NewTimestampFromTime(time.Now())
		eventCreated := now.AsTime().UTC().Format(time.RFC3339Nano)

		for {
			raw, err := elements.next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return logs, err
			}

			data, err := e.recordFields(raw)
			if err != nil {
				return logs, err
			}
			if err := e.appendLogRecord(sl, now, eventCreated, data); err != nil {
				return plog.NewLogs(), err
			}

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

	offsetF := func() int64 { return recordCount }
	return xstreamencoding.NewLogsDecoderAdapter(decodeF, offsetF), nil
}

// recordFields is what one record contributes to the log body: the mapped
// fields when mappings are configured with unwrap, otherwise the record
// itself as the message.
func (e *beatsEncodingExtension) recordFields(raw json.RawMessage) ([]MappedField, error) {
	if len(e.config.Mappings) == 0 || len(e.config.Unwrap) == 0 {
		return []MappedField{{
			Mapping: FieldMapping{Type: FieldTypeString, Destination: "message"},
			Value:   string(bytes.TrimSpace(raw)),
		}}, nil
	}

	// Number keeps integers intact for the Integer mappings.
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var asMap map[string]any
	if err := dec.Decode(&asMap); err != nil {
		return nil, fmt.Errorf("decoding record for mappings: %w", err)
	}

	var data []MappedField
	for _, m := range e.config.Mappings {
		if val, ok := asMap[m.Source]; ok {
			data = append(data, MappedField{Mapping: m, Value: val})
		}
	}
	return data, nil
}

// navigateToArray walks the JSON token stream to find and enter the array
// at the path specified by keys. For example, keys ["data", "items"]
// navigates into {"data": {"items": [...]}} and positions the decoder
// right after the opening '[' of the target array. If a key repeats, the
// first one is used; Beats uses the last, but valid producers do not repeat
// keys and keeping the first lets the array stream.
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

// jsonElements walks a JSON stream the way the Beats aws-s3 input reads an
// object: top-level values one after another, and when an unwrap path is
// set, the elements of the array at that path inside each value. Keys after
// the array are skipped so the next value is reached.
type jsonElements struct {
	dec *json.Decoder

	// unwrap is the key path to the array inside each value. Empty when
	// every value is a record, and when rootArray is set.
	unwrap []string

	// rootArray means each top-level value is itself the array to split.
	rootArray bool

	// inArray is true while the decoder sits inside the target array of the
	// current value.
	inArray bool
}

func newJSONElements(r io.Reader, unwrap []string) *jsonElements {
	it := &jsonElements{dec: json.NewDecoder(r)}
	if len(unwrap) == 1 && unwrap[0] == unwrapRootArray {
		it.rootArray = true
	} else {
		it.unwrap = unwrap
	}
	return it
}

// next returns the raw bytes of the next record, or io.EOF once the input
// has no more values.
func (it *jsonElements) next() (json.RawMessage, error) {
	raw, err := it.read()
	if err != nil && err != io.EOF && errors.Is(err, io.EOF) {
		// The input ended inside a value, so it is truncated.
		return nil, fmt.Errorf("%w: %v", io.ErrUnexpectedEOF, err)
	}
	return raw, err
}

// read returns the next record. It returns the bare io.EOF only at a clean
// end between top-level values.
func (it *jsonElements) read() (json.RawMessage, error) {
	for {
		if it.inArray {
			if it.dec.More() {
				var raw json.RawMessage
				if err := it.dec.Decode(&raw); err != nil {
					return nil, fmt.Errorf("decoding array element: %w", err)
				}
				return raw, nil
			}
			// The array is done: consume its ']' and read past the rest of
			// every object navigateToArray opened, one per unwrap key.
			if err := expectDelim(it.dec, ']'); err != nil {
				return nil, err
			}
			for range it.unwrap {
				if err := closeObject(it.dec); err != nil {
					return nil, err
				}
			}
			it.inArray = false
			continue
		}

		if !it.dec.More() {
			// More is also false on a read error or a stray closing
			// delimiter, so read the token to tell those from the end.
			tok, err := it.dec.Token()
			if err == io.EOF {
				return nil, io.EOF
			}
			if err != nil {
				return nil, fmt.Errorf("reading next value: %w", err)
			}
			return nil, fmt.Errorf("unexpected %v between top-level values", tok)
		}

		switch {
		case it.rootArray:
			if err := expectDelim(it.dec, '['); err != nil {
				return nil, fmt.Errorf("unwrap %s: %w", unwrapRootArray, err)
			}
		case len(it.unwrap) > 0:
			if err := navigateToArray(it.dec, it.unwrap); err != nil {
				return nil, fmt.Errorf("navigating to unwrap %v: %w", it.unwrap, err)
			}
		default:
			var raw json.RawMessage
			if err := it.dec.Decode(&raw); err != nil {
				return nil, fmt.Errorf("decoding value: %w", err)
			}
			return raw, nil
		}
		it.inArray = true
	}
}

// expectDelim consumes one token and checks it is the delimiter d.
func expectDelim(dec *json.Decoder, d json.Delim) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("expected '%c': %w", d, err)
	}
	if got, ok := tok.(json.Delim); !ok || got != d {
		return fmt.Errorf("expected '%c', got %v", d, tok)
	}
	return nil
}

// closeObject skips the remaining members of the object the decoder is in
// and consumes its closing brace.
func closeObject(dec *json.Decoder) error {
	for dec.More() {
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("reading key after array: %w", err)
		}
		if err := skipValue(dec); err != nil {
			return fmt.Errorf("skipping value after array: %w", err)
		}
	}
	return expectDelim(dec, '}')
}
