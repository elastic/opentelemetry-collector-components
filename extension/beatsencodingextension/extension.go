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
	"time"

	"github.com/goccy/go-json"
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

// UnmarshalLogs converts raw bytes into OTel log records formatted for
// Beats/EA integration compatibility.
//
// Each extracted record is stored as a raw string under the configured
// target field (default: "message") in the log record body map.
// Data stream routing attributes and elastic.mapping.mode are set
// so mOTLP routes the document to the correct integration data stream.
func (e *beatsEncodingExtension) UnmarshalLogs(buf []byte) (plog.Logs, error) {
	records, err := e.extractRecords(buf)
	if err != nil {
		return plog.Logs{}, fmt.Errorf("extracting records: %w", err)
	}

	if len(records) == 0 {
		return plog.NewLogs(), nil
	}

	logs := plog.NewLogs()
	sl := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()

	sl.Scope().Attributes().PutStr("elastic.mapping.mode", "bodymap")

	now := pcommon.NewTimestampFromTime(time.Now())

	for _, record := range records {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(now)
		lr.SetObservedTimestamp(now)

		lr.Body().SetEmptyMap().PutStr(e.config.TargetField, record)

		attrs := lr.Attributes()
		attrs.PutStr("data_stream.type", "logs")
		attrs.PutStr("data_stream.dataset", e.config.Routing.Dataset)
		attrs.PutStr("data_stream.namespace", e.config.Routing.Namespace)
	}

	return logs, nil
}

// extractRecords extracts individual string records from the raw input
// bytes based on the configured format and unwrap settings.
func (e *beatsEncodingExtension) extractRecords(buf []byte) ([]string, error) {
	switch e.config.Format {
	case FormatText:
		return e.extractTextRecords(buf)
	case FormatNDJSON:
		return e.extractNDJSONRecords(buf)
	case FormatJSON:
		return e.extractJSONRecords(buf)
	default:
		return nil, fmt.Errorf("unsupported format: %q", e.config.Format)
	}
}

// extractTextRecords splits newline-delimited text into individual records.
func (e *beatsEncodingExtension) extractTextRecords(buf []byte) ([]string, error) {
	var records []string
	for _, line := range bytes.Split(buf, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		records = append(records, string(line))
	}
	return records, nil
}

// extractNDJSONRecords splits newline-delimited JSON into individual
// records. Each non-empty line is treated as a separate JSON object
// and stored as a raw string (not parsed).
func (e *beatsEncodingExtension) extractNDJSONRecords(buf []byte) ([]string, error) {
	var records []string
	for _, line := range bytes.Split(buf, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		records = append(records, string(line))
	}
	return records, nil
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
//
// It supports any valid JSONPath expression, including:
//   - $.records[*]   (Azure Diagnostic Settings)
//   - $.Records[*]   (AWS CloudTrail)
//   - $.events[*]    (custom wrappers)
func (e *beatsEncodingExtension) unwrapJSON(buf []byte) ([]string, error) {
	extracted, err := e.unwrapPath.Extract(buf)
	if err != nil {
		return nil, fmt.Errorf("unwrapping JSON with path %q: %w", e.config.Unwrap, err)
	}
	if len(extracted) == 0 {
		// JSONPath returned no results —
		// treat the entire input as a single record.
		// This handles cases where the wrapper field is missing,
		// but may also indicate a misconfigured unwrap expression.
		e.logger.Warn("unwrap JSONPath matched no elements, treating entire input as a single record",
			zap.String("unwrap", e.config.Unwrap),
		)
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
		e.logger.Warn("unwrap JSONPath matched only empty elements, treating entire input as a single record",
			zap.String("unwrap", e.config.Unwrap),
		)
		return []string{string(buf)}, nil
	}

	return records, nil
}
