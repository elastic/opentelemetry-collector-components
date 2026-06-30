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
	"fmt"
)

// Format defines how the incoming raw bytes should be interpreted.
type Format string

// FieldType defines the supported OTel pcommon value types for field mappings.
type FieldType string

const (
	// FormatJSON indicates the input is a JSON document that may contain
	// wrapped records (use Unwrap to extract them).
	FormatJSON Format = "json"

	// FormatText indicates the input is newline-delimited text where
	// each line becomes a separate log record.
	FormatText Format = "text"

	// FormatCSV indicates the input is CSV: the first record is the header
	// (unless CSV.FieldsNames is set) and each subsequent record becomes a
	// log record whose "message" is a JSON object keyed by the header. This
	// mirrors the Beats aws-s3 input's decoding.codec.csv behaviour.
	FormatCSV Format = "csv"

	// FieldTypeString maps to pcommon.Map.PutStr.
	FieldTypeString FieldType = "String"

	// FieldTypeInteger maps to pcommon.Map.PutInt.
	FieldTypeInteger FieldType = "Integer"
)

// DataStreamConfig defines the data stream routing attributes.
type DataStreamConfig struct {
	Dataset   string `mapstructure:"dataset"`
	Namespace string `mapstructure:"namespace"`
}

// CSVConfig configures CSV decoding. Only used when Format is "csv". The
// option names mirror the Beats aws-s3 input's decoding.codec.csv settings so
// configurations are consistent across the Agent and forwarder paths.
type CSVConfig struct {
	// Comma is the field separator, a single character. Defaults to ",".
	// Netskope Log Streaming, for example, uses a single space.
	Comma string `mapstructure:"comma,omitempty"`

	// Comment, if set, is the comment character. Lines beginning with it
	// (before the first non-comment record) are skipped. A single character;
	// must differ from Comma.
	Comment string `mapstructure:"comment,omitempty"`

	// FieldsNames overrides the header. When empty, the first non-comment CSV
	// record is read and used as the header. Named "fields_names" to match the
	// Beats csv codec option.
	FieldsNames []string `mapstructure:"fields_names,omitempty"`

	// LazyQuotes, if true, allows a quote to appear in an unquoted field and
	// a non-doubled quote in a quoted field.
	LazyQuotes bool `mapstructure:"lazy_quotes,omitempty"`

	// TrimLeadingSpace, if true, trims leading white space in a field.
	TrimLeadingSpace bool `mapstructure:"trim_leading_space,omitempty"`
}

// Config defines the configuration for the beats encoding extension.
type Config struct {
	// Format of the incoming data: "json" or "text".
	Format Format `mapstructure:"format"`

	// Unwrap is the sequence of JSON object keys to traverse in order
	// to reach the target array whose elements become individual log records.
	// For example, ["records"] navigates into {"records": [...]}, and
	// ["data", "items"] navigates into {"data": {"items": [...]}}.
	// Only used when Format is "json". When empty, the entire input
	// is treated as a single record.
	Unwrap []string `mapstructure:"unwrap,omitempty"`

	// DataStream defines the data stream routing attributes.
	DataStream DataStreamConfig `mapstructure:"data_stream"`

	// InputType sets the "input.type" field in the log record body,
	// matching what Beats inputs report (e.g. "aws-s3", "azure-eventhub").
	InputType string `mapstructure:"input_type,omitempty"`

	// Tags is a list of strings appended to the "tags" field in the log
	// record body (e.g. ["forwarded", "aws-cloudtrail"]).
	Tags []string `mapstructure:"tags,omitempty"`

	// Fields is a map of key-value pairs added to every log record body.
	// This matches the Beats "fields" configuration, allowing users to
	// inject custom metadata (e.g., environment, team) into each event.
	Fields map[string]any `mapstructure:"fields,omitempty"`

	// Mappings defines which JSON keys to extract from each decoded JSON
	// element and how to store them in the log record body.
	// Only used when Format is "json" and Unwrap is set.
	Mappings []FieldMapping `mapstructure:"mappings,omitempty"`

	// CSV configures CSV decoding. Only used when Format is "csv".
	CSV CSVConfig `mapstructure:"csv,omitempty"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// FieldMapping defines how a single JSON key is extracted and written
// to the log record body.
type FieldMapping struct {
	// Source is the JSON key to read from the decoded object.
	Source string `mapstructure:"source"`

	// Destination is the key to write to in the log body map.
	Destination string `mapstructure:"destination"`

	// Type is the OTel pcommon value type: "String" or "Integer".
	Type FieldType `mapstructure:"type"`

	// Multiplier scales the numeric value before storing.
	// Only applies to FieldTypeInteger. A value of 0 means no scaling.
	Multiplier int64 `mapstructure:"multiplier,omitempty"`
}

func (c *Config) Validate() error {
	switch c.Format {
	case FormatJSON, FormatText, FormatCSV:
	default:
		return fmt.Errorf("invalid format %q: must be %q, %q or %q", c.Format, FormatJSON, FormatText, FormatCSV)
	}

	if len(c.Unwrap) > 0 && c.Format != FormatJSON {
		return fmt.Errorf("unwrap is only supported when format is %q", FormatJSON)
	}

	if c.Format != FormatCSV &&
		(c.CSV.Comma != "" || c.CSV.Comment != "" || len(c.CSV.FieldsNames) > 0 ||
			c.CSV.LazyQuotes || c.CSV.TrimLeadingSpace) {
		return fmt.Errorf("csv options are only supported when format is %q", FormatCSV)
	}

	if c.CSV.Comma != "" && len([]rune(c.CSV.Comma)) != 1 {
		return fmt.Errorf("csv.comma must be a single character, got %q", c.CSV.Comma)
	}

	if c.CSV.Comment != "" && len([]rune(c.CSV.Comment)) != 1 {
		return fmt.Errorf("csv.comment must be a single character, got %q", c.CSV.Comment)
	}

	if c.DataStream.Dataset == "" {
		return fmt.Errorf("data_stream.dataset is required")
	}

	if c.DataStream.Namespace == "" {
		return fmt.Errorf("data_stream.namespace is required")
	}

	for i, m := range c.Mappings {
		if m.Source == "" {
			return fmt.Errorf("mappings[%d].source is required", i)
		}
		if m.Destination == "" {
			return fmt.Errorf("mappings[%d].destination is required", i)
		}
		switch m.Type {
		case FieldTypeString, FieldTypeInteger:
		default:
			return fmt.Errorf("mappings[%d].type %q is invalid: must be %q or %q", i, m.Type, FieldTypeString, FieldTypeInteger)
		}
	}

	return nil
}
