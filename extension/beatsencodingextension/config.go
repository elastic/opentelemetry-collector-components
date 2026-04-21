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
	"errors"
	"fmt"
)

// Format defines how the incoming raw bytes should be interpreted.
type Format string

const (
	// FormatJSON indicates the input is a JSON document that may contain
	// wrapped records (use Unwrap to extract them).
	FormatJSON Format = "json"

	// FormatText indicates the input is newline-delimited text where
	// each line becomes a separate log record.
	FormatText Format = "text"
)

// DataStreamConfig defines the data stream routing attributes.
type DataStreamConfig struct {
	Dataset   string `mapstructure:"dataset"`
	Namespace string `mapstructure:"namespace"`
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

	// prevent unkeyed literal initialization
	_ struct{}
}

func (c *Config) Validate() error {
	switch c.Format {
	case FormatJSON, FormatText:
	default:
		return fmt.Errorf("invalid format %q: must be %q or %q", c.Format, FormatJSON, FormatText)
	}

	if len(c.Unwrap) > 0 && c.Format != FormatJSON {
		return fmt.Errorf("unwrap is only supported when format is %q", FormatJSON)
	}

	if c.DataStream.Dataset == "" {
		return errors.New("data_stream.dataset is required")
	}

	if c.DataStream.Namespace == "" {
		return errors.New("data_stream.namespace is required")
	}

	return nil
}
