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

	"github.com/goccy/go-json"
)

// Format defines how the incoming raw bytes should be interpreted.
type Format string

const (
	// FormatJSON indicates the input is a JSON document that may contain
	// wrapped records (use Unwrap to extract them).
	FormatJSON Format = "json"

	// FormatNDJSON indicates the input is newline-delimited JSON (one
	// JSON object per line). Each line becomes a separate log record.
	FormatNDJSON Format = "ndjson"

	// FormatText indicates the input is newline-delimited text where
	// each line becomes a separate log record.
	FormatText Format = "text"
)

// RoutingConfig defines the data stream routing attributes.
type RoutingConfig struct {
	Dataset   string `mapstructure:"dataset"`
	Namespace string `mapstructure:"namespace"`
}

// Config defines the configuration for the beats encoding extension.
type Config struct {
	// Format of the incoming data: "json", "ndjson", or "text".
	Format Format `mapstructure:"format"`

	// Unwrap is a JSONPath expression to extract individual records from
	// a wrapper structure (e.g. "$.records[*]" for Azure Diagnostic Settings,
	// "$.Records[*]" for AWS CloudTrail). Only used when Format is "json".
	// When empty and Format is "json", the entire input is treated as a
	// single record.
	Unwrap string `mapstructure:"unwrap,omitempty"`

	// TargetField is the body map key where the raw content of each
	// record is stored. Defaults to "message".
	TargetField string `mapstructure:"target_field,omitempty"`

	// Routing defines the data stream routing attributes.
	Routing RoutingConfig `mapstructure:"routing"`

	// prevent unkeyed literal initialization
	_ struct{}
}

func (c *Config) Validate() error {
	switch c.Format {
	case FormatJSON, FormatNDJSON, FormatText:
	default:
		return fmt.Errorf("invalid format %q: must be %q, %q, or %q", c.Format, FormatJSON, FormatNDJSON, FormatText)
	}

	if c.Unwrap != "" && c.Format != FormatJSON {
		return fmt.Errorf("unwrap is only supported when format is %q", FormatJSON)
	}

	if c.Unwrap != "" {
		if _, err := json.CreatePath(c.Unwrap); err != nil {
			return fmt.Errorf("invalid unwrap JSONPath expression %q: %w", c.Unwrap, err)
		}
	}

	if c.Routing.Dataset == "" {
		return fmt.Errorf("routing.dataset is required")
	}

	if c.Routing.Namespace == "" {
		return fmt.Errorf("routing.namespace is required")
	}

	return nil
}
