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

package processors // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/processors"

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

type compiledSplit struct {
	from             string
	to               string
	re               *regexp.Regexp
	ignoreMissing    *bool
	preserveTrailing *bool
	ignoreFailure    bool
}

func compileSplit(p *dsl.SplitProcessor) (Compiled, error) {
	if p.From == "" {
		return nil, errors.New("streamlang/split: 'from' is required")
	}
	if p.Separator == "" {
		return nil, errors.New("streamlang/split: 'separator' is required")
	}
	re, err := regexp.Compile(p.Separator)
	if err != nil {
		return nil, fmt.Errorf("streamlang/split: invalid separator regex: %w", err)
	}
	to := p.To
	if to == "" {
		to = p.From
	}
	return &compiledSplit{
		from:             p.From,
		to:               to,
		re:               re,
		ignoreMissing:    p.IgnoreMissing,
		preserveTrailing: p.PreserveTrailing,
		ignoreFailure:    p.IgnoreFailure,
	}, nil
}

func (c *compiledSplit) Action() string      { return "split" }
func (c *compiledSplit) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledSplit) Execute(d document.Document) error {
	v, ok := d.Get(c.from)
	if !ok {
		if c.ignoreMissing != nil && *c.ignoreMissing {
			return &SkipError{Reason: "field missing"}
		}
		return fmt.Errorf("streamlang/split: field %q not found", c.from)
	}
	s := v.Str()
	parts := c.re.Split(s, -1)
	if c.preserveTrailing == nil || !*c.preserveTrailing {
		// Trim trailing empty strings.
		for len(parts) > 0 && parts[len(parts)-1] == "" {
			parts = parts[:len(parts)-1]
		}
	}
	out := make([]document.Value, len(parts))
	for i, p := range parts {
		out[i] = document.StringValue(p)
	}
	return d.Set(c.to, document.SliceValue(out))
}
