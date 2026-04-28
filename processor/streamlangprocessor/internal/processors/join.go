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
	"strings"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

type compiledJoin struct {
	from          []string
	delimiter     string
	to            string
	ignoreMissing *bool
	ignoreFailure bool
}

func compileJoin(p *dsl.JoinProcessor) (Compiled, error) {
	if len(p.From) == 0 {
		return nil, errors.New("streamlang/join: 'from' is required")
	}
	if p.To == "" {
		return nil, errors.New("streamlang/join: 'to' is required")
	}
	return &compiledJoin{
		from:          p.From,
		delimiter:     p.Delimiter,
		to:            p.To,
		ignoreMissing: p.IgnoreMissing,
		ignoreFailure: p.IgnoreFailure,
	}, nil
}

func (c *compiledJoin) Action() string      { return "join" }
func (c *compiledJoin) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledJoin) Execute(d document.Document) error {
	parts := make([]string, 0, len(c.from))
	for _, path := range c.from {
		v, ok := d.Get(path)
		if !ok {
			if c.ignoreMissing != nil && *c.ignoreMissing {
				parts = append(parts, "")
				continue
			}
			return fmt.Errorf("streamlang/join: field %q not found", path)
		}
		s, err := joinStringify(v, c.delimiter)
		if err != nil {
			return fmt.Errorf("streamlang/join: field %q: %w", path, err)
		}
		parts = append(parts, s)
	}
	return d.Set(c.to, document.StringValue(strings.Join(parts, c.delimiter)))
}

// joinStringify renders a Value to a string. Slices are recursively joined
// with the same delimiter. Maps are an error.
func joinStringify(v document.Value, delim string) (string, error) {
	if v.Type() == document.ValueTypeSlice {
		items := v.Slice()
		strs := make([]string, len(items))
		for i, it := range items {
			s, err := joinStringify(it, delim)
			if err != nil {
				return "", err
			}
			strs[i] = s
		}
		return strings.Join(strs, delim), nil
	}
	if v.Type() == document.ValueTypeMap {
		return "", errors.New("cannot join a map value")
	}
	return v.Str(), nil
}
