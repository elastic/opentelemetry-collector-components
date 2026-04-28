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

type compiledConcat struct {
	parts         []dsl.ConcatPart
	to            string
	ignoreMissing *bool
	ignoreFailure bool
}

func compileConcat(p *dsl.ConcatProcessor) (Compiled, error) {
	if len(p.From) == 0 {
		return nil, errors.New("streamlang/concat: 'from' is required")
	}
	if p.To == "" {
		return nil, errors.New("streamlang/concat: 'to' is required")
	}
	return &compiledConcat{
		parts:         p.From,
		to:            p.To,
		ignoreMissing: p.IgnoreMissing,
		ignoreFailure: p.IgnoreFailure,
	}, nil
}

func (c *compiledConcat) Action() string      { return "concat" }
func (c *compiledConcat) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledConcat) Execute(d document.Document) error {
	var sb strings.Builder
	for _, part := range c.parts {
		switch part.Type {
		case "literal":
			sb.WriteString(part.Value)
		case "field":
			v, ok := d.Get(part.Value)
			if !ok {
				if c.ignoreMissing != nil && *c.ignoreMissing {
					continue
				}
				return fmt.Errorf("streamlang/concat: field %q not found", part.Value)
			}
			if v.Type() == document.ValueTypeSlice || v.Type() == document.ValueTypeMap {
				return fmt.Errorf("streamlang/concat: field %q is not a scalar", part.Value)
			}
			sb.WriteString(v.Str())
		default:
			return fmt.Errorf("streamlang/concat: invalid part type %q", part.Type)
		}
	}
	return d.Set(c.to, document.StringValue(sb.String()))
}
