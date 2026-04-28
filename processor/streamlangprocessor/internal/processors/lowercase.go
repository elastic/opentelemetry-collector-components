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

type compiledLowercase struct {
	from          string
	to            string
	ignoreMissing *bool
	ignoreFailure bool
}

func compileLowercase(p *dsl.LowercaseProcessor) (Compiled, error) {
	if p.From == "" {
		return nil, errors.New("streamlang/lowercase: 'from' is required")
	}
	return &compiledLowercase{
		from:          p.From,
		to:            p.To,
		ignoreMissing: p.IgnoreMissing,
		ignoreFailure: p.IgnoreFailure,
	}, nil
}

func (c *compiledLowercase) Action() string      { return "lowercase" }
func (c *compiledLowercase) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledLowercase) Execute(d document.Document) error {
	val, ok := d.Get(c.from)
	if !ok {
		if c.ignoreMissing != nil && *c.ignoreMissing {
			return &SkipError{Reason: "field missing"}
		}
		return fmt.Errorf("streamlang/lowercase: field %q not found", c.from)
	}
	s, err := stringCoerce(val)
	if err != nil {
		return fmt.Errorf("streamlang/lowercase: %w", err)
	}
	return d.Set(targetOrSelf(c.to, c.from), document.StringValue(strings.ToLower(s)))
}
