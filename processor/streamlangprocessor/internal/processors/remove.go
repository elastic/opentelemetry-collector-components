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

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

type compiledRemove struct {
	from          string
	ignoreMissing *bool
	ignoreFailure bool
}

func compileRemove(p *dsl.RemoveProcessor) (Compiled, error) {
	if p.From == "" {
		return nil, errors.New("streamlang/remove: 'from' is required")
	}
	return &compiledRemove{
		from:          p.From,
		ignoreMissing: p.IgnoreMissing,
		ignoreFailure: p.IgnoreFailure,
	}, nil
}

func (c *compiledRemove) Action() string      { return "remove" }
func (c *compiledRemove) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledRemove) Execute(d document.Document) error {
	if !d.Has(c.from) {
		if c.ignoreMissing != nil && *c.ignoreMissing {
			return &SkipError{Reason: "field missing"}
		}
		return fmt.Errorf("streamlang/remove: field %q not found", c.from)
	}
	d.Remove(c.from)
	return nil
}
