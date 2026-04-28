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

type compiledRename struct {
	from          string
	to            string
	ignoreMissing *bool
	override      *bool
	ignoreFailure bool
}

func compileRename(p *dsl.RenameProcessor) (Compiled, error) {
	if p.From == "" || p.To == "" {
		return nil, errors.New("streamlang/rename: 'from' and 'to' are required")
	}
	return &compiledRename{
		from:          p.From,
		to:            p.To,
		ignoreMissing: p.IgnoreMissing,
		override:      p.Override,
		ignoreFailure: p.IgnoreFailure,
	}, nil
}

func (c *compiledRename) Action() string      { return "rename" }
func (c *compiledRename) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledRename) Execute(d document.Document) error {
	if c.from == c.to {
		// No-op rename: preserve source.
		return nil
	}
	val, ok := d.Get(c.from)
	if !ok {
		if c.ignoreMissing != nil && *c.ignoreMissing {
			return &SkipError{Reason: "field missing"}
		}
		return fmt.Errorf("streamlang/rename: field %q not found", c.from)
	}
	if (c.override == nil || !*c.override) && d.Has(c.to) {
		return fmt.Errorf("streamlang/rename: target %q exists", c.to)
	}
	if err := d.Set(c.to, val); err != nil {
		return err
	}
	d.Remove(c.from)
	return nil
}
