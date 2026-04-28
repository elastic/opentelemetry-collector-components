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

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

type compiledSet struct {
	to            string
	override      *bool
	hasValue      bool
	value         any
	copyFrom      string
	ignoreFailure bool
}

func compileSet(p *dsl.SetProcessor) (Compiled, error) {
	if !p.HasValue && p.CopyFrom == "" {
		return nil, errors.New("streamlang/set: requires exactly one of value or copy_from")
	}
	if p.To == "" {
		return nil, errors.New("streamlang/set: 'to' is required")
	}
	return &compiledSet{
		to:            p.To,
		override:      p.Override,
		hasValue:      p.HasValue,
		value:         p.Value,
		copyFrom:      p.CopyFrom,
		ignoreFailure: p.IgnoreFailure,
	}, nil
}

func (c *compiledSet) Action() string      { return "set" }
func (c *compiledSet) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledSet) Execute(d document.Document) error {
	// Override default for `set` matches Elasticsearch ingest: override=true.
	// Skip only when override is explicitly false and the target already exists.
	if c.override != nil && !*c.override && d.Has(c.to) {
		return &SkipError{Reason: "target exists"}
	}

	var v document.Value
	if c.hasValue {
		// Literal value (including nil → NilValue).
		val, err := document.FromAny(c.value)
		if err != nil {
			return err
		}
		v = val
	} else {
		// copy_from path.
		got, ok := d.Get(c.copyFrom)
		if !ok {
			return &SkipError{Reason: "copy_from missing"}
		}
		v = got
	}
	return d.Set(c.to, v)
}
