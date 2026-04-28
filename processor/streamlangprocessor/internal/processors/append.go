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
	"reflect"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

type compiledAppend struct {
	to              string
	values          []document.Value
	allowDuplicates *bool
	ignoreFailure   bool
}

func compileAppend(p *dsl.AppendProcessor) (Compiled, error) {
	if p.To == "" {
		return nil, errors.New("streamlang/append: 'to' is required")
	}
	vals := make([]document.Value, 0, len(p.Value))
	for _, raw := range p.Value {
		v, err := document.FromAny(raw)
		if err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}
	return &compiledAppend{
		to:              p.To,
		values:          vals,
		allowDuplicates: p.AllowDuplicates,
		ignoreFailure:   p.IgnoreFailure,
	}, nil
}

func (c *compiledAppend) Action() string      { return "append" }
func (c *compiledAppend) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledAppend) Execute(d document.Document) error {
	// Build current array (or coerce existing scalar to single-element array).
	// Materialize through AsAny+FromAny so the values don't share pcommon
	// storage with the slot we're about to overwrite via Set.
	var arr []document.Value
	if existing, ok := d.Get(c.to); ok {
		snap, err := document.FromAny(existing.AsAny())
		if err != nil {
			return err
		}
		switch snap.Type() {
		case document.ValueTypeSlice:
			arr = snap.Slice()
		default:
			arr = []document.Value{snap}
		}
	}

	// Default per task spec: dedupe unless allow_duplicates == true.
	dedupe := c.allowDuplicates == nil || !*c.allowDuplicates

	for _, v := range c.values {
		if dedupe && containsValue(arr, v) {
			continue
		}
		arr = append(arr, v)
	}
	return d.Set(c.to, document.SliceValue(arr))
}

// containsValue reports whether arr already contains v under simple equality.
func containsValue(arr []document.Value, v document.Value) bool {
	for _, e := range arr {
		if equalValue(e, v) {
			return true
		}
	}
	return false
}

func equalValue(a, b document.Value) bool {
	if a.Type() != b.Type() {
		// Allow Int/Double cross-equality (1 == 1.0).
		if (a.Type() == document.ValueTypeInt && b.Type() == document.ValueTypeDouble) ||
			(a.Type() == document.ValueTypeDouble && b.Type() == document.ValueTypeInt) {
			return a.Double() == b.Double()
		}
		return false
	}
	switch a.Type() {
	case document.ValueTypeEmpty:
		return true
	case document.ValueTypeStr:
		return a.Str() == b.Str()
	case document.ValueTypeBool:
		return a.Bool() == b.Bool()
	case document.ValueTypeInt:
		return a.Int() == b.Int()
	case document.ValueTypeDouble:
		return a.Double() == b.Double()
	default:
		return reflect.DeepEqual(a.AsAny(), b.AsAny())
	}
}
