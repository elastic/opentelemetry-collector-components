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
	"slices"
	"strconv"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

type compiledSort struct {
	from          string
	to            string
	desc          bool
	ignoreMissing *bool
	ignoreFailure bool
}

func compileSort(p *dsl.SortProcessor) (Compiled, error) {
	if p.From == "" {
		return nil, errors.New("streamlang/sort: 'from' is required")
	}
	to := p.To
	if to == "" {
		to = p.From
	}
	desc := false
	switch p.Order {
	case "", "asc":
	case "desc":
		desc = true
	default:
		return nil, fmt.Errorf("streamlang/sort: invalid order %q", p.Order)
	}
	return &compiledSort{
		from:          p.From,
		to:            to,
		desc:          desc,
		ignoreMissing: p.IgnoreMissing,
		ignoreFailure: p.IgnoreFailure,
	}, nil
}

func (c *compiledSort) Action() string      { return "sort" }
func (c *compiledSort) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledSort) Execute(d document.Document) error {
	v, ok := d.Get(c.from)
	if !ok {
		if c.ignoreMissing != nil && *c.ignoreMissing {
			return &SkipError{Reason: "field missing"}
		}
		return fmt.Errorf("streamlang/sort: field %q not found", c.from)
	}
	if v.Type() != document.ValueTypeSlice {
		return fmt.Errorf("streamlang/sort: field %q is not an array", c.from)
	}
	src := v.Slice()
	out := make([]document.Value, len(src))
	copy(out, src)

	allNumeric := true
	allBool := true
	for _, e := range out {
		if !isNumericValue(e) {
			allNumeric = false
		}
		if e.Type() != document.ValueTypeBool {
			allBool = false
		}
	}

	cmp := func(a, b document.Value) int {
		switch {
		case allNumeric:
			af, _ := numericFloat(a)
			bf, _ := numericFloat(b)
			switch {
			case af < bf:
				return -1
			case af > bf:
				return 1
			default:
				return 0
			}
		case allBool:
			ab := a.Bool()
			bb := b.Bool()
			switch {
			case !ab && bb:
				return -1
			case ab && !bb:
				return 1
			default:
				return 0
			}
		default:
			as, bs := a.Str(), b.Str()
			switch {
			case as < bs:
				return -1
			case as > bs:
				return 1
			default:
				return 0
			}
		}
	}
	if c.desc {
		slices.SortStableFunc(out, func(a, b document.Value) int { return -cmp(a, b) })
	} else {
		slices.SortStableFunc(out, cmp)
	}
	return d.Set(c.to, document.SliceValue(out))
}

func isNumericValue(v document.Value) bool {
	switch v.Type() {
	case document.ValueTypeInt, document.ValueTypeDouble:
		return true
	case document.ValueTypeStr:
		_, err := strconv.ParseFloat(v.Str(), 64)
		return err == nil
	}
	return false
}

func numericFloat(v document.Value) (float64, bool) {
	switch v.Type() {
	case document.ValueTypeInt:
		return float64(v.Int()), true
	case document.ValueTypeDouble:
		return v.Double(), true
	case document.ValueTypeStr:
		f, err := strconv.ParseFloat(v.Str(), 64)
		return f, err == nil
	}
	return 0, false
}
