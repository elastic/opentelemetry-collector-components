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

package condition // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/condition"

import (
	"fmt"
	"strings"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/coerce"
)

// Compile builds a CompiledCondition from a dsl.Condition. A nil input or a
// CondAlways returns nil — the hot-path sentinel meaning "no guard".
// CondNever returns a closure that always returns false.
func Compile(c *dsl.Condition) (CompiledCondition, error) {
	if c == nil {
		return nil, nil
	}
	switch c.Kind {
	case dsl.CondAlways:
		return nil, nil
	case dsl.CondNever:
		return func(Source) bool { return false }, nil
	case dsl.CondAnd:
		return compileAnd(c.And)
	case dsl.CondOr:
		return compileOr(c.Or)
	case dsl.CondNot:
		return compileNot(c.Not)
	case dsl.CondFilter:
		if c.Filter == nil {
			return nil, fmt.Errorf("condition: filter kind with nil Filter")
		}
		return compileFilter(c.Filter)
	case dsl.CondInvalid:
		return nil, fmt.Errorf("condition: invalid kind (zero value)")
	default:
		return nil, fmt.Errorf("condition: unknown kind %v", c.Kind)
	}
}

func compileAnd(items []dsl.Condition) (CompiledCondition, error) {
	if len(items) == 0 {
		// Empty AND is true — drop to the always-true sentinel.
		return nil, nil
	}
	children := make([]CompiledCondition, 0, len(items))
	for i := range items {
		child, err := Compile(&items[i])
		if err != nil {
			return nil, err
		}
		if child == nil {
			// Always-true child: skip it.
			continue
		}
		children = append(children, child)
	}
	if len(children) == 0 {
		return nil, nil
	}
	return func(s Source) bool {
		for _, ch := range children {
			if !ch(s) {
				return false
			}
		}
		return true
	}, nil
}

func compileOr(items []dsl.Condition) (CompiledCondition, error) {
	if len(items) == 0 {
		// Empty OR is false.
		return func(Source) bool { return false }, nil
	}
	children := make([]CompiledCondition, 0, len(items))
	hasAlways := false
	for i := range items {
		child, err := Compile(&items[i])
		if err != nil {
			return nil, err
		}
		if child == nil {
			// Always-true short-circuits the whole OR.
			hasAlways = true
			continue
		}
		children = append(children, child)
	}
	if hasAlways {
		return nil, nil
	}
	if len(children) == 0 {
		return func(Source) bool { return false }, nil
	}
	return func(s Source) bool {
		for _, ch := range children {
			if ch(s) {
				return true
			}
		}
		return false
	}, nil
}

func compileNot(inner *dsl.Condition) (CompiledCondition, error) {
	if inner == nil {
		return nil, fmt.Errorf("condition: not kind with nil child")
	}
	child, err := Compile(inner)
	if err != nil {
		return nil, err
	}
	if child == nil {
		// Not(always) -> false.
		return func(Source) bool { return false }, nil
	}
	return func(s Source) bool { return !child(s) }, nil
}

// operator counts the number of operator fields set on a FilterCondition.
// The parser already enforces "exactly one" so we treat anything else as
// a programming error and surface it.
func countOperators(f *dsl.FilterCondition) int {
	n := 0
	if f.HasEq {
		n++
	}
	if f.HasNeq {
		n++
	}
	if f.HasLt {
		n++
	}
	if f.HasLte {
		n++
	}
	if f.HasGt {
		n++
	}
	if f.HasGte {
		n++
	}
	if f.HasContain {
		n++
	}
	if f.HasStarts {
		n++
	}
	if f.HasEnds {
		n++
	}
	if f.HasIncl {
		n++
	}
	if f.Range != nil {
		n++
	}
	if f.Exists != nil {
		n++
	}
	return n
}

func compileFilter(f *dsl.FilterCondition) (CompiledCondition, error) {
	if strings.TrimSpace(f.Field) == "" {
		return nil, fmt.Errorf("condition: filter has empty field")
	}
	if n := countOperators(f); n != 1 {
		return nil, fmt.Errorf("condition: filter on %q must have exactly one operator (got %d)", f.Field, n)
	}

	field := f.Field

	switch {
	case f.HasEq:
		target := f.Eq
		return func(s Source) bool {
			v, ok := s.Field(field)
			if !ok {
				return false
			}
			return coerce.Equal(v, target)
		}, nil

	case f.HasNeq:
		target := f.Neq
		return func(s Source) bool {
			v, ok := s.Field(field)
			if !ok {
				// ES semantics: "field is not equal to X" is true when
				// the field is absent.
				return true
			}
			return !coerce.Equal(v, target)
		}, nil

	case f.HasLt:
		return cmpFilter(field, f.Lt, func(o int) bool { return o < 0 }), nil
	case f.HasLte:
		return cmpFilter(field, f.Lte, func(o int) bool { return o <= 0 }), nil
	case f.HasGt:
		return cmpFilter(field, f.Gt, func(o int) bool { return o > 0 }), nil
	case f.HasGte:
		return cmpFilter(field, f.Gte, func(o int) bool { return o >= 0 }), nil

	case f.HasContain:
		target := f.Contains
		return stringFilter(field, target, strings.Contains), nil
	case f.HasStarts:
		target := f.StartsWith
		return stringFilter(field, target, strings.HasPrefix), nil
	case f.HasEnds:
		target := f.EndsWith
		return stringFilter(field, target, strings.HasSuffix), nil

	case f.HasIncl:
		target := f.Includes
		return func(s Source) bool {
			v, ok := s.Field(field)
			if !ok {
				return false
			}
			arr, ok := v.([]any)
			if !ok {
				// Non-array: fall back to scalar equality, matching the
				// Rust reference's array_includes.
				return coerce.Equal(v, target)
			}
			for _, elem := range arr {
				if coerce.Equal(elem, target) {
					return true
				}
			}
			return false
		}, nil

	case f.Range != nil:
		return compileRange(field, f.Range), nil

	case f.Exists != nil:
		shouldExist := *f.Exists
		return func(s Source) bool {
			return s.Has(field) == shouldExist
		}, nil
	}

	return nil, fmt.Errorf("condition: unreachable — no operator matched on %q", field)
}

func cmpFilter(field string, target any, accept func(int) bool) CompiledCondition {
	return func(s Source) bool {
		v, ok := s.Field(field)
		if !ok {
			return false
		}
		ord, ok := coerce.Compare(v, target)
		if !ok {
			return false
		}
		return accept(ord)
	}
}

func stringFilter(field string, target any, op func(string, string) bool) CompiledCondition {
	// Lowercase the target once at compile-time.
	needle := strings.ToLower(coerce.Stringify(target))
	return func(s Source) bool {
		v, ok := s.Field(field)
		if !ok {
			return false
		}
		hay := strings.ToLower(coerce.Stringify(v))
		return op(hay, needle)
	}
}

func compileRange(field string, r *dsl.RangeCondition) CompiledCondition {
	hasGt := r.Gt != nil
	hasGte := r.Gte != nil
	hasLt := r.Lt != nil
	hasLte := r.Lte != nil

	gt, gte, lt, lte := r.Gt, r.Gte, r.Lt, r.Lte

	return func(s Source) bool {
		fv, ok := s.Field(field)
		if !ok {
			return false
		}
		if hasGt {
			ord, ok := coerce.Compare(fv, gt)
			if !ok || ord <= 0 {
				return false
			}
		}
		if hasGte {
			ord, ok := coerce.Compare(fv, gte)
			if !ok || ord < 0 {
				return false
			}
		}
		if hasLt {
			ord, ok := coerce.Compare(fv, lt)
			if !ok || ord >= 0 {
				return false
			}
		}
		if hasLte {
			ord, ok := coerce.Compare(fv, lte)
			if !ok || ord > 0 {
				return false
			}
		}
		return true
	}
}
