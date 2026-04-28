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

package dsl // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"

// Flatten unwinds every ConditionBlock by recursively walking its Steps and
// AND-combining the block's Condition with each contained processor's
// existing Where clause. The result is a flat slice of FlatProcessor.
//
// Mirrors flatten_steps.ts and condition.rs::flatten.
func Flatten(d DSL) []FlatProcessor {
	return flattenSteps(d.Steps, nil)
}

func flattenSteps(steps []Step, parent *Condition) []FlatProcessor {
	out := make([]FlatProcessor, 0, len(steps))
	for _, s := range steps {
		switch step := s.(type) {
		case *ConditionBlock:
			combined := combineAnd(parent, step.Condition)
			// If the combined condition is Never, this block contributes
			// no processors at all.
			if combined != nil && combined.Kind == CondNever {
				continue
			}
			out = append(out, flattenSteps(step.Steps, combined)...)
		default:
			p, ok := s.(Processor)
			if !ok {
				// Should never happen — Step is sealed.
				continue
			}
			where := p.Base().Where
			combined := combineAnd(parent, where)
			if combined != nil && combined.Kind == CondNever {
				// Drop processor: it can never run.
				continue
			}
			out = append(out, FlatProcessor{Processor: p, Where: combined})
		}
	}
	return out
}

// combineAnd combines two optional conditions with the rules:
//   - either side nil          → return the other (or nil if both nil)
//   - either side Always       → return the other (or Always if both Always)
//   - either side Never        → return Never
//   - otherwise                → wrap as And{a, b}
func combineAnd(a, b *Condition) *Condition {
	// Treat a pure Always as "no constraint", equivalent to nil.
	if a != nil && a.Kind == CondAlways {
		a = nil
	}
	if b != nil && b.Kind == CondAlways {
		b = nil
	}
	switch {
	case a == nil && b == nil:
		return nil
	case a == nil:
		return b
	case b == nil:
		return a
	}
	if a.Kind == CondNever || b.Kind == CondNever {
		return &Condition{Kind: CondNever}
	}
	return &Condition{Kind: CondAnd, And: []Condition{*a, *b}}
}
