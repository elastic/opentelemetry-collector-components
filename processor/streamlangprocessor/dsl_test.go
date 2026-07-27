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

package streamlangprocessor

import (
	"testing"
)

func TestDocument_GetSetRemove(t *testing.T) {
	doc := NewDocument(map[string]any{
		"a": map[string]any{
			"b": "value",
		},
	})

	// Get nested
	val, ok := doc.Get("a.b")
	if !ok || val != "value" {
		t.Fatalf("expected 'value', got %v (ok=%v)", val, ok)
	}

	// Get missing
	_, ok = doc.Get("a.c")
	if ok {
		t.Fatal("expected missing field")
	}

	// Set nested
	doc.Set("a.c", 42)
	val, ok = doc.Get("a.c")
	if !ok || val != 42 {
		t.Fatalf("expected 42, got %v", val)
	}

	// Remove
	removed := doc.Remove("a.b")
	if !removed {
		t.Fatal("expected field to be removed")
	}
	if doc.Has("a.b") {
		t.Fatal("expected field to be gone")
	}
}

func TestDocument_SetCreatesIntermediateMaps(t *testing.T) {
	doc := NewDocument(map[string]any{})
	doc.Set("x.y.z", "deep")

	val, ok := doc.Get("x.y.z")
	if !ok || val != "deep" {
		t.Fatalf("expected 'deep', got %v", val)
	}
}

func TestFlattenDSLSteps_Simple(t *testing.T) {
	steps := []Step{
		{Processor: &ProcessorDef{Action: ProcessorSet, To: "a", Value: "1"}},
		{Processor: &ProcessorDef{Action: ProcessorRemove, From: "b"}},
	}
	flat := FlattenDSLSteps(steps, nil)
	if len(flat) != 2 {
		t.Fatalf("expected 2 flat steps, got %d", len(flat))
	}
	if flat[0].Proc.Action != ProcessorSet {
		t.Fatalf("expected set, got %s", flat[0].Proc.Action)
	}
}

func TestFlattenDSLSteps_WithCondition(t *testing.T) {
	eq := any("val")
	steps := []Step{
		{Condition: &ConditionBlock{
			Condition: &Condition{Field: "f", Eq: eq},
			Steps: []Step{
				{Processor: &ProcessorDef{Action: ProcessorSet, To: "a", Value: "1"}},
			},
			Else: []Step{
				{Processor: &ProcessorDef{Action: ProcessorSet, To: "a", Value: "2"}},
			},
		}},
	}
	flat := FlattenDSLSteps(steps, nil)
	// Expected: set("1") with combined condition, set("2") with negated condition
	if len(flat) != 2 {
		t.Fatalf("expected 2 flat steps, got %d", len(flat))
	}
	if flat[0].Cond == nil {
		t.Fatal("expected condition on first step")
	}
	if flat[1].Cond == nil {
		t.Fatal("expected condition on second step")
	}
}

func TestEvalCondition_LogicalOps(t *testing.T) {
	doc := NewDocument(map[string]any{"x": 10, "y": "hello"})

	// Always
	if !EvalCondition(&Condition{Always: &struct{}{}}, doc) {
		t.Fatal("always should be true")
	}

	// Never
	if EvalCondition(&Condition{Never: &struct{}{}}, doc) {
		t.Fatal("never should be false")
	}

	// And
	eq10 := any(10)
	eqHello := any("hello")
	and := Condition{And: []Condition{
		{Field: "x", Eq: eq10},
		{Field: "y", Eq: eqHello},
	}}
	if !EvalCondition(&and, doc) {
		t.Fatal("and should be true")
	}

	// Or (first false, second true)
	eq5 := any(5)
	or := Condition{Or: []Condition{
		{Field: "x", Eq: eq5},
		{Field: "y", Eq: eqHello},
	}}
	if !EvalCondition(&or, doc) {
		t.Fatal("or should be true")
	}

	// Not
	not := Condition{Not: &Condition{Field: "x", Eq: eq5}}
	if !EvalCondition(&not, doc) {
		t.Fatal("not should be true")
	}
}

func TestEvalCondition_ComparisonOps(t *testing.T) {
	doc := NewDocument(map[string]any{"x": 10, "s": "hello"})

	tests := []struct {
		name string
		cond Condition
		want bool
	}{
		{"eq true", Condition{Field: "x", Eq: 10}, true},
		{"eq false", Condition{Field: "x", Eq: 5}, false},
		{"neq true", Condition{Field: "x", Neq: 5}, true},
		{"lt true", Condition{Field: "x", Lt: 20}, true},
		{"lt false", Condition{Field: "x", Lt: 5}, false},
		{"lte true", Condition{Field: "x", Lte: 10}, true},
		{"gt true", Condition{Field: "x", Gt: 5}, true},
		{"gte true", Condition{Field: "x", Gte: 10}, true},
		{"contains true", Condition{Field: "s", Contains: "ell"}, true},
		{"startsWith true", Condition{Field: "s", StartsWith: "hel"}, true},
		{"endsWith true", Condition{Field: "s", EndsWith: "llo"}, true},
		{"exists true", Condition{Field: "x", Exists: boolPtr(true)}, true},
		{"exists false", Condition{Field: "missing", Exists: boolPtr(true)}, false},
		{"not exists", Condition{Field: "missing", Exists: boolPtr(false)}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EvalCondition(&tt.cond, doc)
			if got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

func TestEvalCondition_Range(t *testing.T) {
	doc := NewDocument(map[string]any{"x": 10})

	gt5 := any(5)
	lt20 := any(20)
	r := Condition{Field: "x", Range: &RangeCond{Gt: gt5, Lt: lt20}}
	if !EvalCondition(&r, doc) {
		t.Fatal("range should match")
	}

	gt15 := any(15)
	r2 := Condition{Field: "x", Range: &RangeCond{Gt: gt15}}
	if EvalCondition(&r2, doc) {
		t.Fatal("range should not match")
	}
}

func TestEvalCondition_Includes(t *testing.T) {
	doc := NewDocument(map[string]any{"tags": []any{"a", "b", "c"}})

	incl := Condition{Field: "tags", Includes: "b"}
	if !EvalCondition(&incl, doc) {
		t.Fatal("includes should match")
	}

	incl2 := Condition{Field: "tags", Includes: "d"}
	if EvalCondition(&incl2, doc) {
		t.Fatal("includes should not match")
	}
}

func boolPtr(b bool) *bool { return &b }
