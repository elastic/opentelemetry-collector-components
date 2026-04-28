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

package condition

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

// --- mock source ---------------------------------------------------------

type mockSource struct{ m map[string]any }

func newMockSource(m map[string]any) *mockSource { return &mockSource{m: m} }

func (s *mockSource) Field(path string) (any, bool) {
	v, ok := s.m[path]
	if !ok {
		return nil, false
	}
	return v, true
}

func (s *mockSource) Has(path string) bool {
	_, ok := s.m[path]
	return ok
}

// --- helpers -------------------------------------------------------------

func filter(field string, mut func(*dsl.FilterCondition)) *dsl.Condition {
	f := &dsl.FilterCondition{Field: field}
	mut(f)
	return &dsl.Condition{Kind: dsl.CondFilter, Filter: f}
}

func mustCompile(t *testing.T, c *dsl.Condition) CompiledCondition {
	t.Helper()
	cc, err := Compile(c)
	require.NoError(t, err)
	return cc
}

func eval(cc CompiledCondition, s Source) bool {
	if cc == nil {
		return true
	}
	return cc(s)
}

// --- tests ---------------------------------------------------------------

func TestCompile_AlwaysNever(t *testing.T) {
	t.Parallel()
	always, err := Compile(&dsl.Condition{Kind: dsl.CondAlways})
	require.NoError(t, err)
	assert.Nil(t, always, "always compiles to the nil sentinel")

	never, err := Compile(&dsl.Condition{Kind: dsl.CondNever})
	require.NoError(t, err)
	require.NotNil(t, never)
	assert.False(t, never(newMockSource(nil)))

	// nil input is also "always".
	nilCC, err := Compile(nil)
	require.NoError(t, err)
	assert.Nil(t, nilCC)
}

func TestCompile_BinaryOps_Present(t *testing.T) {
	t.Parallel()
	doc := newMockSource(map[string]any{
		"status":  200,
		"message": "Hello, World",
		"flag":    true,
	})

	t.Run("eq numeric", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, filter("status", func(f *dsl.FilterCondition) {
			f.HasEq = true
			f.Eq = 200
		}))
		assert.True(t, eval(cc, doc))
	})

	t.Run("eq numeric coercion: string-200 eq 200", func(t *testing.T) {
		t.Parallel()
		s := newMockSource(map[string]any{"status": "200"})
		cc := mustCompile(t, filter("status", func(f *dsl.FilterCondition) {
			f.HasEq = true
			f.Eq = 200
		}))
		assert.True(t, eval(cc, s))
	})

	t.Run("neq present-not-equal", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, filter("status", func(f *dsl.FilterCondition) {
			f.HasNeq = true
			f.Neq = 500
		}))
		assert.True(t, eval(cc, doc))
	})

	t.Run("lt numeric coercion: string-200 lt 300", func(t *testing.T) {
		t.Parallel()
		s := newMockSource(map[string]any{"status": "200"})
		cc := mustCompile(t, filter("status", func(f *dsl.FilterCondition) {
			f.HasLt = true
			f.Lt = 300
		}))
		assert.True(t, eval(cc, s))
	})

	t.Run("gt numeric", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, filter("status", func(f *dsl.FilterCondition) {
			f.HasGt = true
			f.Gt = 100
		}))
		assert.True(t, eval(cc, doc))
	})

	t.Run("lte equal", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, filter("status", func(f *dsl.FilterCondition) {
			f.HasLte = true
			f.Lte = 200
		}))
		assert.True(t, eval(cc, doc))
	})

	t.Run("gte equal", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, filter("status", func(f *dsl.FilterCondition) {
			f.HasGte = true
			f.Gte = 200
		}))
		assert.True(t, eval(cc, doc))
	})

	t.Run("contains case-insensitive", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, filter("message", func(f *dsl.FilterCondition) {
			f.HasContain = true
			f.Contains = "hello"
		}))
		assert.True(t, eval(cc, doc))
	})

	t.Run("startsWith", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, filter("message", func(f *dsl.FilterCondition) {
			f.HasStarts = true
			f.StartsWith = "Hello"
		}))
		assert.True(t, eval(cc, doc))
	})

	t.Run("endsWith", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, filter("message", func(f *dsl.FilterCondition) {
			f.HasEnds = true
			f.EndsWith = "world"
		}))
		assert.True(t, eval(cc, doc))
	})

	t.Run("eq bool strict", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, filter("flag", func(f *dsl.FilterCondition) {
			f.HasEq = true
			f.Eq = true
		}))
		assert.True(t, eval(cc, doc))
	})
}

func TestCompile_MissingFieldSemantics(t *testing.T) {
	t.Parallel()
	empty := newMockSource(map[string]any{})

	cases := []struct {
		name string
		cond *dsl.Condition
		want bool
	}{
		{"eq missing -> false",
			filter("nope", func(f *dsl.FilterCondition) { f.HasEq = true; f.Eq = 1 }),
			false},
		{"neq missing -> true (ES semantics)",
			filter("nope", func(f *dsl.FilterCondition) { f.HasNeq = true; f.Neq = 1 }),
			true},
		{"lt missing -> false",
			filter("nope", func(f *dsl.FilterCondition) { f.HasLt = true; f.Lt = 1 }),
			false},
		{"gt missing -> false",
			filter("nope", func(f *dsl.FilterCondition) { f.HasGt = true; f.Gt = 1 }),
			false},
		{"contains missing -> false",
			filter("nope", func(f *dsl.FilterCondition) { f.HasContain = true; f.Contains = "x" }),
			false},
		{"includes missing -> false",
			filter("nope", func(f *dsl.FilterCondition) { f.HasIncl = true; f.Includes = 1 }),
			false},
		{"exists:true missing -> false",
			filter("nope", func(f *dsl.FilterCondition) { v := true; f.Exists = &v }),
			false},
		{"exists:false missing -> true",
			filter("nope", func(f *dsl.FilterCondition) { v := false; f.Exists = &v }),
			true},
		{"range missing -> false",
			filter("nope", func(f *dsl.FilterCondition) {
				f.Range = &dsl.RangeCondition{Gt: 1, Lt: 10}
			}),
			false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cc := mustCompile(t, tc.cond)
			assert.Equal(t, tc.want, eval(cc, empty))
		})
	}
}

func TestCompile_Exists(t *testing.T) {
	t.Parallel()
	doc := newMockSource(map[string]any{"a": 1})
	tT, fF := true, false

	cc := mustCompile(t, filter("a", func(f *dsl.FilterCondition) { f.Exists = &tT }))
	assert.True(t, eval(cc, doc))

	cc = mustCompile(t, filter("a", func(f *dsl.FilterCondition) { f.Exists = &fF }))
	assert.False(t, eval(cc, doc))
}

func TestCompile_Range(t *testing.T) {
	t.Parallel()
	doc := newMockSource(map[string]any{"n": 50})

	cases := []struct {
		name string
		r    *dsl.RangeCondition
		want bool
	}{
		{"gt+lt true", &dsl.RangeCondition{Gt: 10, Lt: 100}, true},
		{"gte boundary true", &dsl.RangeCondition{Gte: 50}, true},
		{"gt boundary false", &dsl.RangeCondition{Gt: 50}, false},
		{"lte boundary true", &dsl.RangeCondition{Lte: 50}, true},
		{"lt boundary false", &dsl.RangeCondition{Lt: 50}, false},
		{"out-of-range high", &dsl.RangeCondition{Gt: 60}, false},
		{"all four", &dsl.RangeCondition{Gte: 0, Lte: 100, Gt: 10, Lt: 100}, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cc := mustCompile(t, filter("n", func(f *dsl.FilterCondition) { f.Range = tc.r }))
			assert.Equal(t, tc.want, eval(cc, doc))
		})
	}
}

func TestCompile_Includes(t *testing.T) {
	t.Parallel()
	doc := newMockSource(map[string]any{
		"tags": []any{"a", "b", 200},
	})

	t.Run("includes string match", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, filter("tags", func(f *dsl.FilterCondition) {
			f.HasIncl = true
			f.Includes = "b"
		}))
		assert.True(t, eval(cc, doc))
	})

	t.Run("includes numeric coerced", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, filter("tags", func(f *dsl.FilterCondition) {
			f.HasIncl = true
			f.Includes = "200"
		}))
		assert.True(t, eval(cc, doc))
	})

	t.Run("includes miss", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, filter("tags", func(f *dsl.FilterCondition) {
			f.HasIncl = true
			f.Includes = "z"
		}))
		assert.False(t, eval(cc, doc))
	})
}

func TestCompile_AndOr_ShortCircuit(t *testing.T) {
	t.Parallel()
	doc := newMockSource(map[string]any{"a": 1, "b": 2})

	t.Run("empty and -> nil (always)", func(t *testing.T) {
		t.Parallel()
		cc, err := Compile(&dsl.Condition{Kind: dsl.CondAnd, And: nil})
		require.NoError(t, err)
		assert.Nil(t, cc)
	})

	t.Run("empty or -> always false", func(t *testing.T) {
		t.Parallel()
		cc, err := Compile(&dsl.Condition{Kind: dsl.CondOr, Or: nil})
		require.NoError(t, err)
		require.NotNil(t, cc)
		assert.False(t, cc(doc))
	})

	t.Run("and mixed true", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, &dsl.Condition{
			Kind: dsl.CondAnd,
			And: []dsl.Condition{
				*filter("a", func(f *dsl.FilterCondition) { f.HasEq = true; f.Eq = 1 }),
				*filter("b", func(f *dsl.FilterCondition) { f.HasEq = true; f.Eq = 2 }),
			},
		})
		assert.True(t, eval(cc, doc))
	})

	t.Run("and one false", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, &dsl.Condition{
			Kind: dsl.CondAnd,
			And: []dsl.Condition{
				*filter("a", func(f *dsl.FilterCondition) { f.HasEq = true; f.Eq = 1 }),
				*filter("b", func(f *dsl.FilterCondition) { f.HasEq = true; f.Eq = 99 }),
			},
		})
		assert.False(t, eval(cc, doc))
	})

	t.Run("or one true", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, &dsl.Condition{
			Kind: dsl.CondOr,
			Or: []dsl.Condition{
				*filter("a", func(f *dsl.FilterCondition) { f.HasEq = true; f.Eq = 99 }),
				*filter("b", func(f *dsl.FilterCondition) { f.HasEq = true; f.Eq = 2 }),
			},
		})
		assert.True(t, eval(cc, doc))
	})

	t.Run("or with always short-circuits", func(t *testing.T) {
		t.Parallel()
		cc, err := Compile(&dsl.Condition{
			Kind: dsl.CondOr,
			Or: []dsl.Condition{
				{Kind: dsl.CondAlways},
				*filter("a", func(f *dsl.FilterCondition) { f.HasEq = true; f.Eq = 99 }),
			},
		})
		require.NoError(t, err)
		assert.Nil(t, cc, "OR containing always collapses to always")
	})

	t.Run("and with always drops it", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, &dsl.Condition{
			Kind: dsl.CondAnd,
			And: []dsl.Condition{
				{Kind: dsl.CondAlways},
				*filter("a", func(f *dsl.FilterCondition) { f.HasEq = true; f.Eq = 1 }),
			},
		})
		require.NotNil(t, cc)
		assert.True(t, cc(doc))
	})
}

func TestCompile_Not(t *testing.T) {
	t.Parallel()
	doc := newMockSource(map[string]any{"a": 1})

	t.Run("not always -> false", func(t *testing.T) {
		t.Parallel()
		cc, err := Compile(&dsl.Condition{
			Kind: dsl.CondNot,
			Not:  &dsl.Condition{Kind: dsl.CondAlways},
		})
		require.NoError(t, err)
		require.NotNil(t, cc)
		assert.False(t, cc(doc))
	})

	t.Run("not never -> true", func(t *testing.T) {
		t.Parallel()
		cc, err := Compile(&dsl.Condition{
			Kind: dsl.CondNot,
			Not:  &dsl.Condition{Kind: dsl.CondNever},
		})
		require.NoError(t, err)
		require.NotNil(t, cc)
		assert.True(t, cc(doc))
	})

	t.Run("not filter", func(t *testing.T) {
		t.Parallel()
		cc := mustCompile(t, &dsl.Condition{
			Kind: dsl.CondNot,
			Not:  filter("a", func(f *dsl.FilterCondition) { f.HasEq = true; f.Eq = 99 }),
		})
		assert.True(t, eval(cc, doc))
	})
}

func TestCompile_NestedEndToEnd(t *testing.T) {
	t.Parallel()

	// and(or(a eq 1, b gt 10), not(message contains "drop"))
	build := func() *dsl.Condition {
		return &dsl.Condition{
			Kind: dsl.CondAnd,
			And: []dsl.Condition{
				{
					Kind: dsl.CondOr,
					Or: []dsl.Condition{
						*filter("a", func(f *dsl.FilterCondition) { f.HasEq = true; f.Eq = 1 }),
						*filter("b", func(f *dsl.FilterCondition) { f.HasGt = true; f.Gt = 10 }),
					},
				},
				{
					Kind: dsl.CondNot,
					Not: filter("message", func(f *dsl.FilterCondition) {
						f.HasContain = true
						f.Contains = "drop"
					}),
				},
			},
		}
	}

	cc := mustCompile(t, build())

	// True: a=1 satisfies OR, message doesn't contain "drop".
	docTrue := newMockSource(map[string]any{
		"a":       1,
		"b":       0,
		"message": "keep me",
	})
	assert.True(t, eval(cc, docTrue))

	// False: OR holds (b>10) but message contains "drop".
	docFalse := newMockSource(map[string]any{
		"a":       0,
		"b":       42,
		"message": "please drop",
	})
	assert.False(t, eval(cc, docFalse))

	// False: OR fails (a!=1, b<=10).
	docOrFails := newMockSource(map[string]any{
		"a":       2,
		"b":       5,
		"message": "ok",
	})
	assert.False(t, eval(cc, docOrFails))
}

func TestCompile_FilterErrors(t *testing.T) {
	t.Parallel()

	t.Run("empty field", func(t *testing.T) {
		t.Parallel()
		_, err := Compile(filter("", func(f *dsl.FilterCondition) { f.HasEq = true; f.Eq = 1 }))
		require.Error(t, err)
	})

	t.Run("zero operators", func(t *testing.T) {
		t.Parallel()
		_, err := Compile(&dsl.Condition{
			Kind:   dsl.CondFilter,
			Filter: &dsl.FilterCondition{Field: "a"},
		})
		require.Error(t, err)
	})

	t.Run("multiple operators", func(t *testing.T) {
		t.Parallel()
		_, err := Compile(filter("a", func(f *dsl.FilterCondition) {
			f.HasEq = true
			f.Eq = 1
			f.HasNeq = true
			f.Neq = 2
		}))
		require.Error(t, err)
	})
}
