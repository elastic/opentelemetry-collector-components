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

package coerce

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEqual(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		a, b any
		want bool
	}{
		{"string-int numeric coerce", "200", 200, true},
		{"string-with-decimal vs int", "200.0", 200, true},
		{"int vs float same value", 200, 200.0, true},
		{"non-numeric string vs int", "xyz", 0, false},
		{"trailing chars don't parse", "200x", 200, false},
		{"nil vs nil", nil, nil, true},
		{"nil vs zero", nil, 0, false},
		{"zero vs nil", 0, nil, false},
		{"bool true vs bool true", true, true, true},
		{"bool true vs bool false", true, false, false},
		{"bool true vs string 'true' — strict", "true", true, false},
		{"bool true vs 1 — strict", true, 1, false},
		{"slice deep equal", []any{1, 2}, []any{1, 2}, true},
		{"slice mismatch", []any{1, 2}, []any{1, 3}, false},
		{"map deep equal", map[string]any{"a": 1}, map[string]any{"a": 1}, true},
		{"single-element slice unwraps", []any{200}, "200", true},
		{"string vs string", "abc", "abc", true},
		{"string vs string differ", "abc", "abd", false},
		{"json.Number vs int", json.Number("200"), 200, true},
		{"int64 vs float64 same", int64(5), float64(5), true},
		{"float vs string match", 1.5, "1.5", true},
		{"bool vs nil", true, nil, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, Equal(tc.a, tc.b),
				"Equal(%#v, %#v)", tc.a, tc.b)
		})
	}
}

func TestCompare(t *testing.T) {
	t.Parallel()
	type result struct {
		ord int
		ok  bool
	}
	cases := []struct {
		name string
		a, b any
		want result
	}{
		{"int less", 1, 2, result{-1, true}},
		{"int equal", 5, 5, result{0, true}},
		{"int greater", 9, 2, result{1, true}},
		{"numeric string greater", "10", 2, result{1, true}},
		{"numeric string vs float", "1.5", 1.0, result{1, true}},
		{"string lexical less", "apple", "banana", result{-1, true}},
		{"string lexical equal", "abc", "abc", result{0, true}},
		{"string vs int not coercible", "a", 1, result{0, false}},
		{"int vs nil", 1, nil, result{0, false}},
		{"nil vs nil", nil, nil, result{0, false}},
		{"bool false<true", false, true, result{-1, true}},
		{"bool true=true", true, true, result{0, true}},
		{"bool vs int not ok", true, 1, result{0, false}},
		{"int64 vs float64 same", int64(200), float64(200.0), result{0, true}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ord, ok := Compare(tc.a, tc.b)
			assert.Equal(t, tc.want.ok, ok, "Compare(%#v, %#v) ok", tc.a, tc.b)
			if tc.want.ok {
				assert.Equal(t, tc.want.ord, ord, "Compare(%#v, %#v) ord", tc.a, tc.b)
			}
		})
	}
}

func TestStringify(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"int-valued float trims .0", 200.0, "200"},
		{"non-int float", 200.5, "200.5"},
		{"true", true, "true"},
		{"false", false, "false"},
		{"nil empty", nil, ""},
		{"int", 42, "42"},
		{"int64", int64(42), "42"},
		{"string passthrough", "hello", "hello"},
		{"json.Number int", json.Number("42"), "42"},
		{"json.Number trailing .0", json.Number("200.0"), "200"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, Stringify(tc.in))
		})
	}
}

func TestToFloat64(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		in     any
		want   float64
		wantOk bool
	}{
		{"float", 1.5, 1.5, true},
		{"int", 42, 42, true},
		{"int64", int64(42), 42, true},
		{"string number", "1.25", 1.25, true},
		{"string non-number", "abc", 0, false},
		{"bool true", true, 1, true},
		{"bool false", false, 0, true},
		{"nil", nil, 0, false},
		{"json.Number", json.Number("3.14"), 3.14, true},
		{"empty string", "", 0, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := ToFloat64(tc.in)
			assert.Equal(t, tc.wantOk, ok)
			if tc.wantOk {
				assert.InDelta(t, tc.want, got, 1e-9)
			}
		})
	}
}

func TestToInt64(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		in     any
		want   int64
		wantOk bool
	}{
		{"int64 passthrough", int64(42), 42, true},
		{"int", 42, 42, true},
		{"int-valued float", 200.0, 200, true},
		{"non-int float fails", 200.5, 0, false},
		{"string int", "200", 200, true},
		{"string float-int", "200.0", 200, true},
		{"string non-numeric", "abc", 0, false},
		{"bool true", true, 1, true},
		{"bool false", false, 0, true},
		{"nil", nil, 0, false},
		{"json.Number int", json.Number("42"), 42, true},
		{"too big float", 1e20, 0, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := ToInt64(tc.in)
			assert.Equal(t, tc.wantOk, ok)
			if tc.wantOk {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func TestToBool(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		in     any
		want   bool
		wantOk bool
	}{
		{"bool true", true, true, true},
		{"bool false", false, false, true},
		{"string true", "true", true, true},
		{"string TRUE caseless", "TRUE", true, true},
		{"string false", "false", false, true},
		{"string 1", "1", true, true},
		{"string 0", "0", false, true},
		{"string yes", "yes", false, false},
		{"int 1", 1, true, true},
		{"int 0", 0, false, true},
		{"int 2 — not coercible", 2, false, false},
		{"float 1.0", 1.0, true, true},
		{"float 0.0", 0.0, false, true},
		{"float 0.5 — not coercible", 0.5, false, false},
		{"nil", nil, false, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := ToBool(tc.in)
			assert.Equal(t, tc.wantOk, ok)
			if tc.wantOk {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func TestToString(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "abc", ToString("abc"))
	assert.Equal(t, "true", ToString(true))
	assert.Equal(t, "200", ToString(200))
	assert.Equal(t, "200", ToString(200.0))
	assert.Equal(t, "", ToString(nil))
}
