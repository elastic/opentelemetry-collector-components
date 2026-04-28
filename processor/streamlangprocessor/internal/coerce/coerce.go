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

// Package coerce provides numeric/string coercion helpers shared by the
// condition evaluator and the processors. The semantics mirror the Rust
// reference implementation in streamlang-runtime/src/value.rs and the
// Elasticsearch-ingest behaviour for cross-target consistency.
package coerce // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/coerce"

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"strings"
)

// Equal reports whether a and b are equal under Streamlang/ES-ingest rules.
//
//   - Numeric coercion: string<->number compares numerically when the string
//     parses. Examples: Equal("200", 200) == true; Equal("200.0", 200) == true;
//     Equal("200x", 200) == false.
//   - bool equality is strict (no string coercion).
//   - nil equals nil. nil != anything else.
//   - slices/maps: structural deep-equal.
func Equal(a, b any) bool {
	a = unwrapSingleSlice(a)
	b = unwrapSingleSlice(b)

	if a == nil || b == nil {
		return a == nil && b == nil
	}

	// Strict bool equality — no string<->bool coercion.
	if ab, aok := a.(bool); aok {
		if bb, bok := b.(bool); bok {
			return ab == bb
		}
		return false
	}
	if _, bok := b.(bool); bok {
		// b is bool, a isn't — already handled above means a is not bool.
		return false
	}

	// Try numeric equality if either side is a number, or if both sides
	// parse as numbers.
	an, aIsNum := toFloat64Strict(a)
	bn, bIsNum := toFloat64Strict(b)
	if aIsNum || bIsNum {
		// If either side is numeric, attempt to coerce the other.
		af, aok := ToFloat64(a)
		bf, bok := ToFloat64(b)
		if aok && bok {
			return af == bf
		}
		// One side numeric, other not coercible: not equal (unless both
		// numeric handled above; this branch implies failure).
		_ = an
		_ = bn
	}

	// Strings: direct compare.
	if as, aok := a.(string); aok {
		if bs, bok := b.(string); bok {
			return as == bs
		}
		return false
	}
	if _, bok := b.(string); bok {
		return false
	}

	// Slices / maps / anything else: structural deep equal.
	return reflect.DeepEqual(a, b)
}

// Compare returns -1, 0, +1 for ordered comparison; ok=false if comparison is
// not meaningful (e.g., comparing a slice to a number).
//
//   - Numbers compared numerically (int64 vs float64 mixes promote to float64).
//   - One numeric + one numeric-string -> numeric.
//   - String vs string -> lexicographic.
//   - bool: false<true.
func Compare(a, b any) (int, bool) {
	a = unwrapSingleSlice(a)
	b = unwrapSingleSlice(b)

	if a == nil || b == nil {
		return 0, false
	}

	// bool vs bool.
	if ab, aok := a.(bool); aok {
		if bb, bok := b.(bool); bok {
			switch {
			case ab == bb:
				return 0, true
			case !ab && bb:
				return -1, true
			default:
				return 1, true
			}
		}
		return 0, false
	}
	if _, bok := b.(bool); bok {
		return 0, false
	}

	// Numeric path: if both coerce to float, compare numerically.
	af, aok := ToFloat64(a)
	bf, bok := ToFloat64(b)
	if aok && bok {
		switch {
		case af < bf:
			return -1, true
		case af > bf:
			return 1, true
		default:
			return 0, true
		}
	}

	// Both strings -> lexicographic.
	if as, aok := a.(string); aok {
		if bs, bok := b.(string); bok {
			return strings.Compare(as, bs), true
		}
	}

	return 0, false
}

// Stringify returns a string representation suitable for `contains`,
// `startsWith`, `endsWith`. nil returns "".
func Stringify(v any) string {
	v = unwrapSingleSlice(v)
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int:
		return strconv.FormatInt(int64(t), 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case uint:
		return strconv.FormatUint(uint64(t), 10)
	case uint32:
		return strconv.FormatUint(uint64(t), 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case float32:
		return formatFloat(float64(t))
	case float64:
		return formatFloat(t)
	case json.Number:
		// Trim trailing .0 so int-valued floats print as int.
		s := t.String()
		if strings.HasSuffix(s, ".0") {
			return strings.TrimSuffix(s, ".0")
		}
		return s
	}
	// Fallback for slices/maps/etc — Go default formatting via fmt is
	// undesirable here; just use reflect-aware fallback.
	if b, err := json.Marshal(v); err == nil {
		return string(b)
	}
	return ""
}

// ToFloat64 attempts to coerce v to a float64.
func ToFloat64(v any) (float64, bool) {
	v = unwrapSingleSlice(v)
	switch t := v.(type) {
	case nil:
		return 0, false
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int32:
		return float64(t), true
	case int64:
		return float64(t), true
	case uint:
		return float64(t), true
	case uint32:
		return float64(t), true
	case uint64:
		return float64(t), true
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

// ToInt64 attempts to coerce v to an int64. From a float, only succeeds when
// the value is integer-valued and within 2^53 (safe-integer range).
func ToInt64(v any) (int64, bool) {
	v = unwrapSingleSlice(v)
	switch t := v.(type) {
	case nil:
		return 0, false
	case int:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case uint:
		if uint64(t) > math.MaxInt64 {
			return 0, false
		}
		return int64(t), true
	case uint32:
		return int64(t), true
	case uint64:
		if t > math.MaxInt64 {
			return 0, false
		}
		return int64(t), true
	case float32:
		return floatToInt64(float64(t))
	case float64:
		return floatToInt64(t)
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, false
		}
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i, true
		}
		// Try float path — accepts "200.0".
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return floatToInt64(f)
		}
		return 0, false
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, true
		}
		if f, err := t.Float64(); err == nil {
			return floatToInt64(f)
		}
		return 0, false
	}
	return 0, false
}

// ToString — best-effort string conversion: strings as-is, numbers via
// strconv (no scientific for ints), bools as "true"/"false", nil as "".
func ToString(v any) string {
	return Stringify(v)
}

// ToBool — accepts bool, "true"/"false" (case-insensitive), 1/0, "1"/"0".
func ToBool(v any) (bool, bool) {
	v = unwrapSingleSlice(v)
	switch t := v.(type) {
	case bool:
		return t, true
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "true", "1":
			return true, true
		case "false", "0":
			return false, true
		}
		return false, false
	case int:
		return numericToBool(int64(t))
	case int32:
		return numericToBool(int64(t))
	case int64:
		return numericToBool(t)
	case uint:
		return numericToBool(int64(t))
	case uint32:
		return numericToBool(int64(t))
	case uint64:
		if t > math.MaxInt64 {
			return false, false
		}
		return numericToBool(int64(t))
	case float32:
		return floatToBool(float64(t))
	case float64:
		return floatToBool(t)
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return numericToBool(i)
		}
		if f, err := t.Float64(); err == nil {
			return floatToBool(f)
		}
		return false, false
	}
	return false, false
}

// --- internal helpers -----------------------------------------------------

// toFloat64Strict reports whether v is *natively* a numeric type (not a
// string that happens to parse).
func toFloat64Strict(v any) (float64, bool) {
	switch t := v.(type) {
	case float32:
		return float64(t), true
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int32:
		return float64(t), true
	case int64:
		return float64(t), true
	case uint:
		return float64(t), true
	case uint32:
		return float64(t), true
	case uint64:
		return float64(t), true
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}

// unwrapSingleSlice returns the element when v is a one-element []any,
// matching the Rust reference's `unwrap_single_array`. Other slice shapes
// pass through unchanged.
func unwrapSingleSlice(v any) any {
	if s, ok := v.([]any); ok && len(s) == 1 {
		return s[0]
	}
	return v
}

func formatFloat(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	// Print integer-valued floats without a trailing ".0", matching the
	// Rust reference's `to_string_val` for serde Number.
	if f == math.Trunc(f) && math.Abs(f) < 1e16 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func floatToInt64(f float64) (int64, bool) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	if f != math.Trunc(f) {
		return 0, false
	}
	const maxSafe = 1 << 53
	if math.Abs(f) >= maxSafe {
		return 0, false
	}
	return int64(f), true
}

func numericToBool(n int64) (bool, bool) {
	switch n {
	case 0:
		return false, true
	case 1:
		return true, true
	}
	return false, false
}

func floatToBool(f float64) (bool, bool) {
	if f == 0 {
		return false, true
	}
	if f == 1 {
		return true, true
	}
	return false, false
}
