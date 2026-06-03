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
	"fmt"
	"strings"
)

// EvalCondition evaluates a condition tree against a document.
// Returns true if the condition matches.
func EvalCondition(cond *Condition, doc *Document) bool {
	if cond == nil {
		return true
	}

	// Special conditions
	if cond.Always != nil {
		return true
	}
	if cond.Never != nil {
		return false
	}

	// Logical operators
	if len(cond.And) > 0 {
		for i := range cond.And {
			if !EvalCondition(&cond.And[i], doc) {
				return false
			}
		}
		return true
	}
	if len(cond.Or) > 0 {
		for i := range cond.Or {
			if EvalCondition(&cond.Or[i], doc) {
				return true
			}
		}
		return false
	}
	if cond.Not != nil {
		return !EvalCondition(cond.Not, doc)
	}

	// Filter conditions require a field.
	if cond.Field == "" {
		return true
	}

	// Exists check
	if cond.Exists != nil {
		exists := doc.Exists(cond.Field)
		if *cond.Exists {
			return exists
		}
		return !exists
	}

	// Get field value
	fieldVal, ok := doc.GetAny(cond.Field)
	if !ok {
		return false
	}

	// Range condition
	if cond.Range != nil {
		return evalRange(fieldVal, cond.Range)
	}

	// Binary operators
	if cond.Eq != nil {
		return compareEq(fieldVal, cond.Eq)
	}
	if cond.Neq != nil {
		return !compareEq(fieldVal, cond.Neq)
	}
	if cond.Lt != nil {
		c := compareOrd(fieldVal, cond.Lt)
		return c < 0
	}
	if cond.Lte != nil {
		c := compareOrd(fieldVal, cond.Lte)
		return c <= 0
	}
	if cond.Gt != nil {
		c := compareOrd(fieldVal, cond.Gt)
		return c > 0
	}
	if cond.Gte != nil {
		c := compareOrd(fieldVal, cond.Gte)
		return c >= 0
	}
	if cond.Contains != nil {
		return evalContains(fieldVal, cond.Contains)
	}
	if cond.StartsWith != nil {
		return evalStartsWith(fieldVal, cond.StartsWith)
	}
	if cond.EndsWith != nil {
		return evalEndsWith(fieldVal, cond.EndsWith)
	}
	if cond.Includes != nil {
		return evalIncludes(fieldVal, cond.Includes)
	}

	return true
}

// toFloat64 converts a value to float64 if possible.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case string:
		var f float64
		_, err := fmt.Sscanf(n, "%f", &f)
		return f, err == nil
	default:
		return 0, false
	}
}

func compareEq(a, b interface{}) bool {
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// compareOrd returns -1, 0, or 1 for ordering comparison.
// Falls back to string comparison if numeric fails.
func compareOrd(a, b interface{}) int {
	fa, okA := toFloat64(a)
	fb, okB := toFloat64(b)
	if okA && okB {
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		default:
			return 0
		}
	}
	// String fallback
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	return strings.Compare(sa, sb)
}

func evalRange(val interface{}, r *RangeCond) bool {
	if r.Gt != nil && compareOrd(val, r.Gt) <= 0 {
		return false
	}
	if r.Gte != nil && compareOrd(val, r.Gte) < 0 {
		return false
	}
	if r.Lt != nil && compareOrd(val, r.Lt) >= 0 {
		return false
	}
	if r.Lte != nil && compareOrd(val, r.Lte) > 0 {
		return false
	}
	return true
}

func evalContains(fieldVal, needle interface{}) bool {
	s := fmt.Sprintf("%v", fieldVal)
	n := fmt.Sprintf("%v", needle)
	return strings.Contains(s, n)
}

func evalStartsWith(fieldVal, prefix interface{}) bool {
	s := fmt.Sprintf("%v", fieldVal)
	p := fmt.Sprintf("%v", prefix)
	return strings.HasPrefix(s, p)
}

func evalEndsWith(fieldVal, suffix interface{}) bool {
	s := fmt.Sprintf("%v", fieldVal)
	su := fmt.Sprintf("%v", suffix)
	return strings.HasSuffix(s, su)
}

func evalIncludes(fieldVal, needle interface{}) bool {
	// fieldVal should be a slice; check if needle is in it.
	if arr, ok := fieldVal.([]interface{}); ok {
		needleStr := fmt.Sprintf("%v", needle)
		for _, item := range arr {
			if fmt.Sprintf("%v", item) == needleStr {
				return true
			}
		}
		return false
	}
	// Single value: direct equality.
	return compareEq(fieldVal, needle)
}
