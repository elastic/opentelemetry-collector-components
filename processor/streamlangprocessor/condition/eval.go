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

// Package condition compiles and evaluates Streamlang conditions against a
// minimal Source interface that the document package satisfies structurally.
//
// The package deliberately does not import internal/document — that package
// imports here for processor evaluation, and we avoid the cycle by abstracting
// over the field-access surface. Any document.Document that exposes
// `Get(string) (Value, bool)` and `Has(string) bool` (where the returned
// Value has an `AsAny() any` method) plugs in.
package condition // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/condition"

// Source is the minimal field-access API needed to evaluate a condition.
// document.Document satisfies it directly via the Field method, so the
// pipeline executor can pass a Document with no adapter allocation per Get.
type Source interface {
	// Field returns the value at the dotted path as an `any` (string, int64,
	// float64, bool, []any, map[string]any, or nil). The second return is
	// false if the path doesn't resolve.
	Field(path string) (any, bool)
	// Has reports whether the dotted path resolves.
	Has(path string) bool
}

// CompiledCondition is the result of compiling a dsl.Condition. A nil
// CompiledCondition is the hot-path sentinel meaning "always true" — callers
// should pre-check for nil and skip the call.
type CompiledCondition func(s Source) bool
