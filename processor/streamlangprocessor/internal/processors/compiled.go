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

// Package processors compiles dsl.Processor values into ready-to-execute
// Compiled steps. Conditional `where` clauses are NOT evaluated here — the
// pipeline executor checks the guard and only calls Compiled.Execute when the
// guard passes (or always, if no guard).
package processors // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/processors"

import (
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
)

// Compiled is a precompiled, ready-to-execute processor.
type Compiled interface {
	Action() string
	IgnoreFailure() bool
	Execute(d document.Document) error
}

// SkipError indicates the processor decided to skip the document because a
// configured precondition was not met (e.g., ignore_missing on a missing
// source field). Skips are not failures: the executor counts them under
// "skipped" telemetry and continues with the next processor.
type SkipError struct{ Reason string }

func (e *SkipError) Error() string { return "streamlang: skipped: " + e.Reason }

// IsSkip reports whether err is a SkipError.
func IsSkip(err error) bool { _, ok := err.(*SkipError); return ok }
