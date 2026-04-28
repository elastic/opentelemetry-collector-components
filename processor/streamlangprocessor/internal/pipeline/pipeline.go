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

// Package pipeline compiles a Streamlang DSL into an executable pipeline and
// runs it against batches of documents with the same per-document error
// isolation semantics as Elasticsearch ingest pipelines and ES|QL.
package pipeline // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/pipeline"

import (
	"fmt"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/condition"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/processors"
)

// Compiled is a fully prepared pipeline ready to execute against batches of
// documents. It owns precompiled regexes, conditions, and processor closures —
// none of these are recompiled on the hot path.
type Compiled struct {
	steps []compiledStep
}

type compiledStep struct {
	proc  processors.Compiled
	where condition.CompiledCondition // nil means "always"
}

// Compile compiles a parsed DSL into an executable pipeline.
func Compile(d dsl.DSL) (*Compiled, error) {
	flat := dsl.Flatten(d)
	steps := make([]compiledStep, 0, len(flat))
	for i, fp := range flat {
		cond, err := condition.Compile(fp.Where)
		if err != nil {
			return nil, fmt.Errorf("compile condition at step %d (action=%q): %w", i, fp.Processor.Action(), err)
		}
		proc, err := processors.Compile(fp.Processor)
		if err != nil {
			return nil, fmt.Errorf("compile processor at step %d (action=%q): %w", i, fp.Processor.Action(), err)
		}
		steps = append(steps, compiledStep{proc: proc, where: cond})
	}
	return &Compiled{steps: steps}, nil
}

// Steps returns the number of compiled steps (used by tests).
func (c *Compiled) Steps() int { return len(c.steps) }
