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

import "fmt"

// stepFn is a compiled processor step.
type stepFn func(doc *Document) error

// ClosureBackend is a closure-based execution engine for streamlang pipelines.
type ClosureBackend struct {
	steps      []stepFn
	processors map[string]ProcessorFunc
}

// NewClosureBackend creates a new closure backend with the given processor registry.
func NewClosureBackend(processors map[string]ProcessorFunc) *ClosureBackend {
	return &ClosureBackend{processors: processors}
}

// Compile pre-compiles a sequence of FlatSteps into a closure chain.
func (b *ClosureBackend) Compile(steps []FlatStep) error {
	b.steps = make([]stepFn, 0, len(steps))
	for i := range steps {
		fn, err := b.compileStep(&steps[i])
		if err != nil {
			return fmt.Errorf("step %d (%s): %w", i, steps[i].Proc.Action, err)
		}
		b.steps = append(b.steps, fn)
	}
	return nil
}

// Execute runs the compiled pipeline against a document.
func (b *ClosureBackend) Execute(doc *Document) error {
	for _, fn := range b.steps {
		if err := fn(doc); err != nil {
			return err
		}
	}
	return nil
}

func (b *ClosureBackend) compileStep(step *FlatStep) (stepFn, error) {
	procFn, ok := b.processors[step.Proc.Action]
	if !ok {
		return nil, fmt.Errorf("unknown processor action %q", step.Proc.Action)
	}

	proc := step.Proc
	ignoreFailure := proc.IgnoreFailure

	var base stepFn
	if ignoreFailure {
		base = func(doc *Document) error {
			_ = procFn(proc, doc)
			return nil
		}
	} else {
		base = func(doc *Document) error {
			return procFn(proc, doc)
		}
	}

	if step.Cond == nil {
		return base, nil
	}

	cond := step.Cond
	return func(doc *Document) error {
		if !EvalCondition(cond, doc) {
			return nil
		}
		return base(doc)
	}, nil
}
