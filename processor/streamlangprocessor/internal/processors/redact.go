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

package processors // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/processors"

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/elastic/go-grok"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

type compiledRedact struct {
	from          string
	parsers       []*grok.Grok
	prefix        string
	suffix        string
	ignoreMissing *bool
	ignoreFailure bool
}

func compileRedact(p *dsl.RedactProcessor) (Compiled, error) {
	if p.From == "" {
		return nil, errors.New("streamlang/redact: 'from' is required")
	}
	if len(p.Patterns) == 0 {
		return nil, errors.New("streamlang/redact: 'patterns' is required")
	}
	prefix, suffix := p.Prefix, p.Suffix
	if prefix == "" {
		prefix = "<"
	}
	if suffix == "" {
		suffix = ">"
	}
	c := &compiledRedact{
		from:          p.From,
		prefix:        prefix,
		suffix:        suffix,
		ignoreMissing: p.IgnoreMissing,
		ignoreFailure: p.IgnoreFailure,
	}
	for i, pat := range p.Patterns {
		g := grok.New()
		if len(p.PatternDefinitions) > 0 {
			if err := g.AddPatterns(p.PatternDefinitions); err != nil {
				return nil, fmt.Errorf("streamlang/redact: invalid pattern_definitions: %w", err)
			}
		}
		// namedCapturesOnly=false so inline anonymous patterns also produce output.
		if err := g.Compile(pat, false); err != nil {
			return nil, fmt.Errorf("streamlang/redact: pattern %d compile: %w", i, err)
		}
		c.parsers = append(c.parsers, g)
	}
	return c, nil
}

// pickRedactCapture returns the (name, value, pos) of the capture to redact,
// or pos=-1 when none match. The selection is deterministic across runs:
// user-named captures (lowercase / mixed case) win over catalog defaults
// like IPV4 / URIPATHPARAM, and ties break by alphabetical name. Without
// this, Go's randomized map iteration causes intermittent test flakes.
func pickRedactCapture(out map[string]string, text string) (string, string, int) {
	keys := make([]string, 0, len(out))
	for k := range out {
		if k == "" || out[k] == "" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		ic := isAllUpperASCII(keys[i])
		jc := isAllUpperASCII(keys[j])
		if ic != jc {
			return !ic
		}
		return keys[i] < keys[j]
	})
	for _, name := range keys {
		value := out[name]
		pos := strings.Index(text, value)
		if pos >= 0 {
			return name, value, pos
		}
	}
	return "", "", -1
}

func isAllUpperASCII(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r == '_' || (r >= '0' && r <= '9') {
			continue
		}
		if !unicode.IsUpper(r) {
			return false
		}
	}
	return true
}

func (c *compiledRedact) Action() string      { return "redact" }
func (c *compiledRedact) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledRedact) Execute(d document.Document) error {
	v, ok := d.Get(c.from)
	if !ok {
		// redact's ignore_missing defaults true (unlike replace which defaults
		// false). Treat unset OR true as "skip silently" — matches Rust.
		if c.ignoreMissing == nil || *c.ignoreMissing {
			return nil
		}
		return fmt.Errorf("streamlang/redact: field %q not found", c.from)
	}
	text := v.Str()
	for _, g := range c.parsers {
		// Repeatedly find a non-empty named capture and substitute its
		// occurrence. Mirrors Rust runtime's redact loop.
		for {
			out, err := g.ParseString(text)
			if err != nil {
				return fmt.Errorf("streamlang/redact: parse error: %w", err)
			}
			if len(out) == 0 {
				break
			}
			hitName, hitValue, hitPos := pickRedactCapture(out, text)
			if hitPos < 0 {
				break
			}
			repl := c.prefix + hitName + c.suffix
			text = text[:hitPos] + repl + text[hitPos+len(hitValue):]
		}
	}
	return d.Set(c.from, document.StringValue(text))
}
