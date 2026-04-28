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

// Grok library: github.com/elastic/go-grok (Elastic's official Go grok port,
// used by Beats / Elastic Agent). Verified via the Phase-2 spike: handles the
// canonical IP+method+URI patterns and typed captures (`%{NUMBER:port:int}`).
package processors // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/processors"

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/elastic/go-grok"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

type compiledGrok struct {
	from          string
	parsers       []*grok.Grok
	patterns      []string          // for error messages
	typedCaptures map[string]string // key=capture-name, value=type-suffix from %{X:name:type}
	ignoreMissing *bool
	ignoreFailure bool
}

func compileGrok(p *dsl.GrokProcessor) (Compiled, error) {
	if p.From == "" {
		return nil, errors.New("streamlang/grok: 'from' is required")
	}
	if len(p.Patterns) == 0 {
		return nil, errors.New("streamlang/grok: 'patterns' is required")
	}
	c := &compiledGrok{
		from:          p.From,
		typedCaptures: extractTypedCaptures(p.Patterns),
		ignoreMissing: p.IgnoreMissing,
		ignoreFailure: p.IgnoreFailure,
	}
	for i, pat := range p.Patterns {
		g := grok.New()
		if len(p.PatternDefinitions) > 0 {
			if err := g.AddPatterns(p.PatternDefinitions); err != nil {
				return nil, fmt.Errorf("streamlang/grok: invalid pattern_definitions: %w", err)
			}
		}
		if err := g.Compile(pat, true); err != nil {
			return nil, fmt.Errorf("streamlang/grok: pattern %d compile: %w", i, err)
		}
		c.parsers = append(c.parsers, g)
		c.patterns = append(c.patterns, pat)
	}
	return c, nil
}

func (c *compiledGrok) Action() string      { return "grok" }
func (c *compiledGrok) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledGrok) Execute(d document.Document) error {
	v, ok := d.Get(c.from)
	if !ok {
		if c.ignoreMissing != nil && *c.ignoreMissing {
			return &SkipError{Reason: "field missing"}
		}
		return fmt.Errorf("streamlang/grok: field %q not found", c.from)
	}
	src := v.Str()
	for _, g := range c.parsers {
		out, err := g.ParseString(src)
		if err != nil {
			return fmt.Errorf("streamlang/grok: parse error: %w", err)
		}
		if len(out) == 0 {
			continue
		}
		// First-match wins.
		for k, raw := range out {
			dv, err := coerceGrokCapture(raw, c.typedCaptures[k])
			if err != nil {
				return fmt.Errorf("streamlang/grok: capture %q: %w", k, err)
			}
			if err := d.Set(k, dv); err != nil {
				return fmt.Errorf("streamlang/grok: set %q: %w", k, err)
			}
		}
		return nil
	}
	return errors.New("streamlang/grok: no pattern matched")
}

// extractTypedCaptures scans patterns for `%{X:name:type}` and records the
// type per capture name. The grok library returns all captures as strings;
// we coerce based on this map.
func extractTypedCaptures(patterns []string) map[string]string {
	out := make(map[string]string)
	for _, pat := range patterns {
		for i := 0; i < len(pat); i++ {
			if pat[i] != '%' || i+1 >= len(pat) || pat[i+1] != '{' {
				continue
			}
			end := i + 2
			for end < len(pat) && pat[end] != '}' {
				end++
			}
			if end >= len(pat) {
				break
			}
			body := pat[i+2 : end]
			// body looks like "PATTERN:name:type" or "PATTERN:name" or "PATTERN"
			a := splitColon(body)
			if len(a) >= 3 {
				out[a[1]] = a[2]
			}
			i = end
		}
	}
	return out
}

func splitColon(s string) []string {
	var parts []string
	cur := ""
	for _, r := range s {
		if r == ':' {
			parts = append(parts, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	parts = append(parts, cur)
	return parts
}

func coerceGrokCapture(raw string, typeSuffix string) (document.Value, error) {
	switch typeSuffix {
	case "", "string":
		return document.StringValue(raw), nil
	case "int", "long":
		i, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			// Fall back to float-then-truncate (matches ES grok).
			f, err2 := strconv.ParseFloat(raw, 64)
			if err2 != nil {
				return document.Value{}, fmt.Errorf("cannot parse %q as integer", raw)
			}
			return document.IntValue(int64(f)), nil
		}
		return document.IntValue(i), nil
	case "float", "double":
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return document.Value{}, fmt.Errorf("cannot parse %q as float", raw)
		}
		return document.DoubleValue(f), nil
	case "bool", "boolean":
		switch raw {
		case "true", "1":
			return document.BoolValue(true), nil
		case "false", "0":
			return document.BoolValue(false), nil
		}
		return document.Value{}, fmt.Errorf("cannot parse %q as bool", raw)
	}
	return document.StringValue(raw), nil
}
