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

// Hand-port of the Rust streamlang-runtime dissect parser. Supports
// %{key}, %{?skip}, %{+name} (append), and %{name->} (right pad). More
// elaborate modifiers (numeric ordering, named-skip groups) are Phase-6.
package processors // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/processors"

import (
	"errors"
	"fmt"
	"strings"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

type dissectStep struct {
	key       string
	append    bool
	rightPad  bool
	delimiter string // text after this key, before next %{ or end
}

type compiledDissect struct {
	from            string
	steps           []dissectStep
	leadingDelim    string
	appendSeparator string
	ignoreMissing   *bool
	ignoreFailure   bool
}

func compileDissect(p *dsl.DissectProcessor) (Compiled, error) {
	if p.From == "" {
		return nil, errors.New("streamlang/dissect: 'from' is required")
	}
	if p.Pattern == "" {
		return nil, errors.New("streamlang/dissect: 'pattern' is required")
	}
	leading, steps, err := parseDissectPattern(p.Pattern)
	if err != nil {
		return nil, fmt.Errorf("streamlang/dissect: %w", err)
	}
	return &compiledDissect{
		from:            p.From,
		leadingDelim:    leading,
		steps:           steps,
		appendSeparator: p.AppendSeparator,
		ignoreMissing:   p.IgnoreMissing,
		ignoreFailure:   p.IgnoreFailure,
	}, nil
}

func (c *compiledDissect) Action() string      { return "dissect" }
func (c *compiledDissect) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledDissect) Execute(d document.Document) error {
	v, ok := d.Get(c.from)
	if !ok {
		if c.ignoreMissing != nil && *c.ignoreMissing {
			return &SkipError{Reason: "field missing"}
		}
		return fmt.Errorf("streamlang/dissect: field %q not found", c.from)
	}
	text := v.Str()

	pos := 0
	if c.leadingDelim != "" {
		if !strings.HasPrefix(text, c.leadingDelim) {
			return fmt.Errorf("streamlang/dissect: leading delimiter %q not found", c.leadingDelim)
		}
		pos += len(c.leadingDelim)
	}

	results := make(map[string]string)
	for i, step := range c.steps {
		var value string
		switch {
		case step.delimiter == "" && i == len(c.steps)-1:
			// Last key, no trailing delimiter: take rest.
			value = text[pos:]
			pos = len(text)
		case step.delimiter != "":
			end := strings.Index(text[pos:], step.delimiter)
			if end < 0 {
				return fmt.Errorf("streamlang/dissect: delimiter %q not found", step.delimiter)
			}
			value = text[pos : pos+end]
			if step.rightPad {
				value = strings.TrimRight(value, " ")
			}
			pos += end + len(step.delimiter)
		default:
			// Two adjacent keys with no delimiter between them — ambiguous.
			return errors.New("streamlang/dissect: adjacent keys without delimiter")
		}

		if step.key == "" || step.key == "?" {
			continue // skip
		}
		if step.append {
			if existing, ok := results[step.key]; ok {
				results[step.key] = existing + c.appendSeparator + value
			} else {
				results[step.key] = value
			}
		} else {
			results[step.key] = value
		}
	}

	for k, v := range results {
		if err := d.Set(k, document.StringValue(v)); err != nil {
			return fmt.Errorf("streamlang/dissect: set %q: %w", k, err)
		}
	}
	return nil
}

func parseDissectPattern(pat string) (leading string, steps []dissectStep, err error) {
	i := 0
	// Read leading literal text up to first '%{'.
	for i < len(pat) {
		if pat[i] == '%' && i+1 < len(pat) && pat[i+1] == '{' {
			break
		}
		leading += string(pat[i])
		i++
	}

	for i < len(pat) {
		if pat[i] != '%' || i+1 >= len(pat) || pat[i+1] != '{' {
			return "", nil, fmt.Errorf("expected %%{ at position %d", i)
		}
		i += 2 // past %{
		end := strings.IndexByte(pat[i:], '}')
		if end < 0 {
			return "", nil, errors.New("unclosed %{")
		}
		raw := pat[i : i+end]
		i += end + 1

		key, isAppend, isRightPad := parseDissectKeyModifiers(raw)

		// Read delimiter up to next %{ or end.
		delimEnd := i
		for delimEnd < len(pat) {
			if pat[delimEnd] == '%' && delimEnd+1 < len(pat) && pat[delimEnd+1] == '{' {
				break
			}
			delimEnd++
		}
		delim := pat[i:delimEnd]
		i = delimEnd

		steps = append(steps, dissectStep{
			key:       key,
			append:    isAppend,
			rightPad:  isRightPad,
			delimiter: delim,
		})
	}
	return leading, steps, nil
}

func parseDissectKeyModifiers(raw string) (string, bool, bool) {
	key := raw
	var isAppend, isRightPad bool
	if strings.HasPrefix(key, "+") {
		isAppend = true
		key = key[1:]
	}
	if strings.HasSuffix(key, "->") {
		isRightPad = true
		key = key[:len(key)-2]
	}
	return key, isAppend, isRightPad
}
