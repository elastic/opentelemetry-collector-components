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
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

// Phase-1 TinyMath subset:
//
//	expr   := term  (('+' | '-') term)*
//	term   := factor (('*' | '/') factor)*
//	factor := '-' factor | '(' expr ')' | number | field_ref
//
// No functions, no constants. Adding pow/sqrt/etc. is Phase-6 work.
type compiledMath struct {
	root          mathNode
	to            string
	ignoreMissing *bool
	ignoreFailure bool
}

type mathNode interface {
	eval(d document.Document) (float64, error)
}

type mathNum struct{ v float64 }
type mathField struct{ path string }
type mathUnary struct {
	op    byte
	child mathNode
}
type mathBinary struct {
	op       byte
	lhs, rhs mathNode
}

// errMissingField is returned by mathField.eval when a field reference is
// missing. The processor wraps it in SkipError if ignore_missing is set.
type errMissingField struct{ path string }

func (e *errMissingField) Error() string { return "missing field: " + e.path }

func compileMath(p *dsl.MathProcessor) (Compiled, error) {
	if p.Expression == "" {
		return nil, errors.New("streamlang/math: 'expression' is required")
	}
	if p.To == "" {
		return nil, errors.New("streamlang/math: 'to' is required")
	}
	root, err := parseMath(p.Expression)
	if err != nil {
		return nil, fmt.Errorf("streamlang/math: %w", err)
	}
	return &compiledMath{
		root:          root,
		to:            p.To,
		ignoreMissing: p.IgnoreMissing,
		ignoreFailure: p.IgnoreFailure,
	}, nil
}

func (c *compiledMath) Action() string      { return "math" }
func (c *compiledMath) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledMath) Execute(d document.Document) error {
	v, err := c.root.eval(d)
	if err != nil {
		var miss *errMissingField
		if errors.As(err, &miss) {
			if c.ignoreMissing != nil && *c.ignoreMissing {
				return &SkipError{Reason: "field missing: " + miss.path}
			}
		}
		return fmt.Errorf("streamlang/math: %w", err)
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Errorf("streamlang/math: result is NaN/Inf")
	}
	if v == math.Trunc(v) && math.Abs(v) < (1<<53) {
		return d.Set(c.to, document.IntValue(int64(v)))
	}
	return d.Set(c.to, document.DoubleValue(v))
}

// --- evaluator ---

func (n *mathNum) eval(_ document.Document) (float64, error) { return n.v, nil }

func (n *mathField) eval(d document.Document) (float64, error) {
	v, ok := d.Get(n.path)
	if !ok {
		return 0, &errMissingField{path: n.path}
	}
	switch v.Type() {
	case document.ValueTypeInt:
		return float64(v.Int()), nil
	case document.ValueTypeDouble:
		return v.Double(), nil
	case document.ValueTypeBool:
		if v.Bool() {
			return 1, nil
		}
		return 0, nil
	case document.ValueTypeStr:
		f, err := strconv.ParseFloat(v.Str(), 64)
		if err != nil {
			return 0, fmt.Errorf("field %q is not numeric: %v", n.path, v.Str())
		}
		return f, nil
	}
	return 0, fmt.Errorf("field %q is not numeric", n.path)
}

func (n *mathUnary) eval(d document.Document) (float64, error) {
	v, err := n.child.eval(d)
	if err != nil {
		return 0, err
	}
	if n.op == '-' {
		return -v, nil
	}
	return v, nil
}

func (n *mathBinary) eval(d document.Document) (float64, error) {
	l, err := n.lhs.eval(d)
	if err != nil {
		return 0, err
	}
	r, err := n.rhs.eval(d)
	if err != nil {
		return 0, err
	}
	switch n.op {
	case '+':
		return l + r, nil
	case '-':
		return l - r, nil
	case '*':
		return l * r, nil
	case '/':
		if r == 0 {
			return 0, errors.New("division by zero")
		}
		return l / r, nil
	}
	return 0, fmt.Errorf("unknown operator %c", n.op)
}

// --- parser (recursive descent) ---

type mathParser struct {
	s   string
	pos int
}

func parseMath(s string) (mathNode, error) {
	p := &mathParser{s: s}
	n, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	p.skipWS()
	if p.pos != len(p.s) {
		return nil, fmt.Errorf("unexpected trailing input at %d: %q", p.pos, p.s[p.pos:])
	}
	return n, nil
}

func (p *mathParser) parseExpr() (mathNode, error) {
	lhs, err := p.parseTerm()
	if err != nil {
		return nil, err
	}
	for {
		p.skipWS()
		if p.pos >= len(p.s) {
			return lhs, nil
		}
		op := p.s[p.pos]
		if op != '+' && op != '-' {
			return lhs, nil
		}
		p.pos++
		rhs, err := p.parseTerm()
		if err != nil {
			return nil, err
		}
		lhs = &mathBinary{op: op, lhs: lhs, rhs: rhs}
	}
}

func (p *mathParser) parseTerm() (mathNode, error) {
	lhs, err := p.parseFactor()
	if err != nil {
		return nil, err
	}
	for {
		p.skipWS()
		if p.pos >= len(p.s) {
			return lhs, nil
		}
		op := p.s[p.pos]
		if op != '*' && op != '/' {
			return lhs, nil
		}
		p.pos++
		rhs, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		lhs = &mathBinary{op: op, lhs: lhs, rhs: rhs}
	}
}

func (p *mathParser) parseFactor() (mathNode, error) {
	p.skipWS()
	if p.pos >= len(p.s) {
		return nil, errors.New("unexpected end of expression")
	}
	c := p.s[p.pos]
	switch {
	case c == '-':
		p.pos++
		child, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		return &mathUnary{op: '-', child: child}, nil
	case c == '+':
		p.pos++
		return p.parseFactor()
	case c == '(':
		p.pos++
		n, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		p.skipWS()
		if p.pos >= len(p.s) || p.s[p.pos] != ')' {
			return nil, errors.New("missing ')'")
		}
		p.pos++
		return n, nil
	case c >= '0' && c <= '9' || c == '.':
		return p.parseNumber()
	case isMathIdentStart(rune(c)):
		return p.parseFieldRef()
	}
	return nil, fmt.Errorf("unexpected character %q at %d", c, p.pos)
}

func (p *mathParser) parseNumber() (mathNode, error) {
	start := p.pos
	for p.pos < len(p.s) && (isDigit(p.s[p.pos]) || p.s[p.pos] == '.') {
		p.pos++
	}
	v, err := strconv.ParseFloat(p.s[start:p.pos], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number %q", p.s[start:p.pos])
	}
	return &mathNum{v: v}, nil
}

func (p *mathParser) parseFieldRef() (mathNode, error) {
	start := p.pos
	for p.pos < len(p.s) {
		c := rune(p.s[p.pos])
		if !isMathIdentStart(c) && !isDigit(p.s[p.pos]) && c != '.' && c != '_' {
			break
		}
		p.pos++
	}
	path := p.s[start:p.pos]
	if path == "" {
		return nil, errors.New("empty field reference")
	}
	return &mathField{path: strings.TrimSuffix(path, ".")}, nil
}

func (p *mathParser) skipWS() {
	for p.pos < len(p.s) && unicode.IsSpace(rune(p.s[p.pos])) {
		p.pos++
	}
}

func isDigit(b byte) bool          { return b >= '0' && b <= '9' }
func isMathIdentStart(c rune) bool { return unicode.IsLetter(c) || c == '_' }
