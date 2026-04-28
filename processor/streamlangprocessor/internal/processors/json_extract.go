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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

type compiledJSONExtract struct {
	field         string
	extractions   []compiledExtraction
	ignoreMissing *bool
	ignoreFailure bool
}

type compiledExtraction struct {
	steps       []selectorStep
	targetField string
	typeName    string // "", "keyword", "integer", "long", "double", "boolean"
}

// selectorStep is one step in a parsed dotted selector.
//   - kind=key: traverse map[string]any with name
//   - kind=index: traverse []any with idx
type selectorStep struct {
	kind selKind
	name string
	idx  int
}

type selKind int

const (
	selKey selKind = iota
	selIndex
)

func compileJSONExtract(p *dsl.JSONExtractProcessor) (Compiled, error) {
	if p.Field == "" {
		return nil, errors.New("streamlang/json_extract: 'field' is required")
	}
	if len(p.Extractions) == 0 {
		return nil, errors.New("streamlang/json_extract: 'extractions' is required")
	}
	out := make([]compiledExtraction, 0, len(p.Extractions))
	for i, e := range p.Extractions {
		steps, err := parseSelector(e.Selector)
		if err != nil {
			return nil, fmt.Errorf("streamlang/json_extract: extraction %d: %w", i, err)
		}
		out = append(out, compiledExtraction{
			steps:       steps,
			targetField: e.TargetField,
			typeName:    e.Type,
		})
	}
	return &compiledJSONExtract{
		field:         p.Field,
		extractions:   out,
		ignoreMissing: p.IgnoreMissing,
		ignoreFailure: p.IgnoreFailure,
	}, nil
}

func (c *compiledJSONExtract) Action() string      { return "json_extract" }
func (c *compiledJSONExtract) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledJSONExtract) Execute(d document.Document) error {
	v, ok := d.Get(c.field)
	if !ok {
		if c.ignoreMissing != nil && *c.ignoreMissing {
			return &SkipError{Reason: "field missing"}
		}
		return fmt.Errorf("streamlang/json_extract: field %q not found", c.field)
	}
	src := v.Str()
	if src == "" {
		// Empty input is treated as no-op (consistent with most ES processors).
		return nil
	}
	var parsed any
	if err := json.Unmarshal([]byte(src), &parsed); err != nil {
		return fmt.Errorf("streamlang/json_extract: invalid JSON in %q: %w", c.field, err)
	}

	for _, e := range c.extractions {
		val, ok := walkSelector(parsed, e.steps)
		if !ok {
			// Missing JSON path: silently skip this extraction (matches Rust runtime).
			continue
		}
		coerced, err := coerceJSONValue(val, e.typeName)
		if err != nil {
			return fmt.Errorf("streamlang/json_extract: target %q: %w", e.targetField, err)
		}
		dv, err := document.FromAny(coerced)
		if err != nil {
			return fmt.Errorf("streamlang/json_extract: target %q: %w", e.targetField, err)
		}
		if err := d.Set(e.targetField, dv); err != nil {
			return fmt.Errorf("streamlang/json_extract: set %q: %w", e.targetField, err)
		}
	}
	return nil
}

// parseSelector parses dotted-path-with-array-index selectors.
// Examples: "user.id", "$.metadata.client.ip", "items[0].name", "tags[1]".
// Rejects globs, filters, and recursive-descent (`*`, `..`).
func parseSelector(s string) ([]selectorStep, error) {
	if s == "" {
		return nil, errors.New("empty selector")
	}
	s = strings.TrimPrefix(s, "$.")
	s = strings.TrimPrefix(s, "$")
	if strings.ContainsAny(s, "*?") || strings.Contains(s, "..") {
		return nil, fmt.Errorf("unsupported selector dialect: %q", s)
	}
	var steps []selectorStep
	i := 0
	for i < len(s) {
		// Read name up to next '.' or '['
		j := i
		for j < len(s) && s[j] != '.' && s[j] != '[' {
			j++
		}
		if j > i {
			steps = append(steps, selectorStep{kind: selKey, name: s[i:j]})
			i = j
		}
		if i >= len(s) {
			break
		}
		switch s[i] {
		case '.':
			i++
		case '[':
			closeIdx := strings.IndexByte(s[i:], ']')
			if closeIdx < 0 {
				return nil, fmt.Errorf("unclosed '[' in selector %q", s)
			}
			n, err := strconv.Atoi(s[i+1 : i+closeIdx])
			if err != nil || n < 0 {
				return nil, fmt.Errorf("invalid array index in selector %q", s)
			}
			steps = append(steps, selectorStep{kind: selIndex, idx: n})
			i += closeIdx + 1
			if i < len(s) && s[i] == '.' {
				i++
			}
		}
	}
	return steps, nil
}

func walkSelector(root any, steps []selectorStep) (any, bool) {
	cur := root
	for _, st := range steps {
		switch st.kind {
		case selKey:
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, false
			}
			cur, ok = m[st.name]
			if !ok {
				return nil, false
			}
		case selIndex:
			arr, ok := cur.([]any)
			if !ok || st.idx >= len(arr) {
				return nil, false
			}
			cur = arr[st.idx]
		}
	}
	return cur, true
}

func coerceJSONValue(v any, typeName string) (any, error) {
	if typeName == "" || typeName == "keyword" {
		return jsonToString(v), nil
	}
	switch typeName {
	case "integer", "long":
		switch x := v.(type) {
		case float64:
			return int64(x), nil
		case string:
			i, err := strconv.ParseInt(x, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot parse %q as integer", x)
			}
			return i, nil
		case bool:
			if x {
				return int64(1), nil
			}
			return int64(0), nil
		}
		return nil, fmt.Errorf("cannot coerce %T to integer", v)
	case "double":
		switch x := v.(type) {
		case float64:
			return x, nil
		case string:
			f, err := strconv.ParseFloat(x, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot parse %q as double", x)
			}
			return f, nil
		}
		return nil, fmt.Errorf("cannot coerce %T to double", v)
	case "boolean":
		switch x := v.(type) {
		case bool:
			return x, nil
		case string:
			switch strings.ToLower(x) {
			case "true", "1":
				return true, nil
			case "false", "0":
				return false, nil
			}
			return nil, fmt.Errorf("cannot parse %q as boolean", x)
		case float64:
			return x != 0, nil
		}
		return nil, fmt.Errorf("cannot coerce %T to boolean", v)
	}
	return nil, fmt.Errorf("unknown extraction type %q", typeName)
}

func jsonToString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		// Trim trailing .0 for integer-valued floats.
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		if x {
			return "true"
		}
		return "false"
	case nil:
		return ""
	}
	b, _ := json.Marshal(v)
	return string(b)
}
