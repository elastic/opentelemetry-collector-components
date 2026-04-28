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

package dsl // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"

import (
	"encoding/json"
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
)

// known field names shared across many processors.
var commonProcessorFields = []string{
	"action", "customIdentifier", "description", "ignore_failure", "where",
}

// Parse decodes a user-supplied configuration into a DSL AST.
//
// The accepted shapes are:
//   - []any / []map[string]any — a bare list of steps (top-level `steps:` already unwrapped).
//   - map[string]any with a `steps` key — the full DSL document.
func Parse(raw any) (DSL, error) {
	steps, err := extractStepsList(raw)
	if err != nil {
		return DSL{}, err
	}
	parsed, err := parseSteps(steps)
	if err != nil {
		return DSL{}, err
	}
	return DSL{Steps: parsed}, nil
}

// ParseYAML decodes a YAML byte slice and parses it as a DSL.
func ParseYAML(b []byte) (DSL, error) {
	var raw any
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return DSL{}, fmt.Errorf("yaml: %w", err)
	}
	raw = normalizeYAML(raw)
	return Parse(raw)
}

// ParseJSON decodes a JSON byte slice and parses it as a DSL.
func ParseJSON(b []byte) (DSL, error) {
	var raw any
	if err := json.Unmarshal(b, &raw); err != nil {
		return DSL{}, fmt.Errorf("json: %w", err)
	}
	return Parse(raw)
}

// ParseCondition parses a single Condition map (the same shape as a `where`
// clause body or a routing-rule predicate) into an AST Condition. Use this
// when a caller needs to compile a standalone condition outside of a full
// pipeline (e.g. routing-rule predicates in the runtime's graph compiler).
func ParseCondition(m map[string]any) (*Condition, error) {
	return parseCondition(m)
}

// extractStepsList accepts the top-level config value and returns the
// list of step maps.
func extractStepsList(raw any) ([]any, error) {
	switch v := raw.(type) {
	case []any:
		return v, nil
	case []map[string]any:
		out := make([]any, len(v))
		for i, m := range v {
			out[i] = m
		}
		return out, nil
	case map[string]any:
		stepsRaw, ok := v["steps"]
		if !ok {
			return nil, fmt.Errorf("DSL document missing required key %q", "steps")
		}
		return extractStepsList(stepsRaw)
	default:
		return nil, fmt.Errorf("DSL must be a list of steps or an object with a 'steps' key, got %T", raw)
	}
}

func parseSteps(items []any) ([]Step, error) {
	out := make([]Step, 0, len(items))
	for i, item := range items {
		m, err := asStringMap(item)
		if err != nil {
			return nil, fmt.Errorf("at step %d: %w", i, err)
		}
		step, err := parseStep(m)
		if err != nil {
			action, _ := stringField(m, "action")
			return nil, fmt.Errorf("at step %d (action=%q): %w", i, action, err)
		}
		out = append(out, step)
	}
	return out, nil
}

func parseStep(m map[string]any) (Step, error) {
	_, hasAction := m["action"]
	_, hasCondition := m["condition"]

	switch {
	case hasAction && !hasCondition:
		return parseProcessor(m)
	case hasCondition && !hasAction:
		return parseConditionBlock(m)
	case hasAction && hasCondition:
		return nil, fmt.Errorf("step has both 'action' and 'condition' keys; pick one")
	default:
		return nil, fmt.Errorf("step must have either 'action' or 'condition'")
	}
}

func parseConditionBlock(m map[string]any) (*ConditionBlock, error) {
	if extras := unknownKeys(m, "condition", "customIdentifier"); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s) in condition block: %v", extras)
	}
	condRaw, _ := m["condition"]
	condMap, err := asStringMap(condRaw)
	if err != nil {
		return nil, fmt.Errorf("condition: %w", err)
	}

	stepsRaw, ok := condMap["steps"]
	if !ok {
		return nil, fmt.Errorf("condition block missing 'steps'")
	}
	stepsList, err := asAnyList(stepsRaw)
	if err != nil {
		return nil, fmt.Errorf("condition.steps: %w", err)
	}

	// The remaining keys form the actual condition body.
	condBody := make(map[string]any, len(condMap)-1)
	for k, v := range condMap {
		if k == "steps" {
			continue
		}
		condBody[k] = v
	}
	cond, err := parseCondition(condBody)
	if err != nil {
		return nil, fmt.Errorf("condition: %w", err)
	}

	innerSteps, err := parseSteps(stepsList)
	if err != nil {
		return nil, err
	}

	cb := &ConditionBlock{
		Condition: cond,
		Steps:     innerSteps,
	}
	if id, ok := optionalString(m, "customIdentifier"); ok {
		cb.CustomIdentifier = id
	}
	return cb, nil
}

// parseProcessor dispatches on the action key.
func parseProcessor(m map[string]any) (Processor, error) {
	action, ok := stringField(m, "action")
	if !ok {
		return nil, fmt.Errorf("'action' must be a non-empty string")
	}
	switch action {
	case "set":
		return parseSet(m)
	case "append":
		return parseAppend(m)
	case "remove":
		return parseRemove(m)
	case "remove_by_prefix":
		return parseRemoveByPrefix(m)
	case "rename":
		return parseRename(m)
	case "drop_document":
		return parseDropDocument(m)
	case "uppercase":
		return parseUppercase(m)
	case "lowercase":
		return parseLowercase(m)
	case "trim":
		return parseTrim(m)
	case "replace":
		return parseReplace(m)
	case "convert":
		return parseConvert(m)
	case "grok":
		return parseGrok(m)
	case "dissect":
		return parseDissect(m)
	case "date":
		return parseDate(m)
	case "redact":
		return parseRedact(m)
	case "math":
		return parseMath(m)
	case "json_extract":
		return parseJSONExtract(m)
	case "network_direction":
		return parseNetworkDirection(m)
	case "split":
		return parseSplit(m)
	case "sort":
		return parseSort(m)
	case "join":
		return parseJoin(m)
	case "concat":
		return parseConcat(m)
	}

	// Recognise (but reject) the actions that exist in the schema but are not yet
	// implemented in this parser.
	switch action {
	case "enrich", "manual_ingest_pipeline":
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedAction, action)
	}

	return nil, fmt.Errorf("unknown action %q", action)
}

// --- Per-processor parsers --------------------------------------------------

func parseBase(m map[string]any) (ProcessorBase, error) {
	var b ProcessorBase
	if v, ok := optionalString(m, "customIdentifier"); ok {
		b.CustomIdentifier = v
	}
	if v, ok := optionalString(m, "description"); ok {
		b.Description = v
	}
	if v, ok, err := optionalBool(m, "ignore_failure"); err != nil {
		return b, err
	} else if ok {
		b.IgnoreFailure = v
	}
	if raw, ok := m["where"]; ok {
		whereMap, err := asStringMap(raw)
		if err != nil {
			return b, fmt.Errorf("where: %w", err)
		}
		cond, err := parseCondition(whereMap)
		if err != nil {
			return b, fmt.Errorf("where: %w", err)
		}
		b.Where = cond
	}
	return b, nil
}

func parseSet(m map[string]any) (*SetProcessor, error) {
	known := append([]string{"to", "override", "value", "copy_from"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	to, ok := stringField(m, "to")
	if !ok {
		return nil, fmt.Errorf("'to' is required")
	}
	p := &SetProcessor{ProcessorBase: base, To: to}
	if v, ok, err := optionalBool(m, "override"); err != nil {
		return nil, err
	} else if ok {
		p.Override = boolPtr(v)
	}
	value, hasValue := m["value"]
	copyFrom, hasCopyFrom := m["copy_from"]
	switch {
	case hasValue && hasCopyFrom:
		return nil, fmt.Errorf("set must have either 'value' or 'copy_from', not both")
	case hasValue:
		p.Value = value
		p.HasValue = true
	case hasCopyFrom:
		s, ok := copyFrom.(string)
		if !ok {
			return nil, fmt.Errorf("'copy_from' must be a string, got %T", copyFrom)
		}
		p.CopyFrom = s
	default:
		return nil, fmt.Errorf("set must have either 'value' or 'copy_from'")
	}
	return p, nil
}

func parseAppend(m map[string]any) (*AppendProcessor, error) {
	known := append([]string{"to", "value", "allow_duplicates"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	to, ok := stringField(m, "to")
	if !ok {
		return nil, fmt.Errorf("'to' is required")
	}
	rawValue, ok := m["value"]
	if !ok {
		return nil, fmt.Errorf("'value' is required")
	}
	values, err := asAnyList(rawValue)
	if err != nil {
		return nil, fmt.Errorf("value: %w", err)
	}
	p := &AppendProcessor{ProcessorBase: base, To: to, Value: values}
	if v, ok, err := optionalBool(m, "allow_duplicates"); err != nil {
		return nil, err
	} else if ok {
		p.AllowDuplicates = boolPtr(v)
	}
	return p, nil
}

func parseRemove(m map[string]any) (*RemoveProcessor, error) {
	known := append([]string{"from", "ignore_missing"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	from, ok := stringField(m, "from")
	if !ok {
		return nil, fmt.Errorf("'from' is required")
	}
	p := &RemoveProcessor{ProcessorBase: base, From: from}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}
	return p, nil
}

func parseRemoveByPrefix(m map[string]any) (*RemoveByPrefixProcessor, error) {
	// remove_by_prefix has no `where`.
	known := []string{"action", "customIdentifier", "description", "ignore_failure", "from"}
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	if _, ok := m["where"]; ok {
		return nil, fmt.Errorf("remove_by_prefix does not support 'where'")
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	from, ok := stringField(m, "from")
	if !ok {
		return nil, fmt.Errorf("'from' is required")
	}
	return &RemoveByPrefixProcessor{ProcessorBase: base, From: from}, nil
}

func parseRename(m map[string]any) (*RenameProcessor, error) {
	known := append([]string{"from", "to", "ignore_missing", "override"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	from, ok := stringField(m, "from")
	if !ok {
		return nil, fmt.Errorf("'from' is required")
	}
	to, ok := stringField(m, "to")
	if !ok {
		return nil, fmt.Errorf("'to' is required")
	}
	p := &RenameProcessor{ProcessorBase: base, From: from, To: to}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}
	if v, ok, err := optionalBool(m, "override"); err != nil {
		return nil, err
	} else if ok {
		p.Override = boolPtr(v)
	}
	return p, nil
}

func parseDropDocument(m map[string]any) (*DropDocumentProcessor, error) {
	if extras := unknownKeys(m, commonProcessorFields...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	if base.Where == nil {
		return nil, fmt.Errorf("drop_document requires a 'where' clause")
	}
	return &DropDocumentProcessor{ProcessorBase: base}, nil
}

func parseStringTransform(m map[string]any, action string) (from, to string, ignoreMissing *bool, base ProcessorBase, err error) {
	known := append([]string{"from", "to", "ignore_missing"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return "", "", nil, ProcessorBase{}, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err = parseBase(m)
	if err != nil {
		return "", "", nil, ProcessorBase{}, err
	}
	var ok bool
	from, ok = stringField(m, "from")
	if !ok {
		return "", "", nil, ProcessorBase{}, fmt.Errorf("'from' is required")
	}
	if v, ok := optionalString(m, "to"); ok {
		to = v
	}
	if v, ok, e := optionalBool(m, "ignore_missing"); e != nil {
		err = e
		return "", "", nil, ProcessorBase{}, e
	} else if ok {
		ignoreMissing = boolPtr(v)
	}
	_ = action
	return from, to, ignoreMissing, base, nil
}

func parseUppercase(m map[string]any) (*UppercaseProcessor, error) {
	from, to, im, base, err := parseStringTransform(m, "uppercase")
	if err != nil {
		return nil, err
	}
	return &UppercaseProcessor{ProcessorBase: base, From: from, To: to, IgnoreMissing: im}, nil
}

func parseLowercase(m map[string]any) (*LowercaseProcessor, error) {
	from, to, im, base, err := parseStringTransform(m, "lowercase")
	if err != nil {
		return nil, err
	}
	return &LowercaseProcessor{ProcessorBase: base, From: from, To: to, IgnoreMissing: im}, nil
}

func parseTrim(m map[string]any) (*TrimProcessor, error) {
	from, to, im, base, err := parseStringTransform(m, "trim")
	if err != nil {
		return nil, err
	}
	return &TrimProcessor{ProcessorBase: base, From: from, To: to, IgnoreMissing: im}, nil
}

func parseReplace(m map[string]any) (*ReplaceProcessor, error) {
	known := append([]string{"from", "pattern", "replacement", "to", "ignore_missing"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	from, ok := stringField(m, "from")
	if !ok {
		return nil, fmt.Errorf("'from' is required")
	}
	pattern, ok := stringField(m, "pattern")
	if !ok {
		return nil, fmt.Errorf("'pattern' is required (non-empty)")
	}
	replacementRaw, hasReplacement := m["replacement"]
	if !hasReplacement {
		return nil, fmt.Errorf("'replacement' is required")
	}
	replacement, ok := replacementRaw.(string)
	if !ok {
		return nil, fmt.Errorf("'replacement' must be a string, got %T", replacementRaw)
	}
	p := &ReplaceProcessor{ProcessorBase: base, From: from, Pattern: pattern, Replacement: replacement}
	if v, ok := optionalString(m, "to"); ok {
		p.To = v
	}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}
	return p, nil
}

func parseConvert(m map[string]any) (*ConvertProcessor, error) {
	known := append([]string{"from", "to", "type", "ignore_missing"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	from, ok := stringField(m, "from")
	if !ok {
		return nil, fmt.Errorf("'from' is required")
	}
	typeStr, ok := stringField(m, "type")
	if !ok {
		return nil, fmt.Errorf("'type' is required")
	}
	ct, err := parseConvertType(typeStr)
	if err != nil {
		return nil, err
	}
	p := &ConvertProcessor{ProcessorBase: base, From: from, Type: ct}
	if v, ok := optionalString(m, "to"); ok {
		p.To = v
	}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}

	// where ⇒ to required AND to != from (unless where is `always`).
	if base.Where != nil && base.Where.Kind != CondAlways {
		if p.To == "" {
			return nil, fmt.Errorf("convert with a 'where' condition requires 'to'")
		}
		if p.To == p.From {
			return nil, fmt.Errorf("convert with a 'where' condition requires 'to' to differ from 'from'")
		}
	}
	return p, nil
}

func parseConvertType(s string) (ConvertType, error) {
	switch ConvertType(s) {
	case ConvertTypeInteger, ConvertTypeLong, ConvertTypeDouble, ConvertTypeBoolean, ConvertTypeString:
		return ConvertType(s), nil
	}
	return "", fmt.Errorf("invalid convert type %q (want integer|long|double|boolean|string)", s)
}

// --- Phase 2 / Phase 3 parsers ---------------------------------------------

func parseGrok(m map[string]any) (*GrokProcessor, error) {
	known := append([]string{"from", "patterns", "pattern_definitions", "ignore_missing"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	from, ok := stringField(m, "from")
	if !ok {
		return nil, fmt.Errorf("'from' is required")
	}
	patternsRaw, ok := m["patterns"]
	if !ok {
		return nil, fmt.Errorf("'patterns' is required")
	}
	patterns, err := nonEmptyStringList(patternsRaw, "patterns")
	if err != nil {
		return nil, err
	}
	p := &GrokProcessor{ProcessorBase: base, From: from, Patterns: patterns}
	if v, ok := m["pattern_definitions"]; ok {
		defs, err := stringStringMap(v, "pattern_definitions")
		if err != nil {
			return nil, err
		}
		p.PatternDefinitions = defs
	}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}
	return p, nil
}

func parseDissect(m map[string]any) (*DissectProcessor, error) {
	known := append([]string{"from", "pattern", "append_separator", "ignore_missing"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	from, ok := stringField(m, "from")
	if !ok {
		return nil, fmt.Errorf("'from' is required")
	}
	pattern, ok := stringField(m, "pattern")
	if !ok {
		return nil, fmt.Errorf("'pattern' is required (non-empty)")
	}
	p := &DissectProcessor{ProcessorBase: base, From: from, Pattern: pattern}
	if v, ok := optionalString(m, "append_separator"); ok {
		p.AppendSeparator = v
	}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}
	return p, nil
}

func parseDate(m map[string]any) (*DateProcessor, error) {
	known := append([]string{"from", "to", "formats", "output_format", "timezone", "locale"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	from, ok := stringField(m, "from")
	if !ok {
		return nil, fmt.Errorf("'from' is required")
	}
	formatsRaw, ok := m["formats"]
	if !ok {
		return nil, fmt.Errorf("'formats' is required")
	}
	formats, err := nonEmptyStringList(formatsRaw, "formats")
	if err != nil {
		return nil, err
	}
	p := &DateProcessor{ProcessorBase: base, From: from, Formats: formats}
	if v, ok := optionalString(m, "to"); ok {
		p.To = v
	}
	if v, ok := optionalString(m, "output_format"); ok {
		p.OutputFormat = v
	}
	if v, ok := optionalString(m, "timezone"); ok {
		p.Timezone = v
	}
	if v, ok := optionalString(m, "locale"); ok {
		p.Locale = v
	}
	return p, nil
}

func parseRedact(m map[string]any) (*RedactProcessor, error) {
	known := append([]string{"from", "patterns", "pattern_definitions", "prefix", "suffix", "ignore_missing"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	from, ok := stringField(m, "from")
	if !ok {
		return nil, fmt.Errorf("'from' is required")
	}
	patternsRaw, ok := m["patterns"]
	if !ok {
		return nil, fmt.Errorf("'patterns' is required")
	}
	patterns, err := nonEmptyStringList(patternsRaw, "patterns")
	if err != nil {
		return nil, err
	}
	p := &RedactProcessor{ProcessorBase: base, From: from, Patterns: patterns}
	if v, ok := m["pattern_definitions"]; ok {
		defs, err := stringStringMap(v, "pattern_definitions")
		if err != nil {
			return nil, err
		}
		p.PatternDefinitions = defs
	}
	if v, ok := optionalString(m, "prefix"); ok {
		p.Prefix = v
	}
	if v, ok := optionalString(m, "suffix"); ok {
		p.Suffix = v
	}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}
	return p, nil
}

func parseMath(m map[string]any) (*MathProcessor, error) {
	known := append([]string{"expression", "to", "ignore_missing"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	expression, ok := stringField(m, "expression")
	if !ok {
		return nil, fmt.Errorf("'expression' is required (non-empty)")
	}
	to, ok := stringField(m, "to")
	if !ok {
		return nil, fmt.Errorf("'to' is required")
	}
	p := &MathProcessor{ProcessorBase: base, Expression: expression, To: to}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}
	return p, nil
}

func parseJSONExtract(m map[string]any) (*JSONExtractProcessor, error) {
	known := append([]string{"field", "extractions", "ignore_missing"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	field, ok := stringField(m, "field")
	if !ok {
		return nil, fmt.Errorf("'field' is required")
	}
	extractionsRaw, ok := m["extractions"]
	if !ok {
		return nil, fmt.Errorf("'extractions' is required")
	}
	extractions, err := parseJSONExtractions(extractionsRaw)
	if err != nil {
		return nil, err
	}
	p := &JSONExtractProcessor{ProcessorBase: base, Field: field, Extractions: extractions}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}
	return p, nil
}

func parseJSONExtractions(raw any) ([]JSONExtraction, error) {
	list, err := asAnyList(raw)
	if err != nil {
		return nil, fmt.Errorf("extractions: %w", err)
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("extractions: must be a non-empty list")
	}
	out := make([]JSONExtraction, 0, len(list))
	for i, item := range list {
		mm, err := asStringMap(item)
		if err != nil {
			return nil, fmt.Errorf("extractions[%d]: %w", i, err)
		}
		if extras := unknownKeys(mm, "selector", "target_field", "type"); len(extras) > 0 {
			return nil, fmt.Errorf("extractions[%d]: unknown key(s): %v", i, extras)
		}
		selector, ok := stringField(mm, "selector")
		if !ok {
			return nil, fmt.Errorf("extractions[%d]: 'selector' is required", i)
		}
		target, ok := stringField(mm, "target_field")
		if !ok {
			return nil, fmt.Errorf("extractions[%d]: 'target_field' is required", i)
		}
		ext := JSONExtraction{Selector: selector, TargetField: target}
		if v, ok := optionalString(mm, "type"); ok {
			if err := validateJSONExtractType(v); err != nil {
				return nil, fmt.Errorf("extractions[%d]: %w", i, err)
			}
			ext.Type = v
		}
		out = append(out, ext)
	}
	return out, nil
}

func validateJSONExtractType(s string) error {
	switch JSONExtractType(s) {
	case JSONExtractTypeKeyword, JSONExtractTypeInteger, JSONExtractTypeLong,
		JSONExtractTypeDouble, JSONExtractTypeBoolean:
		return nil
	}
	return fmt.Errorf("invalid type %q (want keyword|integer|long|double|boolean)", s)
}

func parseNetworkDirection(m map[string]any) (*NetworkDirectionProcessor, error) {
	known := append([]string{
		"source_ip", "destination_ip", "target_field", "ignore_missing",
		"internal_networks", "internal_networks_field",
	}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	sourceIP, ok := stringField(m, "source_ip")
	if !ok {
		return nil, fmt.Errorf("'source_ip' is required")
	}
	destIP, ok := stringField(m, "destination_ip")
	if !ok {
		return nil, fmt.Errorf("'destination_ip' is required")
	}
	p := &NetworkDirectionProcessor{ProcessorBase: base, SourceIP: sourceIP, DestinationIP: destIP}
	if v, ok := optionalString(m, "target_field"); ok {
		p.TargetField = v
	}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}

	_, hasNets := m["internal_networks"]
	_, hasNetsField := m["internal_networks_field"]
	switch {
	case hasNets && hasNetsField:
		return nil, fmt.Errorf("network_direction must have either 'internal_networks' or 'internal_networks_field', not both")
	case hasNets:
		nets, err := stringSliceField(m["internal_networks"], "internal_networks")
		if err != nil {
			return nil, err
		}
		p.InternalNetworks = nets
	case hasNetsField:
		s, ok := stringField(m, "internal_networks_field")
		if !ok {
			return nil, fmt.Errorf("'internal_networks_field' must be a non-empty string")
		}
		p.InternalNetworksField = s
	default:
		return nil, fmt.Errorf("network_direction must have either 'internal_networks' or 'internal_networks_field'")
	}
	return p, nil
}

func parseSplit(m map[string]any) (*SplitProcessor, error) {
	known := append([]string{"from", "separator", "to", "ignore_missing", "preserve_trailing"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	from, ok := stringField(m, "from")
	if !ok {
		return nil, fmt.Errorf("'from' is required")
	}
	separator, ok := stringField(m, "separator")
	if !ok {
		return nil, fmt.Errorf("'separator' is required (non-empty)")
	}
	p := &SplitProcessor{ProcessorBase: base, From: from, Separator: separator}
	if v, ok := optionalString(m, "to"); ok {
		p.To = v
	}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}
	if v, ok, err := optionalBool(m, "preserve_trailing"); err != nil {
		return nil, err
	} else if ok {
		p.PreserveTrailing = boolPtr(v)
	}
	return p, nil
}

func parseSort(m map[string]any) (*SortProcessor, error) {
	known := append([]string{"from", "to", "order", "ignore_missing"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	from, ok := stringField(m, "from")
	if !ok {
		return nil, fmt.Errorf("'from' is required")
	}
	p := &SortProcessor{ProcessorBase: base, From: from}
	if v, ok := optionalString(m, "to"); ok {
		p.To = v
	}
	if v, ok := optionalString(m, "order"); ok {
		switch SortOrder(v) {
		case SortOrderAsc, SortOrderDesc:
			p.Order = SortOrder(v)
		default:
			return nil, fmt.Errorf("invalid order %q (want asc|desc)", v)
		}
	}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}
	return p, nil
}

func parseJoin(m map[string]any) (*JoinProcessor, error) {
	known := append([]string{"from", "delimiter", "to", "ignore_missing"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	fromRaw, ok := m["from"]
	if !ok {
		return nil, fmt.Errorf("'from' is required")
	}
	from, err := nonEmptyStringList(fromRaw, "from")
	if err != nil {
		return nil, err
	}
	delimiterRaw, hasDelim := m["delimiter"]
	if !hasDelim {
		return nil, fmt.Errorf("'delimiter' is required")
	}
	delimiter, ok := delimiterRaw.(string)
	if !ok {
		return nil, fmt.Errorf("'delimiter' must be a string, got %T", delimiterRaw)
	}
	to, ok := stringField(m, "to")
	if !ok {
		return nil, fmt.Errorf("'to' is required")
	}
	p := &JoinProcessor{ProcessorBase: base, From: from, Delimiter: delimiter, To: to}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}
	return p, nil
}

func parseConcat(m map[string]any) (*ConcatProcessor, error) {
	known := append([]string{"from", "to", "ignore_missing"}, commonProcessorFields...)
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s): %v", extras)
	}
	base, err := parseBase(m)
	if err != nil {
		return nil, err
	}
	fromRaw, ok := m["from"]
	if !ok {
		return nil, fmt.Errorf("'from' is required")
	}
	parts, err := parseConcatParts(fromRaw)
	if err != nil {
		return nil, err
	}
	to, ok := stringField(m, "to")
	if !ok {
		return nil, fmt.Errorf("'to' is required")
	}
	p := &ConcatProcessor{ProcessorBase: base, From: parts, To: to}
	if v, ok, err := optionalBool(m, "ignore_missing"); err != nil {
		return nil, err
	} else if ok {
		p.IgnoreMissing = boolPtr(v)
	}
	return p, nil
}

func parseConcatParts(raw any) ([]ConcatPart, error) {
	list, err := asAnyList(raw)
	if err != nil {
		return nil, fmt.Errorf("from: %w", err)
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("from: must be a non-empty list")
	}
	out := make([]ConcatPart, 0, len(list))
	for i, item := range list {
		mm, err := asStringMap(item)
		if err != nil {
			return nil, fmt.Errorf("from[%d]: %w", i, err)
		}
		if extras := unknownKeys(mm, "type", "value"); len(extras) > 0 {
			return nil, fmt.Errorf("from[%d]: unknown key(s): %v", i, extras)
		}
		typeStr, ok := stringField(mm, "type")
		if !ok {
			return nil, fmt.Errorf("from[%d]: 'type' is required", i)
		}
		var partType ConcatPartType
		switch ConcatPartType(typeStr) {
		case ConcatPartField, ConcatPartLiteral:
			partType = ConcatPartType(typeStr)
		default:
			return nil, fmt.Errorf("from[%d]: invalid type %q (want field|literal)", i, typeStr)
		}
		valueRaw, hasValue := mm["value"]
		if !hasValue {
			return nil, fmt.Errorf("from[%d]: 'value' is required", i)
		}
		value, ok := valueRaw.(string)
		if !ok {
			return nil, fmt.Errorf("from[%d]: 'value' must be a string, got %T", i, valueRaw)
		}
		if partType == ConcatPartField && value == "" {
			return nil, fmt.Errorf("from[%d]: 'value' must be non-empty for field parts", i)
		}
		out = append(out, ConcatPart{Type: partType, Value: value})
	}
	return out, nil
}

// --- Conditions -------------------------------------------------------------

func parseCondition(m map[string]any) (*Condition, error) {
	if m == nil {
		return nil, fmt.Errorf("condition must be an object")
	}
	if _, ok := m["always"]; ok {
		return &Condition{Kind: CondAlways}, nil
	}
	if _, ok := m["never"]; ok {
		return &Condition{Kind: CondNever}, nil
	}
	if v, ok := m["and"]; ok {
		list, err := asAnyList(v)
		if err != nil {
			return nil, fmt.Errorf("and: %w", err)
		}
		subs, err := parseConditionList(list)
		if err != nil {
			return nil, fmt.Errorf("and: %w", err)
		}
		return &Condition{Kind: CondAnd, And: subs}, nil
	}
	if v, ok := m["or"]; ok {
		list, err := asAnyList(v)
		if err != nil {
			return nil, fmt.Errorf("or: %w", err)
		}
		subs, err := parseConditionList(list)
		if err != nil {
			return nil, fmt.Errorf("or: %w", err)
		}
		return &Condition{Kind: CondOr, Or: subs}, nil
	}
	if v, ok := m["not"]; ok {
		inner, err := asStringMap(v)
		if err != nil {
			return nil, fmt.Errorf("not: %w", err)
		}
		sub, err := parseCondition(inner)
		if err != nil {
			return nil, fmt.Errorf("not: %w", err)
		}
		return &Condition{Kind: CondNot, Not: sub}, nil
	}
	// Filter condition.
	if _, ok := m["field"]; ok {
		f, err := parseFilterCondition(m)
		if err != nil {
			return nil, err
		}
		return &Condition{Kind: CondFilter, Filter: f}, nil
	}
	return nil, fmt.Errorf("condition must contain one of: always, never, and, or, not, or 'field' (got keys: %v)", sortedKeys(m))
}

func parseConditionList(items []any) ([]Condition, error) {
	out := make([]Condition, 0, len(items))
	for i, it := range items {
		mm, err := asStringMap(it)
		if err != nil {
			return nil, fmt.Errorf("[%d]: %w", i, err)
		}
		c, err := parseCondition(mm)
		if err != nil {
			return nil, fmt.Errorf("[%d]: %w", i, err)
		}
		out = append(out, *c)
	}
	return out, nil
}

func parseFilterCondition(m map[string]any) (*FilterCondition, error) {
	known := []string{
		"field", "eq", "neq", "lt", "lte", "gt", "gte",
		"contains", "startsWith", "startswith", "endsWith", "endswith",
		"range", "includes", "exists",
	}
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s) in filter: %v", extras)
	}

	field, ok := stringField(m, "field")
	if !ok {
		return nil, fmt.Errorf("filter condition: 'field' is required")
	}
	f := &FilterCondition{Field: field}

	setBinary := func(key string, dst *any, has *bool) error {
		if v, ok := m[key]; ok {
			coerced, err := coerceOperand(v)
			if err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
			*dst = coerced
			*has = true
		}
		return nil
	}

	if err := setBinary("eq", &f.Eq, &f.HasEq); err != nil {
		return nil, err
	}
	if err := setBinary("neq", &f.Neq, &f.HasNeq); err != nil {
		return nil, err
	}
	if err := setBinary("lt", &f.Lt, &f.HasLt); err != nil {
		return nil, err
	}
	if err := setBinary("lte", &f.Lte, &f.HasLte); err != nil {
		return nil, err
	}
	if err := setBinary("gt", &f.Gt, &f.HasGt); err != nil {
		return nil, err
	}
	if err := setBinary("gte", &f.Gte, &f.HasGte); err != nil {
		return nil, err
	}
	if err := setBinary("contains", &f.Contains, &f.HasContain); err != nil {
		return nil, err
	}
	// startsWith / startswith
	for _, k := range []string{"startsWith", "startswith"} {
		if v, ok := m[k]; ok {
			coerced, err := coerceOperand(v)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
			if f.HasStarts {
				return nil, fmt.Errorf("filter has both startsWith and startswith")
			}
			f.StartsWith = coerced
			f.HasStarts = true
		}
	}
	for _, k := range []string{"endsWith", "endswith"} {
		if v, ok := m[k]; ok {
			coerced, err := coerceOperand(v)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
			if f.HasEnds {
				return nil, fmt.Errorf("filter has both endsWith and endswith")
			}
			f.EndsWith = coerced
			f.HasEnds = true
		}
	}
	if err := setBinary("includes", &f.Includes, &f.HasIncl); err != nil {
		return nil, err
	}
	if v, ok := m["range"]; ok {
		mm, err := asStringMap(v)
		if err != nil {
			return nil, fmt.Errorf("range: %w", err)
		}
		r, err := parseRangeCondition(mm)
		if err != nil {
			return nil, fmt.Errorf("range: %w", err)
		}
		f.Range = r
	}
	if v, ok := m["exists"]; ok {
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("exists: expected bool, got %T", v)
		}
		f.Exists = boolPtr(b)
	}

	if !filterHasAnyOperator(f) {
		return nil, fmt.Errorf("filter condition must specify at least one operator")
	}
	return f, nil
}

func parseRangeCondition(m map[string]any) (*RangeCondition, error) {
	known := []string{"gt", "gte", "lt", "lte"}
	if extras := unknownKeys(m, known...); len(extras) > 0 {
		return nil, fmt.Errorf("unknown key(s) in range: %v", extras)
	}
	r := &RangeCondition{}
	for _, k := range known {
		if v, ok := m[k]; ok {
			coerced, err := coerceOperand(v)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
			switch k {
			case "gt":
				r.Gt = coerced
			case "gte":
				r.Gte = coerced
			case "lt":
				r.Lt = coerced
			case "lte":
				r.Lte = coerced
			}
		}
	}
	if r.Gt == nil && r.Gte == nil && r.Lt == nil && r.Lte == nil {
		return nil, fmt.Errorf("range must specify at least one of gt/gte/lt/lte")
	}
	return r, nil
}

func filterHasAnyOperator(f *FilterCondition) bool {
	return f.HasEq || f.HasNeq || f.HasLt || f.HasLte || f.HasGt || f.HasGte ||
		f.HasContain || f.HasStarts || f.HasEnds || f.HasIncl ||
		f.Range != nil || f.Exists != nil
}

// --- helpers ---------------------------------------------------------------

func unknownKeys(m map[string]any, known ...string) []string {
	allowed := make(map[string]struct{}, len(known))
	for _, k := range known {
		allowed[k] = struct{}{}
	}
	var extras []string
	for k := range m {
		if _, ok := allowed[k]; !ok {
			extras = append(extras, k)
		}
	}
	sort.Strings(extras)
	return extras
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func asStringMap(v any) (map[string]any, error) {
	switch m := v.(type) {
	case map[string]any:
		return m, nil
	case map[any]any:
		// Result of yaml.v3 decode into `any`.
		out := make(map[string]any, len(m))
		for k, val := range m {
			ks, ok := k.(string)
			if !ok {
				return nil, fmt.Errorf("non-string key %v", k)
			}
			out[ks] = normalizeYAML(val)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("expected object, got %T", v)
	}
}

func asAnyList(v any) ([]any, error) {
	switch l := v.(type) {
	case []any:
		return l, nil
	case []map[string]any:
		out := make([]any, len(l))
		for i, m := range l {
			out[i] = m
		}
		return out, nil
	default:
		return nil, fmt.Errorf("expected list, got %T", v)
	}
}

func stringField(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return "", false
	}
	return s, true
}

func optionalString(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	return s, true
}

func optionalBool(m map[string]any, key string) (bool, bool, error) {
	v, ok := m[key]
	if !ok {
		return false, false, nil
	}
	b, ok := v.(bool)
	if !ok {
		return false, false, fmt.Errorf("%s: expected bool, got %T", key, v)
	}
	return b, true, nil
}

func boolPtr(b bool) *bool { return &b }

// stringSliceField coerces a list of strings (`[]any` of strings, or
// `[]string` directly). Returns an error if any element is non-string.
func stringSliceField(v any, key string) ([]string, error) {
	list, err := asAnyList(v)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	out := make([]string, 0, len(list))
	for i, item := range list {
		s, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("%s[%d]: expected string, got %T", key, i, item)
		}
		out = append(out, s)
	}
	return out, nil
}

// nonEmptyStringList is stringSliceField that additionally requires a
// non-empty list of non-empty strings.
func nonEmptyStringList(v any, key string) ([]string, error) {
	list, err := stringSliceField(v, key)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("%s: must be a non-empty list", key)
	}
	for i, s := range list {
		if s == "" {
			return nil, fmt.Errorf("%s[%d]: must be non-empty", key, i)
		}
	}
	return list, nil
}

// stringStringMap coerces a map[string]string from a YAML/JSON-decoded value,
// accepting both `map[string]any` and `map[any]any` shapes.
func stringStringMap(v any, key string) (map[string]string, error) {
	mm, err := asStringMap(v)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	out := make(map[string]string, len(mm))
	for k, val := range mm {
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("%s[%q]: expected string, got %T", key, k, val)
		}
		out[k] = s
	}
	return out, nil
}

// coerceOperand normalises a numeric / string / bool value for filter operands.
//
// JSON unmarshals all numbers as float64; YAML may produce int / float64; we
// keep strings/bools as-is and convert any int kinds to float64 so the
// evaluator only ever sees string|float64|bool.
func coerceOperand(v any) (any, error) {
	switch x := v.(type) {
	case nil:
		return nil, fmt.Errorf("operand must not be null")
	case string, bool, float64:
		return x, nil
	case int:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case uint:
		return float64(x), nil
	case uint32:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	case float32:
		return float64(x), nil
	default:
		return nil, fmt.Errorf("unsupported operand type %T", v)
	}
}

// normalizeYAML walks a value produced by yaml.v3 and converts every
// map[any]any to map[string]any (recursively). This lets the rest of the
// parser assume map[string]any everywhere.
func normalizeYAML(v any) any {
	switch x := v.(type) {
	case map[any]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			ks, ok := k.(string)
			if !ok {
				return v // leave it; asStringMap will error.
			}
			out[ks] = normalizeYAML(val)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = normalizeYAML(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = normalizeYAML(item)
		}
		return out
	default:
		return v
	}
}
