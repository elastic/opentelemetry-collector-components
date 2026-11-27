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

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/trivago/grok"
)

// Processors returns a map of all processor implementations keyed by action name.
func Processors() map[string]ProcessorFunc {
	return map[string]ProcessorFunc{
		"set":               procSet,
		"rename":            procRename,
		"remove":            procRemove,
		"remove_by_prefix":  procRemoveByPrefix,
		"convert":           procConvert,
		"uppercase":         procUppercase,
		"lowercase":         procLowercase,
		"trim":              procTrim,
		"grok":              procGrok,
		"dissect":           procDissect,
		"replace":           procReplace,
		"redact":            procRedact,
		"split":             procSplit,
		"join":              procJoin,
		"sort":              procSort,
		"append":            procAppend,
		"concat":            procConcat,
		"date":              procDate,
		"math":              procMath,
		"json_extract":      procJSONExtract,
		"drop_document":     procDropDocument,
		"network_direction": procNetworkDirection,
	}
}

// boolDefault returns the value of a *bool or a default if nil.
func boolDefault(b *bool, def bool) bool {
	if b == nil {
		return def
	}
	return *b
}

// toString converts any value to its string representation.
func toString(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

// --- Processor implementations ---

func procSet(proc *ProcessorDef, doc *Document) error {
	to := proc.To
	if to == "" {
		to = proc.Field
	}
	if to == "" {
		return fmt.Errorf("set: missing 'to' or 'field' parameter")
	}

	override := boolDefault(proc.Override, true)
	if !override && doc.Exists(to) {
		return nil
	}

	if proc.CopyFrom != "" {
		val, ok := doc.GetAny(proc.CopyFrom)
		if !ok {
			return fmt.Errorf("set: source field %q does not exist", proc.CopyFrom)
		}
		doc.Set(to, val)
		return nil
	}

	if proc.Value == nil {
		return fmt.Errorf("set: missing 'value' or 'copy_from' parameter")
	}
	doc.Set(to, proc.Value)
	return nil
}

func procRename(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		return fmt.Errorf("rename: missing 'from' parameter")
	}
	to := proc.To
	if to == "" {
		return fmt.Errorf("rename: missing 'to' parameter")
	}

	val, ok := doc.GetAny(from)
	if !ok {
		if boolDefault(proc.IgnoreMissing, false) {
			return nil
		}
		return fmt.Errorf("rename: field %q does not exist", from)
	}

	override := boolDefault(proc.Override, true)
	if !override && doc.Exists(to) {
		return fmt.Errorf("rename: target field %q already exists", to)
	}

	doc.Set(to, val)
	doc.Remove(from)
	return nil
}

func procRemove(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		from = proc.Field
	}
	if from == "" {
		return fmt.Errorf("remove: missing 'from' parameter")
	}

	removed := doc.Remove(from)
	if !removed && !boolDefault(proc.IgnoreMissing, false) {
		return fmt.Errorf("remove: field %q does not exist", from)
	}
	return nil
}

func procRemoveByPrefix(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		from = proc.Field
	}
	if from == "" {
		return fmt.Errorf("remove_by_prefix: missing 'from' parameter")
	}
	doc.RemoveByPrefix(from)
	return nil
}

func procConvert(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		from = proc.Field
	}
	if from == "" {
		return fmt.Errorf("convert: missing 'from' parameter")
	}
	if proc.Type == "" {
		return fmt.Errorf("convert: missing 'type' parameter")
	}

	val, ok := doc.GetAny(from)
	if !ok {
		if boolDefault(proc.IgnoreMissing, false) {
			return nil
		}
		return fmt.Errorf("convert: field %q does not exist", from)
	}

	converted, err := convertValue(val, proc.Type)
	if err != nil {
		return fmt.Errorf("convert: field %q: %w", from, err)
	}

	to := from
	if proc.To != "" {
		to = proc.To
	}
	doc.Set(to, converted)
	return nil
}

func convertValue(val interface{}, typ string) (interface{}, error) {
	s := toString(val)
	switch typ {
	case "integer", "long":
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to %s: %w", s, typ, err)
		}
		return int64(f), nil
	case "double":
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to double: %w", s, err)
		}
		return f, nil
	case "boolean":
		b, err := strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to boolean: %w", s, err)
		}
		return b, nil
	case "string":
		return s, nil
	default:
		return nil, fmt.Errorf("unsupported convert type %q", typ)
	}
}

func procUppercase(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		from = proc.Field
	}
	if from == "" {
		return fmt.Errorf("uppercase: missing 'from' parameter")
	}

	val, ok := doc.GetAny(from)
	if !ok {
		if boolDefault(proc.IgnoreMissing, false) {
			return nil
		}
		return fmt.Errorf("uppercase: field %q does not exist", from)
	}

	to := from
	if proc.To != "" {
		to = proc.To
	}
	doc.Set(to, strings.ToUpper(toString(val)))
	return nil
}

func procLowercase(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		from = proc.Field
	}
	if from == "" {
		return fmt.Errorf("lowercase: missing 'from' parameter")
	}

	val, ok := doc.GetAny(from)
	if !ok {
		if boolDefault(proc.IgnoreMissing, false) {
			return nil
		}
		return fmt.Errorf("lowercase: field %q does not exist", from)
	}

	to := from
	if proc.To != "" {
		to = proc.To
	}
	doc.Set(to, strings.ToLower(toString(val)))
	return nil
}

func procTrim(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		from = proc.Field
	}
	if from == "" {
		return fmt.Errorf("trim: missing 'from' parameter")
	}

	val, ok := doc.GetAny(from)
	if !ok {
		if boolDefault(proc.IgnoreMissing, false) {
			return nil
		}
		return fmt.Errorf("trim: field %q does not exist", from)
	}

	to := from
	if proc.To != "" {
		to = proc.To
	}
	doc.Set(to, strings.TrimSpace(toString(val)))
	return nil
}

func procGrok(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		from = proc.Field
	}
	if from == "" {
		return fmt.Errorf("grok: missing 'from' parameter")
	}
	if len(proc.Patterns) == 0 {
		return fmt.Errorf("grok: missing or empty 'patterns' parameter")
	}

	val, ok := doc.GetAny(from)
	if !ok {
		if boolDefault(proc.IgnoreMissing, false) {
			return nil
		}
		return fmt.Errorf("grok: field %q does not exist", from)
	}
	strVal := toString(val)

	cfg := grok.Config{
		NamedCapturesOnly: true,
		RemoveEmptyValues: true,
	}
	if proc.PatternDefinitions != nil {
		cfg.Patterns = proc.PatternDefinitions
	}

	g, err := grok.New(cfg)
	if err != nil {
		return fmt.Errorf("grok: failed to create grok instance: %w", err)
	}

	for _, pattern := range proc.Patterns {
		values, err := g.ParseString(pattern, strVal)
		if err != nil {
			continue
		}
		if len(values) > 0 {
			for k, v := range values {
				doc.Set(k, v)
			}
			return nil
		}
	}

	return fmt.Errorf("grok: no patterns matched field %q", from)
}

func procDissect(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		from = proc.Field
	}
	if from == "" {
		return fmt.Errorf("dissect: missing 'from' parameter")
	}
	if proc.Pattern == "" {
		return fmt.Errorf("dissect: missing 'pattern' parameter")
	}

	val, ok := doc.GetAny(from)
	if !ok {
		if boolDefault(proc.IgnoreMissing, false) {
			return nil
		}
		return fmt.Errorf("dissect: field %q does not exist", from)
	}

	fields, err := dissectParse(proc.Pattern, toString(val), proc.AppendSeparator)
	if err != nil {
		return fmt.Errorf("dissect: %w", err)
	}
	for k, v := range fields {
		doc.Set(k, v)
	}
	return nil
}

func dissectParse(pattern, input string, appendSep string) (map[string]string, error) {
	type token struct {
		name     string
		modifier byte
		order    int
	}

	var tokens []token
	var delimiters []string
	pos := 0
	for pos < len(pattern) {
		idx := strings.Index(pattern[pos:], "%{")
		if idx < 0 {
			delimiters = append(delimiters, pattern[pos:])
			break
		}
		delimiters = append(delimiters, pattern[pos:pos+idx])
		pos += idx + 2

		end := strings.Index(pattern[pos:], "}")
		if end < 0 {
			return nil, fmt.Errorf("unclosed %%{ in dissect pattern")
		}
		content := pattern[pos : pos+end]
		pos += end + 1

		var t token
		t.order = -1
		if len(content) > 0 && (content[0] == '+' || content[0] == '?' || content[0] == '*' || content[0] == '&') {
			t.modifier = content[0]
			content = content[1:]
		}
		if slashIdx := strings.LastIndex(content, "/"); slashIdx >= 0 {
			if n, err := strconv.Atoi(content[slashIdx+1:]); err == nil {
				t.order = n
				content = content[:slashIdx]
			}
		}
		t.name = content
		tokens = append(tokens, t)
	}

	if len(tokens) == 0 {
		return nil, fmt.Errorf("no fields in dissect pattern")
	}

	values := make([]string, len(tokens))
	remaining := input
	for i := 0; i < len(tokens); i++ {
		if i < len(delimiters) && delimiters[i] != "" {
			if !strings.HasPrefix(remaining, delimiters[i]) {
				return nil, fmt.Errorf("dissect pattern mismatch: expected %q", delimiters[i])
			}
			remaining = remaining[len(delimiters[i]):]
		}

		var endDelim string
		if i+1 < len(delimiters) {
			endDelim = delimiters[i+1]
		}

		if endDelim == "" && i == len(tokens)-1 {
			values[i] = remaining
			remaining = ""
		} else if endDelim != "" {
			idx := strings.Index(remaining, endDelim)
			if idx < 0 {
				return nil, fmt.Errorf("dissect pattern mismatch: expected delimiter %q", endDelim)
			}
			values[i] = remaining[:idx]
			remaining = remaining[idx:]
		} else {
			values[i] = remaining
			remaining = ""
		}
	}

	result := make(map[string]string)
	appendFields := make(map[string][]struct {
		order int
		value string
	})
	keyFields := make(map[string]string)

	for i, t := range tokens {
		switch t.modifier {
		case '?':
			// Skip
		case '*':
			keyFields[t.name] = values[i]
		case '&':
			if key, ok := keyFields[t.name]; ok {
				result[key] = values[i]
			}
		case '+':
			appendFields[t.name] = append(appendFields[t.name], struct {
				order int
				value string
			}{t.order, values[i]})
		default:
			result[t.name] = values[i]
		}
	}

	for name, parts := range appendFields {
		sort.Slice(parts, func(i, j int) bool {
			if parts[i].order >= 0 && parts[j].order >= 0 {
				return parts[i].order < parts[j].order
			}
			return false
		})
		vals := make([]string, len(parts))
		for i, p := range parts {
			vals[i] = p.value
		}
		result[name] = strings.Join(vals, appendSep)
	}

	return result, nil
}

func procReplace(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		from = proc.Field
	}
	if from == "" {
		return fmt.Errorf("replace: missing 'from' parameter")
	}
	if len(proc.Patterns) == 0 && proc.Pattern == "" {
		return fmt.Errorf("replace: missing 'pattern' parameter")
	}

	val, ok := doc.GetAny(from)
	if !ok {
		if boolDefault(proc.IgnoreMissing, false) {
			return nil
		}
		return fmt.Errorf("replace: field %q does not exist", from)
	}

	pattern := proc.Pattern
	if pattern == "" && len(proc.Patterns) > 0 {
		pattern = proc.Patterns[0]
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("replace: invalid pattern %q: %w", pattern, err)
	}

	result := re.ReplaceAllString(toString(val), proc.Replacement)
	to := from
	if proc.To != "" {
		to = proc.To
	}
	doc.Set(to, result)
	return nil
}

func procRedact(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		from = proc.Field
	}
	if from == "" {
		return fmt.Errorf("redact: missing 'from' parameter")
	}
	if len(proc.Patterns) == 0 {
		return fmt.Errorf("redact: missing or empty 'patterns' parameter")
	}

	val, ok := doc.GetAny(from)
	if !ok {
		if boolDefault(proc.IgnoreMissing, true) {
			return nil
		}
		return fmt.Errorf("redact: field %q does not exist", from)
	}

	prefix := "<"
	if proc.Prefix != "" {
		prefix = proc.Prefix
	}
	suffix := ">"
	if proc.Suffix != "" {
		suffix = proc.Suffix
	}

	cfg := grok.Config{
		NamedCapturesOnly: true,
		RemoveEmptyValues: true,
	}
	if proc.PatternDefinitions != nil {
		cfg.Patterns = proc.PatternDefinitions
	}

	g, err := grok.New(cfg)
	if err != nil {
		return fmt.Errorf("redact: failed to create grok instance: %w", err)
	}

	strVal := toString(val)
	for _, pattern := range proc.Patterns {
		values, err := g.ParseString(pattern, strVal)
		if err != nil {
			continue
		}
		for name, matched := range values {
			replacement := prefix + name + suffix
			strVal = strings.ReplaceAll(strVal, matched, replacement)
		}
	}

	doc.Set(from, strVal)
	return nil
}

func procSplit(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		from = proc.Field
	}
	if from == "" {
		return fmt.Errorf("split: missing 'from' parameter")
	}
	separator := proc.Separator
	if separator == "" {
		return fmt.Errorf("split: missing 'separator' parameter")
	}

	val, ok := doc.GetAny(from)
	if !ok {
		if boolDefault(proc.IgnoreMissing, false) {
			return nil
		}
		return fmt.Errorf("split: field %q does not exist", from)
	}

	re, err := regexp.Compile(separator)
	if err != nil {
		return fmt.Errorf("split: invalid separator pattern %q: %w", separator, err)
	}

	parts := re.Split(toString(val), -1)
	if !boolDefault(proc.PreserveTrailing, false) {
		for len(parts) > 0 && parts[len(parts)-1] == "" {
			parts = parts[:len(parts)-1]
		}
	}

	result := make([]interface{}, len(parts))
	for i, p := range parts {
		result[i] = p
	}

	to := from
	if proc.To != "" {
		to = proc.To
	}
	doc.Set(to, result)
	return nil
}

func procJoin(proc *ProcessorDef, doc *Document) error {
	if len(proc.FromFields) == 0 {
		return fmt.Errorf("join: missing or empty 'from_fields' parameter")
	}
	to := proc.To
	if to == "" {
		to = proc.Field
	}
	if to == "" {
		return fmt.Errorf("join: missing 'to' parameter")
	}

	ignoreMissing := boolDefault(proc.IgnoreMissing, false)

	var parts []string
	for _, field := range proc.FromFields {
		val, ok := doc.GetAny(field)
		if !ok {
			if ignoreMissing {
				continue
			}
			return fmt.Errorf("join: field %q does not exist", field)
		}
		if slice, ok := val.([]interface{}); ok {
			for _, item := range slice {
				parts = append(parts, toString(item))
			}
		} else {
			parts = append(parts, toString(val))
		}
	}

	doc.Set(to, strings.Join(parts, proc.Delimiter))
	return nil
}

func procSort(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		from = proc.Field
	}
	if from == "" {
		return fmt.Errorf("sort: missing 'from' parameter")
	}

	val, ok := doc.GetAny(from)
	if !ok {
		if boolDefault(proc.IgnoreMissing, false) {
			return nil
		}
		return fmt.Errorf("sort: field %q does not exist", from)
	}

	slice, ok := val.([]interface{})
	if !ok {
		return fmt.Errorf("sort: field %q is not an array", from)
	}

	desc := proc.Order == "desc"
	sorted := make([]interface{}, len(slice))
	copy(sorted, slice)

	sort.Slice(sorted, func(i, j int) bool {
		si := toString(sorted[i])
		sj := toString(sorted[j])
		fi, iOk := toFloat64(sorted[i])
		fj, jOk := toFloat64(sorted[j])
		if iOk && jOk {
			if desc {
				return fi > fj
			}
			return fi < fj
		}
		if desc {
			return si > sj
		}
		return si < sj
	})

	to := from
	if proc.To != "" {
		to = proc.To
	}
	doc.Set(to, sorted)
	return nil
}

func procAppend(proc *ProcessorDef, doc *Document) error {
	to := proc.To
	if to == "" {
		to = proc.Field
	}
	if to == "" {
		return fmt.Errorf("append: missing 'to' parameter")
	}

	if len(proc.Values) == 0 {
		return fmt.Errorf("append: missing 'values' parameter")
	}

	allowDuplicates := boolDefault(proc.AllowDuplicates, true)

	var existing []interface{}
	if val, ok := doc.GetAny(to); ok {
		if slice, ok := val.([]interface{}); ok {
			existing = slice
		} else {
			existing = []interface{}{val}
		}
	}

	for _, v := range proc.Values {
		if !allowDuplicates {
			dup := false
			for _, e := range existing {
				if fmt.Sprintf("%v", e) == fmt.Sprintf("%v", v) {
					dup = true
					break
				}
			}
			if dup {
				continue
			}
		}
		existing = append(existing, v)
	}

	doc.Set(to, existing)
	return nil
}

func procConcat(proc *ProcessorDef, doc *Document) error {
	to := proc.To
	if to == "" {
		to = proc.Field
	}
	if to == "" {
		return fmt.Errorf("concat: missing 'to' parameter")
	}

	if len(proc.ConcatFrom) == 0 {
		return fmt.Errorf("concat: missing 'concat_from' parameter")
	}

	ignoreMissing := boolDefault(proc.IgnoreMissing, false)

	var parts []string
	for _, src := range proc.ConcatFrom {
		switch src.Type {
		case "literal":
			parts = append(parts, src.Value)
		case "field":
			val, ok := doc.GetAny(src.Value)
			if !ok {
				if ignoreMissing {
					continue
				}
				return fmt.Errorf("concat: field %q does not exist", src.Value)
			}
			parts = append(parts, toString(val))
		default:
			return fmt.Errorf("concat: unknown from type %q", src.Type)
		}
	}

	doc.Set(to, strings.Join(parts, ""))
	return nil
}

func procDate(proc *ProcessorDef, doc *Document) error {
	from := proc.From
	if from == "" {
		from = proc.Field
	}
	if from == "" {
		return fmt.Errorf("date: missing 'from' parameter")
	}
	if len(proc.Formats) == 0 {
		return fmt.Errorf("date: missing or empty 'formats' parameter")
	}

	val, ok := doc.GetAny(from)
	if !ok {
		return fmt.Errorf("date: field %q does not exist", from)
	}

	var loc *time.Location
	if proc.Timezone != "" {
		var err error
		loc, err = time.LoadLocation(proc.Timezone)
		if err != nil {
			return fmt.Errorf("date: invalid timezone %q: %w", proc.Timezone, err)
		}
	}

	strVal := toString(val)
	var parsed time.Time
	var parseErr error
	for _, format := range proc.Formats {
		goFmt := dateFormatToGo(format)
		if loc != nil {
			parsed, parseErr = time.ParseInLocation(goFmt, strVal, loc)
		} else {
			parsed, parseErr = time.Parse(goFmt, strVal)
		}
		if parseErr == nil {
			break
		}
	}
	if parseErr != nil {
		return fmt.Errorf("date: cannot parse %q with any of the given formats: %w", strVal, parseErr)
	}

	to := from
	if proc.To != "" {
		to = proc.To
	}

	if proc.OutputFormat != "" {
		doc.Set(to, parsed.Format(dateFormatToGo(proc.OutputFormat)))
	} else {
		doc.Set(to, parsed.Format(time.RFC3339Nano))
	}
	return nil
}

func dateFormatToGo(format string) string {
	switch format {
	case "ISO8601", "iso8601":
		return time.RFC3339
	case "UNIX":
		return "UNIX"
	case "UNIX_MS":
		return "UNIX_MS"
	}

	replacer := strings.NewReplacer(
		"yyyy", "2006",
		"yy", "06",
		"MMMM", "January",
		"MMM", "Jan",
		"MM", "01",
		"dd", "02",
		"HH", "15",
		"hh", "03",
		"mm", "04",
		"ss", "05",
		"SSS", ".000",
		"SS", ".00",
		"S", ".0",
		"XXX", "-07:00",
		"XX", "-0700",
		"X", "-07",
		"ZZZ", "MST",
		"a", "PM",
		"EEE", "Mon",
		"EEEE", "Monday",
	)
	return replacer.Replace(format)
}

func procMath(proc *ProcessorDef, doc *Document) error {
	if proc.Expression == "" {
		return fmt.Errorf("math: missing 'expression' parameter")
	}
	to := proc.To
	if to == "" {
		to = proc.Field
	}
	if to == "" {
		return fmt.Errorf("math: missing 'to' parameter")
	}

	result, err := evalMathExpr(proc.Expression, doc, boolDefault(proc.IgnoreMissing, false))
	if err != nil {
		return fmt.Errorf("math: %w", err)
	}

	doc.Set(to, result)
	return nil
}

func evalMathExpr(expr string, doc *Document, ignoreMissing bool) (float64, error) {
	p := &mathParser{input: expr, doc: doc, ignoreMissing: ignoreMissing}
	result, err := p.parseExpr()
	if err != nil {
		return 0, err
	}
	p.skipSpace()
	if p.pos < len(p.input) {
		return 0, fmt.Errorf("unexpected character at position %d: %q", p.pos, string(p.input[p.pos]))
	}
	return result, nil
}

type mathParser struct {
	input         string
	pos           int
	doc           *Document
	ignoreMissing bool
}

func (p *mathParser) skipSpace() {
	for p.pos < len(p.input) && p.input[p.pos] == ' ' {
		p.pos++
	}
}

func (p *mathParser) parseExpr() (float64, error) {
	return p.parseAddSub()
}

func (p *mathParser) parseAddSub() (float64, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return 0, err
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.input) {
			return left, nil
		}
		op := p.input[p.pos]
		if op != '+' && op != '-' {
			return left, nil
		}
		p.pos++
		right, err := p.parseMulDiv()
		if err != nil {
			return 0, err
		}
		if op == '+' {
			left += right
		} else {
			left -= right
		}
	}
}

func (p *mathParser) parseMulDiv() (float64, error) {
	left, err := p.parseUnary()
	if err != nil {
		return 0, err
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.input) {
			return left, nil
		}
		op := p.input[p.pos]
		if op != '*' && op != '/' && op != '%' {
			return left, nil
		}
		p.pos++
		right, err := p.parseUnary()
		if err != nil {
			return 0, err
		}
		switch op {
		case '*':
			left *= right
		case '/':
			if right == 0 {
				return 0, fmt.Errorf("division by zero")
			}
			left /= right
		case '%':
			if right == 0 {
				return 0, fmt.Errorf("modulo by zero")
			}
			left = math.Mod(left, right)
		}
	}
}

func (p *mathParser) parseUnary() (float64, error) {
	p.skipSpace()
	if p.pos < len(p.input) && p.input[p.pos] == '-' {
		p.pos++
		val, err := p.parsePrimary()
		if err != nil {
			return 0, err
		}
		return -val, nil
	}
	return p.parsePrimary()
}

func (p *mathParser) parsePrimary() (float64, error) {
	p.skipSpace()
	if p.pos >= len(p.input) {
		return 0, fmt.Errorf("unexpected end of expression")
	}

	if p.input[p.pos] == '(' {
		p.pos++
		val, err := p.parseExpr()
		if err != nil {
			return 0, err
		}
		p.skipSpace()
		if p.pos >= len(p.input) || p.input[p.pos] != ')' {
			return 0, fmt.Errorf("expected closing parenthesis")
		}
		p.pos++
		return val, nil
	}

	if p.input[p.pos] >= '0' && p.input[p.pos] <= '9' || p.input[p.pos] == '.' {
		start := p.pos
		for p.pos < len(p.input) && (p.input[p.pos] >= '0' && p.input[p.pos] <= '9' || p.input[p.pos] == '.') {
			p.pos++
		}
		f, err := strconv.ParseFloat(p.input[start:p.pos], 64)
		if err != nil {
			return 0, fmt.Errorf("invalid number %q", p.input[start:p.pos])
		}
		return f, nil
	}

	// Field reference.
	start := p.pos
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' {
			p.pos++
		} else {
			break
		}
	}
	if p.pos == start {
		return 0, fmt.Errorf("unexpected character %q at position %d", string(p.input[p.pos]), p.pos)
	}
	fieldName := p.input[start:p.pos]
	val, ok := p.doc.GetAny(fieldName)
	if !ok {
		if p.ignoreMissing {
			return 0, nil
		}
		return 0, fmt.Errorf("field %q does not exist", fieldName)
	}
	f, ok := toFloat64(val)
	if !ok {
		if s, sOk := val.(string); sOk {
			var err error
			f, err = strconv.ParseFloat(s, 64)
			if err != nil {
				return 0, fmt.Errorf("field %q is not numeric: %v", fieldName, val)
			}
			return f, nil
		}
		return 0, fmt.Errorf("field %q is not numeric: %v", fieldName, val)
	}
	return f, nil
}

func procJSONExtract(proc *ProcessorDef, doc *Document) error {
	field := proc.Field
	if field == "" {
		field = proc.From
	}
	if field == "" {
		return fmt.Errorf("json_extract: missing 'field' parameter")
	}

	val, ok := doc.GetAny(field)
	if !ok {
		if boolDefault(proc.IgnoreMissing, false) {
			return nil
		}
		return fmt.Errorf("json_extract: field %q does not exist", field)
	}

	if len(proc.Extractions) == 0 {
		return fmt.Errorf("json_extract: missing 'extractions' parameter")
	}

	var parsed interface{}
	if err := json.Unmarshal([]byte(toString(val)), &parsed); err != nil {
		return fmt.Errorf("json_extract: failed to parse JSON from field %q: %w", field, err)
	}

	for _, ext := range proc.Extractions {
		if ext.Selector == "" || ext.TargetField == "" {
			return fmt.Errorf("json_extract: extraction requires 'selector' and 'target_field'")
		}

		extracted := jsonPathSelect(parsed, ext.Selector)
		if extracted == nil {
			continue
		}

		if ext.Type != "" {
			converted, err := convertValue(extracted, ext.Type)
			if err == nil {
				extracted = converted
			}
		}

		doc.Set(ext.TargetField, extracted)
	}

	return nil
}

func jsonPathSelect(data interface{}, selector string) interface{} {
	selector = strings.TrimPrefix(selector, "$.")
	parts := splitJSONPath(selector)
	current := data
	for _, part := range parts {
		if current == nil {
			return nil
		}
		if idx := strings.Index(part, "["); idx >= 0 {
			key := part[:idx]
			indexStr := strings.TrimSuffix(part[idx+1:], "]")
			arrayIdx, err := strconv.Atoi(indexStr)
			if err != nil {
				return nil
			}
			if key != "" {
				m, ok := current.(map[string]interface{})
				if !ok {
					return nil
				}
				current = m[key]
			}
			arr, ok := current.([]interface{})
			if !ok || arrayIdx < 0 || arrayIdx >= len(arr) {
				return nil
			}
			current = arr[arrayIdx]
		} else {
			m, ok := current.(map[string]interface{})
			if !ok {
				return nil
			}
			current = m[part]
		}
	}
	return current
}

func splitJSONPath(path string) []string {
	var parts []string
	var current strings.Builder
	for i := 0; i < len(path); i++ {
		if path[i] == '.' && current.Len() > 0 {
			parts = append(parts, current.String())
			current.Reset()
		} else {
			current.WriteByte(path[i])
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

func procDropDocument(_ *ProcessorDef, _ *Document) error {
	return ErrDropDocument
}

func procNetworkDirection(proc *ProcessorDef, doc *Document) error {
	if proc.SourceIP == "" {
		return fmt.Errorf("network_direction: missing 'source_ip' parameter")
	}
	if proc.DestinationIP == "" {
		return fmt.Errorf("network_direction: missing 'destination_ip' parameter")
	}

	srcVal, srcOk := doc.GetAny(proc.SourceIP)
	dstVal, dstOk := doc.GetAny(proc.DestinationIP)

	if !srcOk || !dstOk {
		if boolDefault(proc.IgnoreMissing, false) {
			return nil
		}
		return fmt.Errorf("network_direction: source or destination IP field missing")
	}

	srcIP := toString(srcVal)
	dstIP := toString(dstVal)

	var internalNetworks []string
	if len(proc.InternalNetworks) > 0 {
		internalNetworks = proc.InternalNetworks
	} else if proc.InternalNetworksField != "" {
		val, ok := doc.GetAny(proc.InternalNetworksField)
		if !ok {
			return fmt.Errorf("network_direction: internal_networks_field %q does not exist", proc.InternalNetworksField)
		}
		if slice, ok := val.([]interface{}); ok {
			for _, item := range slice {
				internalNetworks = append(internalNetworks, toString(item))
			}
		}
	}

	srcInternal := isInternalIP(srcIP, internalNetworks)
	dstInternal := isInternalIP(dstIP, internalNetworks)

	var direction string
	switch {
	case srcInternal && dstInternal:
		direction = "internal"
	case srcInternal && !dstInternal:
		direction = "outbound"
	case !srcInternal && dstInternal:
		direction = "inbound"
	default:
		direction = "external"
	}

	targetField := "network.direction"
	if proc.TargetField != "" {
		targetField = proc.TargetField
	}

	doc.Set(targetField, direction)
	return nil
}

func isInternalIP(ipStr string, networks []string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}

	for _, network := range networks {
		switch network {
		case "loopback":
			if ip.IsLoopback() {
				return true
			}
		case "unicast", "global_unicast":
			if ip.IsGlobalUnicast() {
				return true
			}
		case "multicast":
			if ip.IsMulticast() {
				return true
			}
		case "link_local_unicast":
			if ip.IsLinkLocalUnicast() {
				return true
			}
		case "link_local_multicast":
			if ip.IsLinkLocalMulticast() {
				return true
			}
		case "private":
			if isPrivateIP(ip) {
				return true
			}
		default:
			_, cidr, err := net.ParseCIDR(network)
			if err == nil && cidr.Contains(ip) {
				return true
			}
		}
	}
	return false
}

func isPrivateIP(ip net.IP) bool {
	privateRanges := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"fd00::/8",
	}
	for _, cidr := range privateRanges {
		_, network, _ := net.ParseCIDR(cidr)
		if network.Contains(ip) {
			return true
		}
	}
	return false
}
