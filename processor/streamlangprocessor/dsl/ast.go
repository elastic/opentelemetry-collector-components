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

// Package dsl contains the Go AST and parser for the Streamlang DSL.
//
// The schemas mirror the TypeScript definitions in
// kibana/x-pack/platform/packages/shared/kbn-streamlang/types and the
// Rust reference implementation in streamlang-runtime/src.
package dsl // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"

import "errors"

// ErrUnsupportedAction is returned by Parse when it encounters a processor
// action that is recognised by the schema but not yet implemented in this
// (Phase 1) parser.
var ErrUnsupportedAction = errors.New("unsupported streamlang action")

// ConvertType is the target data type for the convert processor.
type ConvertType string

// Supported convert types (mirrors formats/convert_types.ts).
const (
	ConvertTypeInteger ConvertType = "integer"
	ConvertTypeLong    ConvertType = "long"
	ConvertTypeDouble  ConvertType = "double"
	ConvertTypeBoolean ConvertType = "boolean"
	ConvertTypeString  ConvertType = "string"
)

// Step is the sealed sum type implemented by Processor types and ConditionBlock.
type Step interface {
	isStep()
}

// Processor is the common interface for all processor steps.
type Processor interface {
	Step
	Action() string
	Base() *ProcessorBase
}

// ProcessorBase holds the fields shared by every processor.
type ProcessorBase struct {
	CustomIdentifier string
	Description      string
	IgnoreFailure    bool
	Where            *Condition
}

// Base returns a pointer to this processor's ProcessorBase.
func (p *ProcessorBase) Base() *ProcessorBase { return p }

// DSL is the root Streamlang program: a flat list of steps.
type DSL struct {
	Steps []Step
}

// ConditionBlock wraps a list of nested steps with a condition.
type ConditionBlock struct {
	CustomIdentifier string
	// Condition is required (non-nil after parsing).
	Condition *Condition
	Steps     []Step
}

func (*ConditionBlock) isStep() {}

// --- Concrete processors (Phase 1 scope) -----------------------------------

// SetProcessor — assigns a literal value or copies from another field.
type SetProcessor struct {
	ProcessorBase
	To       string
	Override *bool
	// Exactly one of Value / CopyFrom is non-zero (validated by parser).
	Value    any
	HasValue bool
	CopyFrom string
}

func (*SetProcessor) isStep()        {}
func (*SetProcessor) Action() string { return "set" }

// AppendProcessor — appends one or more values to an array field.
type AppendProcessor struct {
	ProcessorBase
	To              string
	Value           []any
	AllowDuplicates *bool
}

func (*AppendProcessor) isStep()        {}
func (*AppendProcessor) Action() string { return "append" }

// RemoveProcessor — removes a single field.
type RemoveProcessor struct {
	ProcessorBase
	From          string
	IgnoreMissing *bool
}

func (*RemoveProcessor) isStep()        {}
func (*RemoveProcessor) Action() string { return "remove" }

// RemoveByPrefixProcessor — removes a field and any nested fields. Has no `where`.
type RemoveByPrefixProcessor struct {
	ProcessorBase
	From string
}

func (*RemoveByPrefixProcessor) isStep()        {}
func (*RemoveByPrefixProcessor) Action() string { return "remove_by_prefix" }

// RenameProcessor — renames / moves a field.
type RenameProcessor struct {
	ProcessorBase
	From          string
	To            string
	IgnoreMissing *bool
	Override      *bool
}

func (*RenameProcessor) isStep()        {}
func (*RenameProcessor) Action() string { return "rename" }

// DropDocumentProcessor — drops the document when its required `where` matches.
type DropDocumentProcessor struct {
	ProcessorBase
}

func (*DropDocumentProcessor) isStep()        {}
func (*DropDocumentProcessor) Action() string { return "drop_document" }

// UppercaseProcessor — uppercases a string field.
type UppercaseProcessor struct {
	ProcessorBase
	From          string
	To            string
	IgnoreMissing *bool
}

func (*UppercaseProcessor) isStep()        {}
func (*UppercaseProcessor) Action() string { return "uppercase" }

// LowercaseProcessor — lowercases a string field.
type LowercaseProcessor struct {
	ProcessorBase
	From          string
	To            string
	IgnoreMissing *bool
}

func (*LowercaseProcessor) isStep()        {}
func (*LowercaseProcessor) Action() string { return "lowercase" }

// TrimProcessor — trims whitespace from a string field.
type TrimProcessor struct {
	ProcessorBase
	From          string
	To            string
	IgnoreMissing *bool
}

func (*TrimProcessor) isStep()        {}
func (*TrimProcessor) Action() string { return "trim" }

// ReplaceProcessor — replaces occurrences of a substring/regex.
type ReplaceProcessor struct {
	ProcessorBase
	From          string
	Pattern       string
	Replacement   string
	To            string
	IgnoreMissing *bool
}

func (*ReplaceProcessor) isStep()        {}
func (*ReplaceProcessor) Action() string { return "replace" }

// ConvertProcessor — converts a field value to a different data type.
type ConvertProcessor struct {
	ProcessorBase
	From          string
	To            string
	Type          ConvertType
	IgnoreMissing *bool
}

func (*ConvertProcessor) isStep()        {}
func (*ConvertProcessor) Action() string { return "convert" }

// --- Conditions ------------------------------------------------------------

// ConditionKind identifies the variant of a Condition value.
type ConditionKind int

const (
	// CondInvalid is the zero value; never produced by the parser.
	CondInvalid ConditionKind = iota
	CondAlways
	CondNever
	CondAnd
	CondOr
	CondNot
	CondFilter
)

// Condition is a tagged union representing every Streamlang condition shape.
//
// Exactly one of the variant fields is populated, identified by Kind.
type Condition struct {
	Kind ConditionKind

	// CondAnd / CondOr
	And []Condition
	Or  []Condition

	// CondNot
	Not *Condition

	// CondFilter
	Filter *FilterCondition
}

// FilterCondition is a leaf condition that matches a field against an operator.
//
// Exactly one operator field is populated. Operand values are kept as `any`
// (string | float64 | bool) — the evaluator coerces.
type FilterCondition struct {
	Field string

	// Binary operators — exactly one of these is set (or Range / Exists).
	Eq         any
	HasEq      bool
	Neq        any
	HasNeq     bool
	Lt         any
	HasLt      bool
	Lte        any
	HasLte     bool
	Gt         any
	HasGt      bool
	Gte        any
	HasGte     bool
	Contains   any
	HasContain bool
	StartsWith any
	HasStarts  bool
	EndsWith   any
	HasEnds    bool
	Includes   any
	HasIncl    bool

	// Range comparison.
	Range *RangeCondition

	// Unary "exists" check.
	Exists *bool
}

// RangeCondition is a numeric/lexical range used by FilterCondition.Range.
type RangeCondition struct {
	Gt  any
	Gte any
	Lt  any
	Lte any
}

// FlatProcessor is a processor with its (possibly combined) where condition,
// produced by Flatten.
type FlatProcessor struct {
	Processor Processor
	Where     *Condition
}

// --- Phase 2 processors -----------------------------------------------------

// GrokProcessor — extracts fields from text using grok patterns.
type GrokProcessor struct {
	ProcessorBase
	From               string
	Patterns           []string
	PatternDefinitions map[string]string
	IgnoreMissing      *bool
}

func (*GrokProcessor) isStep()        {}
func (*GrokProcessor) Action() string { return "grok" }

// DissectProcessor — extracts fields from text using a dissect pattern.
type DissectProcessor struct {
	ProcessorBase
	From            string
	Pattern         string
	AppendSeparator string
	IgnoreMissing   *bool
}

func (*DissectProcessor) isStep()        {}
func (*DissectProcessor) Action() string { return "dissect" }

// DateProcessor — parses dates from strings using one or more expected formats.
type DateProcessor struct {
	ProcessorBase
	From         string
	To           string
	Formats      []string
	OutputFormat string
	Timezone     string
	Locale       string
}

func (*DateProcessor) isStep()        {}
func (*DateProcessor) Action() string { return "date" }

// RedactProcessor — masks sensitive data using grok patterns.
type RedactProcessor struct {
	ProcessorBase
	From               string
	Patterns           []string
	PatternDefinitions map[string]string
	Prefix             string
	Suffix             string
	IgnoreMissing      *bool
}

func (*RedactProcessor) isStep()        {}
func (*RedactProcessor) Action() string { return "redact" }

// MathProcessor — evaluates a TinyMath expression and stores the result.
type MathProcessor struct {
	ProcessorBase
	Expression    string
	To            string
	IgnoreMissing *bool
}

func (*MathProcessor) isStep()        {}
func (*MathProcessor) Action() string { return "math" }

// JSONExtractType is the target data type for a single JSON extraction.
type JSONExtractType string

// Supported json_extract types (mirrors jsonExtractTypes in index.ts).
const (
	JSONExtractTypeKeyword JSONExtractType = "keyword"
	JSONExtractTypeInteger JSONExtractType = "integer"
	JSONExtractTypeLong    JSONExtractType = "long"
	JSONExtractTypeDouble  JSONExtractType = "double"
	JSONExtractTypeBoolean JSONExtractType = "boolean"
)

// JSONExtraction is one selector → target_field mapping inside a json_extract
// processor.
type JSONExtraction struct {
	Selector    string
	TargetField string
	Type        string
}

// JSONExtractProcessor — extracts values from JSON strings via JSONPath-like
// selectors.
type JSONExtractProcessor struct {
	ProcessorBase
	Field         string
	Extractions   []JSONExtraction
	IgnoreMissing *bool
}

func (*JSONExtractProcessor) isStep()        {}
func (*JSONExtractProcessor) Action() string { return "json_extract" }

// NetworkDirectionProcessor — derives a network.direction value.
//
// Exactly one of InternalNetworks / InternalNetworksField is populated
// (validated by the parser).
type NetworkDirectionProcessor struct {
	ProcessorBase
	SourceIP              string
	DestinationIP         string
	TargetField           string
	IgnoreMissing         *bool
	InternalNetworks      []string
	InternalNetworksField string
}

func (*NetworkDirectionProcessor) isStep()        {}
func (*NetworkDirectionProcessor) Action() string { return "network_direction" }

// SplitProcessor — splits a string field into an array using a separator.
type SplitProcessor struct {
	ProcessorBase
	From             string
	Separator        string
	To               string
	IgnoreMissing    *bool
	PreserveTrailing *bool
}

func (*SplitProcessor) isStep()        {}
func (*SplitProcessor) Action() string { return "split" }

// SortOrder is the ordering for the sort processor.
type SortOrder string

// Supported sort orders (mirrors sortOrders in index.ts).
const (
	SortOrderAsc  SortOrder = "asc"
	SortOrderDesc SortOrder = "desc"
)

// SortProcessor — sorts an array field.
type SortProcessor struct {
	ProcessorBase
	From          string
	To            string
	Order         SortOrder
	IgnoreMissing *bool
}

func (*SortProcessor) isStep()        {}
func (*SortProcessor) Action() string { return "sort" }

// JoinProcessor — joins multiple source fields into a single string.
type JoinProcessor struct {
	ProcessorBase
	From          []string
	Delimiter     string
	To            string
	IgnoreMissing *bool
}

func (*JoinProcessor) isStep()        {}
func (*JoinProcessor) Action() string { return "join" }

// ConcatPartType identifies the kind of a concat input element.
type ConcatPartType string

// Supported concat part types (mirrors ConcatFromField / ConcatFromLiteral).
const (
	ConcatPartField   ConcatPartType = "field"
	ConcatPartLiteral ConcatPartType = "literal"
)

// ConcatPart is a single component of a concat processor's `from` array.
type ConcatPart struct {
	Type  ConcatPartType
	Value string
}

// ConcatProcessor — concatenates literal and field values into a target field.
type ConcatProcessor struct {
	ProcessorBase
	From          []ConcatPart
	To            string
	IgnoreMissing *bool
}

func (*ConcatProcessor) isStep()        {}
func (*ConcatProcessor) Action() string { return "concat" }
