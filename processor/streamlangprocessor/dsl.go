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
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/types"
)

// ProcessorType identifies a streamlang processor action.
type ProcessorType = string

const (
	ProcessorSet              ProcessorType = "set"
	ProcessorRename           ProcessorType = "rename"
	ProcessorRemove           ProcessorType = "remove"
	ProcessorRemoveByPrefix   ProcessorType = "remove_by_prefix"
	ProcessorConvert          ProcessorType = "convert"
	ProcessorUppercase        ProcessorType = "uppercase"
	ProcessorLowercase        ProcessorType = "lowercase"
	ProcessorTrim             ProcessorType = "trim"
	ProcessorGrok             ProcessorType = "grok"
	ProcessorDissect          ProcessorType = "dissect"
	ProcessorReplace          ProcessorType = "replace"
	ProcessorRedact           ProcessorType = "redact"
	ProcessorSplit            ProcessorType = "split"
	ProcessorJoin             ProcessorType = "join"
	ProcessorSort             ProcessorType = "sort"
	ProcessorAppend           ProcessorType = "append"
	ProcessorConcat           ProcessorType = "concat"
	ProcessorDate             ProcessorType = "date"
	ProcessorMath             ProcessorType = "math"
	ProcessorJSONExtract      ProcessorType = "json_extract"
	ProcessorDropDocument     ProcessorType = "drop_document"
	ProcessorNetworkDirection ProcessorType = "network_direction"
)

// ErrDropDocument is a sentinel error returned by Execute when a drop_document
// processor fires. The caller should remove the log record from the batch.
var ErrDropDocument = types.ErrDropDocument

// ProcessorFunc is the signature for individual processor operation implementations.
type ProcessorFunc func(proc *ProcessorDef, doc *Document) error

// DSL is the root streamlang configuration.
type DSL struct {
	Steps []Step `yaml:"steps" mapstructure:"steps"`
}

// Step is either a processor definition or a condition block.
type Step struct {
	Processor *ProcessorDef   `yaml:"processor,omitempty" mapstructure:"processor,omitempty"`
	Condition *ConditionBlock `yaml:"condition,omitempty" mapstructure:"condition,omitempty"`
}

// ConditionBlock wraps a condition with steps and an optional else branch.
type ConditionBlock struct {
	Condition *Condition `yaml:",inline" mapstructure:",squash"`
	Steps     []Step     `yaml:"steps" mapstructure:"steps"`
	Else      []Step     `yaml:"else,omitempty" mapstructure:"else,omitempty"`
}

// ProcessorDef is a single processor step.
type ProcessorDef struct {
	Action           string     `yaml:"action" mapstructure:"action"`
	CustomIdentifier string     `yaml:"custom_identifier,omitempty" mapstructure:"custom_identifier,omitempty"`
	Description      string     `yaml:"description,omitempty" mapstructure:"description,omitempty"`
	IgnoreFailure    bool       `yaml:"ignore_failure,omitempty" mapstructure:"ignore_failure,omitempty"`
	Where            *Condition `yaml:"where,omitempty" mapstructure:"where,omitempty"`

	From  string `yaml:"from,omitempty" mapstructure:"from,omitempty"`
	To    string `yaml:"to,omitempty" mapstructure:"to,omitempty"`
	Field string `yaml:"field,omitempty" mapstructure:"field,omitempty"`

	// set
	Value    interface{} `yaml:"value,omitempty" mapstructure:"value,omitempty"`
	CopyFrom string      `yaml:"copy_from,omitempty" mapstructure:"copy_from,omitempty"`
	Override *bool       `yaml:"override,omitempty" mapstructure:"override,omitempty"`

	// grok / redact
	Patterns           []string          `yaml:"patterns,omitempty" mapstructure:"patterns,omitempty"`
	PatternDefinitions map[string]string `yaml:"pattern_definitions,omitempty" mapstructure:"pattern_definitions,omitempty"`

	// dissect
	Pattern         string `yaml:"pattern,omitempty" mapstructure:"pattern,omitempty"`
	AppendSeparator string `yaml:"append_separator,omitempty" mapstructure:"append_separator,omitempty"`

	// date
	Formats      []string `yaml:"formats,omitempty" mapstructure:"formats,omitempty"`
	OutputFormat string   `yaml:"output_format,omitempty" mapstructure:"output_format,omitempty"`
	Timezone     string   `yaml:"timezone,omitempty" mapstructure:"timezone,omitempty"`
	Locale       string   `yaml:"locale,omitempty" mapstructure:"locale,omitempty"`

	// convert
	Type string `yaml:"type,omitempty" mapstructure:"type,omitempty"`

	// replace
	Replacement string `yaml:"replacement,omitempty" mapstructure:"replacement,omitempty"`

	// redact
	Prefix string `yaml:"prefix,omitempty" mapstructure:"prefix,omitempty"`
	Suffix string `yaml:"suffix,omitempty" mapstructure:"suffix,omitempty"`

	// math
	Expression string `yaml:"expression,omitempty" mapstructure:"expression,omitempty"`

	// append
	Values          []interface{} `yaml:"values,omitempty" mapstructure:"values,omitempty"`
	AllowDuplicates *bool         `yaml:"allow_duplicates,omitempty" mapstructure:"allow_duplicates,omitempty"`

	// join
	FromFields []string `yaml:"from_fields,omitempty" mapstructure:"from_fields,omitempty"`
	Delimiter  string   `yaml:"delimiter,omitempty" mapstructure:"delimiter,omitempty"`

	// split
	Separator        string `yaml:"separator,omitempty" mapstructure:"separator,omitempty"`
	PreserveTrailing *bool  `yaml:"preserve_trailing,omitempty" mapstructure:"preserve_trailing,omitempty"`

	// sort
	Order string `yaml:"order,omitempty" mapstructure:"order,omitempty"`

	// json_extract
	Extractions []JsonExtraction `yaml:"extractions,omitempty" mapstructure:"extractions,omitempty"`

	// concat
	ConcatFrom []ConcatSource `yaml:"concat_from,omitempty" mapstructure:"concat_from,omitempty"`

	// network_direction
	SourceIP              string   `yaml:"source_ip,omitempty" mapstructure:"source_ip,omitempty"`
	DestinationIP         string   `yaml:"destination_ip,omitempty" mapstructure:"destination_ip,omitempty"`
	TargetField           string   `yaml:"target_field,omitempty" mapstructure:"target_field,omitempty"`
	InternalNetworks      []string `yaml:"internal_networks,omitempty" mapstructure:"internal_networks,omitempty"`
	InternalNetworksField string   `yaml:"internal_networks_field,omitempty" mapstructure:"internal_networks_field,omitempty"`

	// Common optional
	IgnoreMissing *bool `yaml:"ignore_missing,omitempty" mapstructure:"ignore_missing,omitempty"`
}

// JsonExtraction defines a single extraction from a JSON field.
type JsonExtraction struct {
	Selector    string `yaml:"selector" mapstructure:"selector"`
	TargetField string `yaml:"target_field" mapstructure:"target_field"`
	Type        string `yaml:"type,omitempty" mapstructure:"type,omitempty"`
}

// ConcatSource is either a field reference or a literal string.
type ConcatSource struct {
	Type  string `yaml:"type" mapstructure:"type"`
	Value string `yaml:"value" mapstructure:"value"`
}

// Condition represents a recursive condition tree.
type Condition struct {
	Field      string      `yaml:"field,omitempty" mapstructure:"field,omitempty"`
	Eq         interface{} `yaml:"eq,omitempty" mapstructure:"eq,omitempty"`
	Neq        interface{} `yaml:"neq,omitempty" mapstructure:"neq,omitempty"`
	Lt         interface{} `yaml:"lt,omitempty" mapstructure:"lt,omitempty"`
	Lte        interface{} `yaml:"lte,omitempty" mapstructure:"lte,omitempty"`
	Gt         interface{} `yaml:"gt,omitempty" mapstructure:"gt,omitempty"`
	Gte        interface{} `yaml:"gte,omitempty" mapstructure:"gte,omitempty"`
	Contains   interface{} `yaml:"contains,omitempty" mapstructure:"contains,omitempty"`
	StartsWith interface{} `yaml:"starts_with,omitempty" mapstructure:"starts_with,omitempty"`
	EndsWith   interface{} `yaml:"ends_with,omitempty" mapstructure:"ends_with,omitempty"`
	Exists     *bool       `yaml:"exists,omitempty" mapstructure:"exists,omitempty"`
	Includes   interface{} `yaml:"includes,omitempty" mapstructure:"includes,omitempty"`
	Range      *RangeCond  `yaml:"range,omitempty" mapstructure:"range,omitempty"`

	And []Condition `yaml:"and,omitempty" mapstructure:"and,omitempty"`
	Or  []Condition `yaml:"or,omitempty" mapstructure:"or,omitempty"`
	Not *Condition  `yaml:"not,omitempty" mapstructure:"not,omitempty"`

	Always *struct{} `yaml:"always,omitempty" mapstructure:"always,omitempty"`
	Never  *struct{} `yaml:"never,omitempty" mapstructure:"never,omitempty"`
}

// RangeCond defines range boundaries.
type RangeCond struct {
	Gt  interface{} `yaml:"gt,omitempty" mapstructure:"gt,omitempty"`
	Gte interface{} `yaml:"gte,omitempty" mapstructure:"gte,omitempty"`
	Lt  interface{} `yaml:"lt,omitempty" mapstructure:"lt,omitempty"`
	Lte interface{} `yaml:"lte,omitempty" mapstructure:"lte,omitempty"`
}

// FlatStep is a flattened representation of a pipeline step.
type FlatStep struct {
	Proc *ProcessorDef
	Cond *Condition
}

// FlattenDSLSteps recursively flattens nested condition blocks from the DSL
// config into a flat list of steps with combined conditions. Used by the
// closure backend.
func FlattenDSLSteps(steps []Step, parentCond *Condition) []FlatStep {
	var result []FlatStep
	for _, step := range steps {
		if step.Condition != nil {
			cb := step.Condition
			cond := cb.Condition
			combined := combineAnd(parentCond, cond)

			result = append(result, FlattenDSLSteps(cb.Steps, combined)...)

			if len(cb.Else) > 0 {
				negated := &Condition{Not: cond}
				combinedElse := combineAnd(parentCond, negated)
				result = append(result, FlattenDSLSteps(cb.Else, combinedElse)...)
			}
		} else if step.Processor != nil {
			proc := step.Processor
			var finalCond *Condition
			if proc.Where != nil {
				finalCond = combineAnd(parentCond, proc.Where)
			} else {
				finalCond = parentCond
			}
			result = append(result, FlatStep{
				Proc: proc,
				Cond: finalCond,
			})
		}
	}
	return result
}

// StreamlangStep is a simplified step representation used by tests.
// It mirrors the Kibana test format: either a processor action or a conditional block.
type StreamlangStep struct {
	Action    *ProcessorDef
	Condition *Condition
	Steps     []StreamlangStep
	Else      []StreamlangStep
}

// FlattenSteps flattens StreamlangSteps into FlatSteps for the closure backend.
func FlattenSteps(steps []StreamlangStep) []FlatStep {
	return flattenStreamlangSteps(steps, nil)
}

func flattenStreamlangSteps(steps []StreamlangStep, parentCond *Condition) []FlatStep {
	var result []FlatStep
	for _, step := range steps {
		if step.Condition != nil {
			cond := step.Condition
			combined := combineAnd(parentCond, cond)
			result = append(result, flattenStreamlangSteps(step.Steps, combined)...)
			if len(step.Else) > 0 {
				negated := &Condition{Not: cond}
				combinedElse := combineAnd(parentCond, negated)
				result = append(result, flattenStreamlangSteps(step.Else, combinedElse)...)
			}
		} else if step.Action != nil {
			proc := step.Action
			var finalCond *Condition
			if proc.Where != nil {
				finalCond = combineAnd(parentCond, proc.Where)
			} else {
				finalCond = parentCond
			}
			result = append(result, FlatStep{
				Proc: proc,
				Cond: finalCond,
			})
		}
	}
	return result
}

func combineAnd(a, b *Condition) *Condition {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return &Condition{
		And: []Condition{*a, *b},
	}
}
