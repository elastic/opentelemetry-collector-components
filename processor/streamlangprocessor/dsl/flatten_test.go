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

package dsl

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// helper: build a filter equality condition.
func filterEq(field string, value any) *Condition {
	return &Condition{
		Kind: CondFilter,
		Filter: &FilterCondition{
			Field: field,
			Eq:    value,
			HasEq: true,
		},
	}
}

func TestFlatten_NoConditionBlocks(t *testing.T) {
	d := DSL{Steps: []Step{
		&SetProcessor{ProcessorBase: ProcessorBase{}, To: "x", Value: 1, HasValue: true},
		&RemoveProcessor{ProcessorBase: ProcessorBase{Where: filterEq("a", "b")}, From: "y"},
	}}
	out := Flatten(d)
	require.Len(t, out, 2)
	require.Nil(t, out[0].Where)
	require.NotNil(t, out[1].Where)
	require.Equal(t, CondFilter, out[1].Where.Kind)
}

func TestFlatten_NestedThreeDeep(t *testing.T) {
	leaf := &SetProcessor{To: "x", Value: 1, HasValue: true}
	innermost := &ConditionBlock{Condition: filterEq("c", "3"), Steps: []Step{leaf}}
	mid := &ConditionBlock{Condition: filterEq("b", "2"), Steps: []Step{innermost}}
	outer := &ConditionBlock{Condition: filterEq("a", "1"), Steps: []Step{mid}}

	out := Flatten(DSL{Steps: []Step{outer}})
	require.Len(t, out, 1)
	require.Equal(t, leaf, out[0].Processor)

	// Combined: AND(AND(a=1, b=2), c=3)
	w := out[0].Where
	require.NotNil(t, w)
	require.Equal(t, CondAnd, w.Kind)
	require.Len(t, w.And, 2)
	left := w.And[0]
	right := w.And[1]
	require.Equal(t, CondAnd, left.Kind)
	require.Len(t, left.And, 2)
	require.True(t, reflect.DeepEqual(left.And[0], *filterEq("a", "1")))
	require.True(t, reflect.DeepEqual(left.And[1], *filterEq("b", "2")))
	require.True(t, reflect.DeepEqual(right, *filterEq("c", "3")))
}

func TestFlatten_AlwaysShortcut(t *testing.T) {
	leaf := &SetProcessor{To: "x", Value: 1, HasValue: true}
	block := &ConditionBlock{
		Condition: &Condition{Kind: CondAlways},
		Steps:     []Step{leaf},
	}
	out := Flatten(DSL{Steps: []Step{block}})
	require.Len(t, out, 1)
	require.Nil(t, out[0].Where, "always-block should not contribute a where")
}

func TestFlatten_AlwaysCombinesWithChildWhere(t *testing.T) {
	leaf := &SetProcessor{
		ProcessorBase: ProcessorBase{Where: filterEq("c", "3")},
		To:            "x", Value: 1, HasValue: true,
	}
	block := &ConditionBlock{
		Condition: &Condition{Kind: CondAlways},
		Steps:     []Step{leaf},
	}
	out := Flatten(DSL{Steps: []Step{block}})
	require.Len(t, out, 1)
	require.NotNil(t, out[0].Where)
	require.Equal(t, CondFilter, out[0].Where.Kind, "always(parent) AND child should be just child")
}

func TestFlatten_NeverShortcut(t *testing.T) {
	leaf := &SetProcessor{To: "x", Value: 1, HasValue: true}
	block := &ConditionBlock{
		Condition: &Condition{Kind: CondNever},
		Steps:     []Step{leaf},
	}
	out := Flatten(DSL{Steps: []Step{block}})
	require.Empty(t, out, "never-block contributes no processors")
}

func TestFlatten_NeverChildWhereDropsProcessor(t *testing.T) {
	d := DSL{Steps: []Step{
		&SetProcessor{
			ProcessorBase: ProcessorBase{Where: &Condition{Kind: CondNever}},
			To:            "x", Value: 1, HasValue: true,
		},
		&RemoveProcessor{From: "y"},
	}}
	out := Flatten(d)
	require.Len(t, out, 1)
	_, ok := out[0].Processor.(*RemoveProcessor)
	require.True(t, ok)
}

func TestFlatten_MixedProcessorsWithOwnWhere(t *testing.T) {
	leafWithWhere := &SetProcessor{
		ProcessorBase: ProcessorBase{Where: filterEq("inner", "v")},
		To:            "x", Value: 1, HasValue: true,
	}
	leafWithoutWhere := &RemoveProcessor{From: "y"}

	block := &ConditionBlock{
		Condition: filterEq("outer", "v"),
		Steps:     []Step{leafWithWhere, leafWithoutWhere},
	}
	out := Flatten(DSL{Steps: []Step{block}})
	require.Len(t, out, 2)

	require.Equal(t, CondAnd, out[0].Where.Kind)
	require.Len(t, out[0].Where.And, 2)
	require.True(t, reflect.DeepEqual(out[0].Where.And[0], *filterEq("outer", "v")))
	require.True(t, reflect.DeepEqual(out[0].Where.And[1], *filterEq("inner", "v")))

	require.Equal(t, CondFilter, out[1].Where.Kind)
}

func TestFlatten_E2EFromParse(t *testing.T) {
	d, err := Parse([]any{
		map[string]any{
			"customIdentifier": "outer",
			"condition": map[string]any{
				"field": "level",
				"eq":    "error",
				"steps": []any{
					map[string]any{
						"action": "set",
						"to":     "x",
						"value":  1,
					},
					map[string]any{
						"condition": map[string]any{
							"field": "service",
							"eq":    "web",
							"steps": []any{
								map[string]any{
									"action": "rename",
									"from":   "a",
									"to":     "b",
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	out := Flatten(d)
	require.Len(t, out, 2)

	// First: SetProcessor with where = level=error
	setP, ok := out[0].Processor.(*SetProcessor)
	require.True(t, ok)
	require.Equal(t, "x", setP.To)
	require.NotNil(t, out[0].Where)
	require.Equal(t, CondFilter, out[0].Where.Kind)
	require.Equal(t, "level", out[0].Where.Filter.Field)

	// Second: RenameProcessor with where = AND(level=error, service=web)
	renameP, ok := out[1].Processor.(*RenameProcessor)
	require.True(t, ok)
	require.Equal(t, "a", renameP.From)
	require.Equal(t, "b", renameP.To)
	require.NotNil(t, out[1].Where)
	require.Equal(t, CondAnd, out[1].Where.Kind)
	require.Len(t, out[1].Where.And, 2)
	require.Equal(t, "level", out[1].Where.And[0].Filter.Field)
	require.Equal(t, "service", out[1].Where.And[1].Filter.Field)
}
