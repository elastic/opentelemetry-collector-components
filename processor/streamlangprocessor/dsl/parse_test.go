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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func mustParse(t *testing.T, steps []any) DSL {
	t.Helper()
	d, err := Parse(steps)
	require.NoError(t, err)
	return d
}

func TestParse_Set_AllOptions(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":           "set",
			"customIdentifier": "id-1",
			"description":      "set foo",
			"ignore_failure":   true,
			"to":               "attributes.foo",
			"override":         true,
			"value":            "hello",
		},
	})
	require.Len(t, d.Steps, 1)
	p, ok := d.Steps[0].(*SetProcessor)
	require.True(t, ok)
	require.Equal(t, "id-1", p.CustomIdentifier)
	require.Equal(t, "set foo", p.Description)
	require.True(t, p.IgnoreFailure)
	require.Equal(t, "attributes.foo", p.To)
	require.NotNil(t, p.Override)
	require.True(t, *p.Override)
	require.True(t, p.HasValue)
	require.Equal(t, "hello", p.Value)
	require.Empty(t, p.CopyFrom)
}

func TestParse_Set_CopyFrom(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{"action": "set", "to": "x", "copy_from": "y"},
	})
	p := d.Steps[0].(*SetProcessor)
	require.Equal(t, "y", p.CopyFrom)
	require.False(t, p.HasValue)
}

func TestParse_Set_BothValueAndCopyFrom(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{"action": "set", "to": "x", "value": 1, "copy_from": "y"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "value")
	require.Contains(t, err.Error(), "copy_from")
}

func TestParse_Set_NeitherValueNorCopyFrom(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{"action": "set", "to": "x"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "either 'value' or 'copy_from'")
}

func TestParse_Set_UnknownKey(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{"action": "set", "to": "x", "value": 1, "wat": "bad"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "wat")
}

func TestParse_Append(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":           "append",
			"to":               "tags",
			"value":            []any{"a", "b"},
			"allow_duplicates": false,
		},
	})
	p := d.Steps[0].(*AppendProcessor)
	require.Equal(t, "tags", p.To)
	require.Equal(t, []any{"a", "b"}, p.Value)
	require.NotNil(t, p.AllowDuplicates)
	require.False(t, *p.AllowDuplicates)
}

func TestParse_Remove(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{"action": "remove", "from": "x", "ignore_missing": true},
	})
	p := d.Steps[0].(*RemoveProcessor)
	require.Equal(t, "x", p.From)
	require.NotNil(t, p.IgnoreMissing)
	require.True(t, *p.IgnoreMissing)
}

func TestParse_RemoveByPrefix_NoWhereAllowed(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{
			"action": "remove_by_prefix",
			"from":   "attr.",
			"where":  map[string]any{"always": map[string]any{}},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "where")
}

func TestParse_RemoveByPrefix_OK(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{"action": "remove_by_prefix", "from": "attr."},
	})
	p := d.Steps[0].(*RemoveByPrefixProcessor)
	require.Equal(t, "attr.", p.From)
}

func TestParse_Rename(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":         "rename",
			"from":           "a",
			"to":             "b",
			"ignore_missing": true,
			"override":       true,
		},
	})
	p := d.Steps[0].(*RenameProcessor)
	require.Equal(t, "a", p.From)
	require.Equal(t, "b", p.To)
}

func TestParse_DropDocument_RequiresWhere(t *testing.T) {
	_, err := Parse([]any{map[string]any{"action": "drop_document"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "where")
}

func TestParse_DropDocument_OK(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action": "drop_document",
			"where":  map[string]any{"field": "x", "eq": "y"},
		},
	})
	p := d.Steps[0].(*DropDocumentProcessor)
	require.NotNil(t, p.Where)
	require.Equal(t, CondFilter, p.Where.Kind)
}

func TestParse_Uppercase_Lowercase_Trim(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{"action": "uppercase", "from": "x", "to": "X"},
		map[string]any{"action": "lowercase", "from": "y"},
		map[string]any{"action": "trim", "from": "z", "ignore_missing": true},
	})
	require.Len(t, d.Steps, 3)
	require.Equal(t, "X", d.Steps[0].(*UppercaseProcessor).To)
	require.Equal(t, "y", d.Steps[1].(*LowercaseProcessor).From)
	require.NotNil(t, d.Steps[2].(*TrimProcessor).IgnoreMissing)
}

func TestParse_Replace(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":      "replace",
			"from":        "msg",
			"pattern":     "foo",
			"replacement": "bar",
			"to":          "msg2",
		},
	})
	p := d.Steps[0].(*ReplaceProcessor)
	require.Equal(t, "msg", p.From)
	require.Equal(t, "foo", p.Pattern)
	require.Equal(t, "bar", p.Replacement)
	require.Equal(t, "msg2", p.To)
}

func TestParse_Convert_OK(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{"action": "convert", "from": "n", "type": "integer"},
	})
	p := d.Steps[0].(*ConvertProcessor)
	require.Equal(t, ConvertTypeInteger, p.Type)
}

func TestParse_Convert_AlwaysWhereWithoutTo(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action": "convert",
			"from":   "n",
			"type":   "integer",
			"where":  map[string]any{"always": map[string]any{}},
		},
	})
	p := d.Steps[0].(*ConvertProcessor)
	require.Equal(t, CondAlways, p.Where.Kind)
}

func TestParse_Convert_NonAlwaysWhereWithoutTo(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{
			"action": "convert",
			"from":   "n",
			"type":   "integer",
			"where":  map[string]any{"field": "x", "eq": 1},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "to")
}

func TestParse_Convert_WhereWithToEqualFrom(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{
			"action": "convert",
			"from":   "n",
			"to":     "n",
			"type":   "integer",
			"where":  map[string]any{"field": "x", "eq": 1},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "differ")
}

func TestParse_Convert_BadType(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{"action": "convert", "from": "n", "type": "bogus"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "bogus")
}

func TestParse_UnsupportedAction(t *testing.T) {
	for _, action := range []string{"enrich", "manual_ingest_pipeline"} {
		t.Run(action, func(t *testing.T) {
			_, err := Parse([]any{
				map[string]any{"action": action},
			})
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrUnsupportedAction))
			require.Contains(t, err.Error(), action)
		})
	}
}

func TestParse_UnknownAction(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{"action": "no_such_thing"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no_such_thing")
}

func TestParse_NestedConditionBlock(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"customIdentifier": "outer",
			"condition": map[string]any{
				"field": "level",
				"eq":    "error",
				"steps": []any{
					map[string]any{
						"condition": map[string]any{
							"field": "service",
							"eq":    "web",
							"steps": []any{
								map[string]any{"action": "set", "to": "x", "value": 1},
							},
						},
					},
				},
			},
		},
	})
	require.Len(t, d.Steps, 1)
	outer, ok := d.Steps[0].(*ConditionBlock)
	require.True(t, ok)
	require.Equal(t, "outer", outer.CustomIdentifier)
	require.Equal(t, CondFilter, outer.Condition.Kind)
	require.Len(t, outer.Steps, 1)

	inner, ok := outer.Steps[0].(*ConditionBlock)
	require.True(t, ok)
	require.Equal(t, CondFilter, inner.Condition.Kind)
	require.Len(t, inner.Steps, 1)

	leaf, ok := inner.Steps[0].(*SetProcessor)
	require.True(t, ok)
	require.Equal(t, "x", leaf.To)
}

func TestParse_FilterOperators(t *testing.T) {
	cases := []struct {
		name string
		body map[string]any
	}{
		{"eq", map[string]any{"field": "f", "eq": "x"}},
		{"neq", map[string]any{"field": "f", "neq": "x"}},
		{"lt", map[string]any{"field": "f", "lt": 1}},
		{"lte", map[string]any{"field": "f", "lte": 1}},
		{"gt", map[string]any{"field": "f", "gt": 1}},
		{"gte", map[string]any{"field": "f", "gte": 1}},
		{"contains", map[string]any{"field": "f", "contains": "x"}},
		{"startsWith", map[string]any{"field": "f", "startsWith": "x"}},
		{"startswith", map[string]any{"field": "f", "startswith": "x"}},
		{"endsWith", map[string]any{"field": "f", "endsWith": "x"}},
		{"endswith", map[string]any{"field": "f", "endswith": "x"}},
		{"includes", map[string]any{"field": "f", "includes": "x"}},
		{"range", map[string]any{"field": "f", "range": map[string]any{"gt": 1, "lt": 10}}},
		{"exists", map[string]any{"field": "f", "exists": true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cond, err := parseCondition(tc.body)
			require.NoError(t, err)
			require.Equal(t, CondFilter, cond.Kind)
			require.NotNil(t, cond.Filter)
		})
	}
}

func TestParse_LogicalConditions(t *testing.T) {
	cases := map[string]map[string]any{
		"and": {"and": []any{
			map[string]any{"always": map[string]any{}},
			map[string]any{"never": map[string]any{}},
		}},
		"or": {"or": []any{
			map[string]any{"field": "f", "eq": 1},
			map[string]any{"field": "f", "eq": 2},
		}},
		"not":    {"not": map[string]any{"always": map[string]any{}}},
		"always": {"always": map[string]any{}},
		"never":  {"never": map[string]any{}},
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			cond, err := parseCondition(body)
			require.NoError(t, err)
			switch name {
			case "and":
				require.Equal(t, CondAnd, cond.Kind)
				require.Len(t, cond.And, 2)
			case "or":
				require.Equal(t, CondOr, cond.Kind)
				require.Len(t, cond.Or, 2)
			case "not":
				require.Equal(t, CondNot, cond.Kind)
				require.NotNil(t, cond.Not)
			case "always":
				require.Equal(t, CondAlways, cond.Kind)
			case "never":
				require.Equal(t, CondNever, cond.Kind)
			}
		})
	}
}

func TestParse_Filter_NoOperator(t *testing.T) {
	_, err := parseCondition(map[string]any{"field": "f"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "operator")
}

func TestParse_DocumentShape(t *testing.T) {
	d, err := Parse(map[string]any{
		"steps": []any{
			map[string]any{"action": "set", "to": "x", "value": 1},
		},
	})
	require.NoError(t, err)
	require.Len(t, d.Steps, 1)
}

func TestParseYAML(t *testing.T) {
	yaml := []byte(`
steps:
  - action: set
    to: attributes.foo
    value: bar
  - condition:
      field: level
      eq: error
      steps:
        - action: drop_document
          where:
            field: service
            eq: web
`)
	d, err := ParseYAML(yaml)
	require.NoError(t, err)
	require.Len(t, d.Steps, 2)
	_, ok := d.Steps[0].(*SetProcessor)
	require.True(t, ok)
	cb, ok := d.Steps[1].(*ConditionBlock)
	require.True(t, ok)
	require.Len(t, cb.Steps, 1)
}

func TestParseJSON(t *testing.T) {
	src := []byte(`{"steps":[{"action":"set","to":"x","value":1}]}`)
	d, err := ParseJSON(src)
	require.NoError(t, err)
	require.Len(t, d.Steps, 1)
}

// --- Phase 2/3 action tests -------------------------------------------------

func TestParse_Grok(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":   "grok",
			"from":     "message",
			"patterns": []any{"%{IP:ip}", "%{NUMBER:n}"},
			"pattern_definitions": map[string]any{
				"FOO": "bar",
			},
			"ignore_missing": true,
		},
	})
	p := d.Steps[0].(*GrokProcessor)
	require.Equal(t, "message", p.From)
	require.Equal(t, []string{"%{IP:ip}", "%{NUMBER:n}"}, p.Patterns)
	require.Equal(t, "bar", p.PatternDefinitions["FOO"])
	require.NotNil(t, p.IgnoreMissing)
	require.True(t, *p.IgnoreMissing)
}

func TestParse_Grok_EmptyPatterns(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{"action": "grok", "from": "msg", "patterns": []any{}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "patterns")
}

func TestParse_Dissect(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":           "dissect",
			"from":             "msg",
			"pattern":          "%{a} %{b}",
			"append_separator": ",",
			"ignore_missing":   true,
		},
	})
	p := d.Steps[0].(*DissectProcessor)
	require.Equal(t, "msg", p.From)
	require.Equal(t, "%{a} %{b}", p.Pattern)
	require.Equal(t, ",", p.AppendSeparator)
	require.NotNil(t, p.IgnoreMissing)
}

func TestParse_Dissect_EmptyPattern(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{"action": "dissect", "from": "msg", "pattern": ""},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "pattern")
}

func TestParse_Date(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":        "date",
			"from":          "ts",
			"to":            "@timestamp",
			"formats":       []any{"ISO8601", "yyyy-MM-dd"},
			"output_format": "yyyy-MM-dd",
			"timezone":      "UTC",
			"locale":        "en",
		},
	})
	p := d.Steps[0].(*DateProcessor)
	require.Equal(t, "ts", p.From)
	require.Equal(t, "@timestamp", p.To)
	require.Equal(t, []string{"ISO8601", "yyyy-MM-dd"}, p.Formats)
	require.Equal(t, "yyyy-MM-dd", p.OutputFormat)
	require.Equal(t, "UTC", p.Timezone)
	require.Equal(t, "en", p.Locale)
}

func TestParse_Date_EmptyFormats(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{"action": "date", "from": "ts", "formats": []any{}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "formats")
}

func TestParse_Redact(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":   "redact",
			"from":     "msg",
			"patterns": []any{"%{EMAILADDRESS:email}"},
			"prefix":   "[REDACT_",
			"suffix":   "]",
		},
	})
	p := d.Steps[0].(*RedactProcessor)
	require.Equal(t, "msg", p.From)
	require.Equal(t, []string{"%{EMAILADDRESS:email}"}, p.Patterns)
	require.Equal(t, "[REDACT_", p.Prefix)
	require.Equal(t, "]", p.Suffix)
}

func TestParse_Math(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":         "math",
			"expression":     "attributes.a * attributes.b",
			"to":             "attributes.c",
			"ignore_missing": true,
		},
	})
	p := d.Steps[0].(*MathProcessor)
	require.Equal(t, "attributes.a * attributes.b", p.Expression)
	require.Equal(t, "attributes.c", p.To)
	require.NotNil(t, p.IgnoreMissing)
	require.True(t, *p.IgnoreMissing)
}

func TestParse_Math_EmptyExpression(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{"action": "math", "expression": "", "to": "x"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expression")
}

func TestParse_JSONExtract(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action": "json_extract",
			"field":  "raw",
			"extractions": []any{
				map[string]any{"selector": "user.id", "target_field": "uid", "type": "keyword"},
				map[string]any{"selector": "$.client.ip", "target_field": "ip"},
			},
		},
	})
	p := d.Steps[0].(*JSONExtractProcessor)
	require.Equal(t, "raw", p.Field)
	require.Len(t, p.Extractions, 2)
	require.Equal(t, "user.id", p.Extractions[0].Selector)
	require.Equal(t, "uid", p.Extractions[0].TargetField)
	require.Equal(t, "keyword", p.Extractions[0].Type)
	require.Empty(t, p.Extractions[1].Type)
}

func TestParse_JSONExtract_EmptyExtractions(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{
			"action":      "json_extract",
			"field":       "raw",
			"extractions": []any{},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "extractions")
}

func TestParse_JSONExtract_BadType(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{
			"action": "json_extract",
			"field":  "raw",
			"extractions": []any{
				map[string]any{"selector": "x", "target_field": "y", "type": "bogus"},
			},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "bogus")
}

func TestParse_NetworkDirection_WithNetworks(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":            "network_direction",
			"source_ip":         "src.ip",
			"destination_ip":    "dst.ip",
			"target_field":      "network.direction",
			"internal_networks": []any{"10.0.0.0/8", "loopback"},
		},
	})
	p := d.Steps[0].(*NetworkDirectionProcessor)
	require.Equal(t, "src.ip", p.SourceIP)
	require.Equal(t, "dst.ip", p.DestinationIP)
	require.Equal(t, "network.direction", p.TargetField)
	require.Equal(t, []string{"10.0.0.0/8", "loopback"}, p.InternalNetworks)
	require.Empty(t, p.InternalNetworksField)
}

func TestParse_NetworkDirection_WithNetworksField(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":                  "network_direction",
			"source_ip":               "src.ip",
			"destination_ip":          "dst.ip",
			"internal_networks_field": "config.nets",
		},
	})
	p := d.Steps[0].(*NetworkDirectionProcessor)
	require.Equal(t, "config.nets", p.InternalNetworksField)
	require.Nil(t, p.InternalNetworks)
}

func TestParse_NetworkDirection_NeitherNetworksOption(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{
			"action":         "network_direction",
			"source_ip":      "src",
			"destination_ip": "dst",
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "internal_networks")
}

func TestParse_NetworkDirection_BothNetworksOptions(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{
			"action":                  "network_direction",
			"source_ip":               "src",
			"destination_ip":          "dst",
			"internal_networks":       []any{"10.0.0.0/8"},
			"internal_networks_field": "f",
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not both")
}

func TestParse_NetworkDirection_WithWhere(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":            "network_direction",
			"source_ip":         "src",
			"destination_ip":    "dst",
			"internal_networks": []any{"10.0.0.0/8"},
			"where":             map[string]any{"field": "type", "eq": "ip"},
		},
	})
	p := d.Steps[0].(*NetworkDirectionProcessor)
	require.NotNil(t, p.Where)
	require.Equal(t, CondFilter, p.Where.Kind)
}

func TestParse_Split(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":            "split",
			"from":              "csv",
			"separator":         ",",
			"to":                "parts",
			"preserve_trailing": true,
		},
	})
	p := d.Steps[0].(*SplitProcessor)
	require.Equal(t, "csv", p.From)
	require.Equal(t, ",", p.Separator)
	require.Equal(t, "parts", p.To)
	require.NotNil(t, p.PreserveTrailing)
	require.True(t, *p.PreserveTrailing)
}

func TestParse_Sort(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action": "sort",
			"from":   "items",
			"order":  "desc",
		},
	})
	p := d.Steps[0].(*SortProcessor)
	require.Equal(t, "items", p.From)
	require.Equal(t, SortOrderDesc, p.Order)
}

func TestParse_Sort_BadOrder(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{"action": "sort", "from": "x", "order": "sideways"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "sideways")
}

func TestParse_Join(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":    "join",
			"from":      []any{"a", "b", "c"},
			"delimiter": "-",
			"to":        "out",
		},
	})
	p := d.Steps[0].(*JoinProcessor)
	require.Equal(t, []string{"a", "b", "c"}, p.From)
	require.Equal(t, "-", p.Delimiter)
	require.Equal(t, "out", p.To)
}

func TestParse_Join_EmptyDelimiter(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action":    "join",
			"from":      []any{"a", "b"},
			"delimiter": "",
			"to":        "out",
		},
	})
	p := d.Steps[0].(*JoinProcessor)
	require.Equal(t, "", p.Delimiter)
}

func TestParse_Concat(t *testing.T) {
	d := mustParse(t, []any{
		map[string]any{
			"action": "concat",
			"from": []any{
				map[string]any{"type": "field", "value": "a"},
				map[string]any{"type": "literal", "value": "-"},
				map[string]any{"type": "field", "value": "b"},
			},
			"to": "out",
		},
	})
	p := d.Steps[0].(*ConcatProcessor)
	require.Equal(t, "out", p.To)
	require.Len(t, p.From, 3)
	require.Equal(t, ConcatPartField, p.From[0].Type)
	require.Equal(t, "a", p.From[0].Value)
	require.Equal(t, ConcatPartLiteral, p.From[1].Type)
	require.Equal(t, "-", p.From[1].Value)
}

func TestParse_Concat_BadType(t *testing.T) {
	_, err := Parse([]any{
		map[string]any{
			"action": "concat",
			"from": []any{
				map[string]any{"type": "wrong", "value": "x"},
			},
			"to": "out",
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong")
}
