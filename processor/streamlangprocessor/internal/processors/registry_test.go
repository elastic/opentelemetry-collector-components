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

package processors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

func TestCompile_DispatchesByType(t *testing.T) {
	cases := []struct {
		name string
		in   dsl.Processor
		want string
	}{
		{"set", &dsl.SetProcessor{To: "x", HasValue: true, Value: "y"}, "set"},
		{"append", &dsl.AppendProcessor{To: "x", Value: []any{"y"}}, "append"},
		{"remove", &dsl.RemoveProcessor{From: "x"}, "remove"},
		{"remove_by_prefix", &dsl.RemoveByPrefixProcessor{From: "x"}, "remove_by_prefix"},
		{"rename", &dsl.RenameProcessor{From: "a", To: "b"}, "rename"},
		{"drop_document", &dsl.DropDocumentProcessor{}, "drop_document"},
		{"uppercase", &dsl.UppercaseProcessor{From: "x"}, "uppercase"},
		{"lowercase", &dsl.LowercaseProcessor{From: "x"}, "lowercase"},
		{"trim", &dsl.TrimProcessor{From: "x"}, "trim"},
		{"replace", &dsl.ReplaceProcessor{From: "x", Pattern: ".*", Replacement: ""}, "replace"},
		{"convert", &dsl.ConvertProcessor{From: "x", Type: dsl.ConvertTypeString}, "convert"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := Compile(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, c.Action())
		})
	}
}

func TestCompile_PropagatesIgnoreFailure(t *testing.T) {
	base := dsl.ProcessorBase{IgnoreFailure: true}
	c, err := Compile(&dsl.SetProcessor{
		ProcessorBase: base, To: "x", HasValue: true, Value: "y",
	})
	require.NoError(t, err)
	assert.True(t, c.IgnoreFailure())
}
