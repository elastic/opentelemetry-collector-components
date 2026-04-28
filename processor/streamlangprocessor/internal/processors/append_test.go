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

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

func TestAppend_CreatesArray(t *testing.T) {
	c, err := compileAppend(&dsl.AppendProcessor{
		To:    "attributes.tags",
		Value: []any{"a", "b"},
	})
	require.NoError(t, err)
	assert.Equal(t, "append", c.Action())

	d := newTestDoc(t)
	require.NoError(t, c.Execute(d))

	v, ok := d.Get("attributes.tags")
	require.True(t, ok)
	assert.Equal(t, document.ValueTypeSlice, v.Type())
	got := v.Slice()
	require.Len(t, got, 2)
	assert.Equal(t, "a", got[0].Str())
	assert.Equal(t, "b", got[1].Str())
}

func TestAppend_DedupeByDefault(t *testing.T) {
	c, err := compileAppend(&dsl.AppendProcessor{
		To:    "attributes.tags",
		Value: []any{"a", "a", "b"},
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, c.Execute(d))

	v, _ := d.Get("attributes.tags")
	got := v.Slice()
	require.Len(t, got, 2)
	assert.Equal(t, "a", got[0].Str())
	assert.Equal(t, "b", got[1].Str())
}

func TestAppend_AllowDuplicates(t *testing.T) {
	c, err := compileAppend(&dsl.AppendProcessor{
		To:              "attributes.tags",
		Value:           []any{"a", "a"},
		AllowDuplicates: ptrBool(true),
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, c.Execute(d))

	v, _ := d.Get("attributes.tags")
	got := v.Slice()
	require.Len(t, got, 2)
}

func TestAppend_ToScalarPromotedToArray(t *testing.T) {
	c, err := compileAppend(&dsl.AppendProcessor{
		To:    "attributes.tag",
		Value: []any{"b"},
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.tag", document.StringValue("a")))
	require.NoError(t, c.Execute(d))

	v, _ := d.Get("attributes.tag")
	got := v.Slice()
	require.Len(t, got, 2)
	assert.Equal(t, "a", got[0].Str())
	assert.Equal(t, "b", got[1].Str())
}

func TestAppend_DedupeAgainstExisting(t *testing.T) {
	c, err := compileAppend(&dsl.AppendProcessor{
		To:    "attributes.tags",
		Value: []any{"a", "c"},
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.tags", document.SliceValue([]document.Value{
		document.StringValue("a"),
		document.StringValue("b"),
	})))
	require.NoError(t, c.Execute(d))

	v, _ := d.Get("attributes.tags")
	got := v.Slice()
	require.Len(t, got, 3) // a, b, c
}
