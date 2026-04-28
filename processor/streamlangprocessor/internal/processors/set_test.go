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

func TestSet_LiteralValue(t *testing.T) {
	c, err := compileSet(&dsl.SetProcessor{
		To:       "attributes.foo",
		HasValue: true,
		Value:    "bar",
	})
	require.NoError(t, err)
	assert.Equal(t, "set", c.Action())

	d := newTestDoc(t)
	require.NoError(t, c.Execute(d))

	v, ok := d.Get("attributes.foo")
	require.True(t, ok)
	assert.Equal(t, "bar", v.Str())
}

func TestSet_NilValue(t *testing.T) {
	c, err := compileSet(&dsl.SetProcessor{
		To:       "attributes.foo",
		HasValue: true,
		Value:    nil,
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, c.Execute(d))

	// Per the document layer's empty-handling, a nil literal materializes as
	// an empty slot at the target path; the important property is that the
	// path is present and not a real string/number.
	v, ok := d.Get("attributes.foo")
	require.True(t, ok)
	assert.NotEqual(t, document.ValueTypeStr, v.Type())
}

func TestSet_CopyFromMissing_Skip(t *testing.T) {
	c, err := compileSet(&dsl.SetProcessor{
		To:       "attributes.dest",
		CopyFrom: "attributes.src",
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	err = c.Execute(d)
	require.Error(t, err)
	assert.True(t, IsSkip(err))
	_, ok := d.Get("attributes.dest")
	assert.False(t, ok)
}

func TestSet_CopyFromOK(t *testing.T) {
	c, err := compileSet(&dsl.SetProcessor{
		To:       "attributes.dest",
		CopyFrom: "attributes.src",
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.src", document.StringValue("hello")))
	require.NoError(t, c.Execute(d))

	v, ok := d.Get("attributes.dest")
	require.True(t, ok)
	assert.Equal(t, "hello", v.Str())
}

func TestSet_TargetExists_OverrideExplicitFalse_Skip(t *testing.T) {
	c, err := compileSet(&dsl.SetProcessor{
		To:       "attributes.foo",
		Override: ptrBool(false),
		HasValue: true,
		Value:    "new",
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.foo", document.StringValue("old")))
	err = c.Execute(d)
	require.Error(t, err)
	assert.True(t, IsSkip(err))

	v, _ := d.Get("attributes.foo")
	assert.Equal(t, "old", v.Str())
}

func TestSet_TargetExists_DefaultOverrides(t *testing.T) {
	// ES ingest `set` defaults override=true. Without an explicit Override,
	// existing values are overwritten.
	c, err := compileSet(&dsl.SetProcessor{
		To:       "attributes.foo",
		HasValue: true,
		Value:    "new",
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.foo", document.StringValue("old")))
	require.NoError(t, c.Execute(d))

	v, _ := d.Get("attributes.foo")
	assert.Equal(t, "new", v.Str())
}

func TestSet_TargetExists_OverrideExplicitTrue(t *testing.T) {
	c, err := compileSet(&dsl.SetProcessor{
		To:       "attributes.foo",
		Override: ptrBool(true),
		HasValue: true,
		Value:    "new",
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.foo", document.StringValue("old")))
	require.NoError(t, c.Execute(d))

	v, _ := d.Get("attributes.foo")
	assert.Equal(t, "new", v.Str())
}

func TestSet_RequiresValueOrCopyFrom(t *testing.T) {
	_, err := compileSet(&dsl.SetProcessor{To: "x"})
	require.Error(t, err)
}
