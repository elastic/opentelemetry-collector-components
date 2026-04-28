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

func TestUppercase_InPlace(t *testing.T) {
	c, err := compileUppercase(&dsl.UppercaseProcessor{From: "attributes.s"})
	require.NoError(t, err)
	assert.Equal(t, "uppercase", c.Action())

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.s", document.StringValue("hello")))
	require.NoError(t, c.Execute(d))

	v, _ := d.Get("attributes.s")
	assert.Equal(t, "HELLO", v.Str())
}

func TestUppercase_Missing_Error(t *testing.T) {
	c, _ := compileUppercase(&dsl.UppercaseProcessor{From: "attributes.s"})
	d := newTestDoc(t)
	err := c.Execute(d)
	require.Error(t, err)
	assert.False(t, IsSkip(err))
}

func TestUppercase_Missing_IgnoreMissing_Skip(t *testing.T) {
	c, _ := compileUppercase(&dsl.UppercaseProcessor{
		From: "attributes.s", IgnoreMissing: ptrBool(true),
	})
	d := newTestDoc(t)
	err := c.Execute(d)
	require.Error(t, err)
	assert.True(t, IsSkip(err))
}

func TestLowercase_ToTarget(t *testing.T) {
	c, err := compileLowercase(&dsl.LowercaseProcessor{
		From: "attributes.s", To: "attributes.lower",
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.s", document.StringValue("HELLO")))
	require.NoError(t, c.Execute(d))

	src, _ := d.Get("attributes.s")
	dst, _ := d.Get("attributes.lower")
	assert.Equal(t, "HELLO", src.Str())
	assert.Equal(t, "hello", dst.Str())
}

func TestTrim_StringsAndNumbers(t *testing.T) {
	c, err := compileTrim(&dsl.TrimProcessor{From: "attributes.s"})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.s", document.StringValue("  hi  ")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.s")
	assert.Equal(t, "hi", v.Str())
}

func TestStringCoerce_RejectsMap(t *testing.T) {
	c, _ := compileUppercase(&dsl.UppercaseProcessor{From: "attributes.m"})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.m", document.MapValue(map[string]document.Value{
		"a": document.StringValue("v"),
	})))
	err := c.Execute(d)
	require.Error(t, err)
	assert.False(t, IsSkip(err))
}

func TestReplace_BadPattern_CompileError(t *testing.T) {
	_, err := compileReplace(&dsl.ReplaceProcessor{
		From: "attributes.s", Pattern: "[unclosed", Replacement: "x",
	})
	require.Error(t, err)
}

func TestReplace_BasicSubstitution(t *testing.T) {
	c, err := compileReplace(&dsl.ReplaceProcessor{
		From: "attributes.s", Pattern: "[aeiou]", Replacement: "*",
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.s", document.StringValue("hello")))
	require.NoError(t, c.Execute(d))

	v, _ := d.Get("attributes.s")
	assert.Equal(t, "h*ll*", v.Str())
}

func TestReplace_ToDifferentTarget_PreservesSource(t *testing.T) {
	c, err := compileReplace(&dsl.ReplaceProcessor{
		From: "attributes.s", Pattern: "world", Replacement: "Go",
		To: "attributes.out",
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.s", document.StringValue("hello world")))
	require.NoError(t, c.Execute(d))

	src, _ := d.Get("attributes.s")
	dst, _ := d.Get("attributes.out")
	assert.Equal(t, "hello world", src.Str())
	assert.Equal(t, "hello Go", dst.Str())
}

func TestReplace_Missing_IgnoreMissing_Skip(t *testing.T) {
	c, _ := compileReplace(&dsl.ReplaceProcessor{
		From: "attributes.s", Pattern: "x", Replacement: "y",
		IgnoreMissing: ptrBool(true),
	})
	d := newTestDoc(t)
	err := c.Execute(d)
	require.Error(t, err)
	assert.True(t, IsSkip(err))
}
