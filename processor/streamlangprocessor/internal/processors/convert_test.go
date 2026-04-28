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

func TestConvert_StringToInteger(t *testing.T) {
	c, err := compileConvert(&dsl.ConvertProcessor{
		From: "attributes.x", Type: dsl.ConvertTypeInteger,
	})
	require.NoError(t, err)
	assert.Equal(t, "convert", c.Action())

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.x", document.StringValue("42")))
	require.NoError(t, c.Execute(d))

	v, _ := d.Get("attributes.x")
	assert.Equal(t, document.ValueTypeInt, v.Type())
	assert.Equal(t, int64(42), v.Int())
}

func TestConvert_DoubleToIntegerExact(t *testing.T) {
	c, _ := compileConvert(&dsl.ConvertProcessor{
		From: "attributes.x", Type: dsl.ConvertTypeLong,
	})

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.x", document.DoubleValue(7.0)))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.x")
	assert.Equal(t, int64(7), v.Int())
}

func TestConvert_DoubleNonIntegerRejected(t *testing.T) {
	c, _ := compileConvert(&dsl.ConvertProcessor{
		From: "attributes.x", Type: dsl.ConvertTypeInteger,
	})

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.x", document.DoubleValue(7.5)))
	err := c.Execute(d)
	require.Error(t, err)
	assert.False(t, IsSkip(err))
}

func TestConvert_BadStringInt(t *testing.T) {
	c, _ := compileConvert(&dsl.ConvertProcessor{
		From: "attributes.x", Type: dsl.ConvertTypeInteger,
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.x", document.StringValue("nope")))
	err := c.Execute(d)
	require.Error(t, err)
	assert.False(t, IsSkip(err))
}

func TestConvert_ToDouble(t *testing.T) {
	c, _ := compileConvert(&dsl.ConvertProcessor{
		From: "attributes.x", Type: dsl.ConvertTypeDouble,
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.x", document.StringValue("3.14")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.x")
	assert.InDelta(t, 3.14, v.Double(), 1e-9)
}

func TestConvert_ToBool(t *testing.T) {
	cases := []struct {
		in   document.Value
		want bool
	}{
		{document.StringValue("true"), true},
		{document.StringValue("FALSE"), false},
		{document.StringValue("1"), true},
		{document.StringValue("0"), false},
		{document.IntValue(2), true},
		{document.IntValue(0), false},
	}
	for _, tc := range cases {
		c, _ := compileConvert(&dsl.ConvertProcessor{
			From: "attributes.x", Type: dsl.ConvertTypeBoolean,
		})
		d := newTestDoc(t)
		require.NoError(t, d.Set("attributes.x", tc.in))
		require.NoError(t, c.Execute(d))
		v, _ := d.Get("attributes.x")
		assert.Equal(t, tc.want, v.Bool())
	}
}

func TestConvert_ToBoolBadString(t *testing.T) {
	c, _ := compileConvert(&dsl.ConvertProcessor{
		From: "attributes.x", Type: dsl.ConvertTypeBoolean,
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.x", document.StringValue("maybe")))
	err := c.Execute(d)
	require.Error(t, err)
}

func TestConvert_ToString(t *testing.T) {
	c, _ := compileConvert(&dsl.ConvertProcessor{
		From: "attributes.x", Type: dsl.ConvertTypeString,
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.x", document.IntValue(42)))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.x")
	assert.Equal(t, document.ValueTypeStr, v.Type())
	assert.Equal(t, "42", v.Str())
}

func TestConvert_Missing_IgnoreMissing_Skip(t *testing.T) {
	c, _ := compileConvert(&dsl.ConvertProcessor{
		From: "attributes.x", Type: dsl.ConvertTypeString,
		IgnoreMissing: ptrBool(true),
	})
	d := newTestDoc(t)
	err := c.Execute(d)
	require.Error(t, err)
	assert.True(t, IsSkip(err))
}

func TestConvert_Missing_Error(t *testing.T) {
	c, _ := compileConvert(&dsl.ConvertProcessor{
		From: "attributes.x", Type: dsl.ConvertTypeString,
	})
	d := newTestDoc(t)
	err := c.Execute(d)
	require.Error(t, err)
	assert.False(t, IsSkip(err))
}

func TestConvert_UnknownType(t *testing.T) {
	_, err := compileConvert(&dsl.ConvertProcessor{
		From: "attributes.x", Type: dsl.ConvertType("nope"),
	})
	require.Error(t, err)
}
