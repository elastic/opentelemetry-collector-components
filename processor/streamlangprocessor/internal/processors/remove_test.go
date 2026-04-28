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

func TestRemove_HappyPath(t *testing.T) {
	c, err := compileRemove(&dsl.RemoveProcessor{From: "attributes.foo"})
	require.NoError(t, err)
	assert.Equal(t, "remove", c.Action())

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.foo", document.StringValue("x")))
	require.NoError(t, c.Execute(d))

	_, ok := d.Get("attributes.foo")
	assert.False(t, ok)
}

func TestRemove_MissingField_Error(t *testing.T) {
	c, err := compileRemove(&dsl.RemoveProcessor{From: "attributes.foo"})
	require.NoError(t, err)

	d := newTestDoc(t)
	err = c.Execute(d)
	require.Error(t, err)
	assert.False(t, IsSkip(err))
}

func TestRemove_MissingField_IgnoreMissing_Skip(t *testing.T) {
	c, err := compileRemove(&dsl.RemoveProcessor{
		From:          "attributes.foo",
		IgnoreMissing: ptrBool(true),
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	err = c.Execute(d)
	require.Error(t, err)
	assert.True(t, IsSkip(err))
}

func TestRemoveByPrefix_NestedAndFlat(t *testing.T) {
	c, err := compileRemoveByPrefix(&dsl.RemoveByPrefixProcessor{From: "attributes.x"})
	require.NoError(t, err)
	assert.Equal(t, "remove_by_prefix", c.Action())

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.x.a", document.StringValue("1")))
	require.NoError(t, d.Set("attributes.x.b", document.StringValue("2")))
	require.NoError(t, d.Set("attributes.y", document.StringValue("keep")))

	require.NoError(t, c.Execute(d))

	_, okA := d.Get("attributes.x.a")
	_, okB := d.Get("attributes.x.b")
	_, okY := d.Get("attributes.y")
	assert.False(t, okA)
	assert.False(t, okB)
	assert.True(t, okY)
}
