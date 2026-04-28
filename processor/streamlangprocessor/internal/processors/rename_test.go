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

func TestRename_HappyPath(t *testing.T) {
	c, err := compileRename(&dsl.RenameProcessor{
		From: "attributes.a", To: "attributes.b",
	})
	require.NoError(t, err)
	assert.Equal(t, "rename", c.Action())

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.a", document.StringValue("v")))
	require.NoError(t, c.Execute(d))

	_, oldOK := d.Get("attributes.a")
	v, newOK := d.Get("attributes.b")
	assert.False(t, oldOK)
	require.True(t, newOK)
	assert.Equal(t, "v", v.Str())
}

func TestRename_Missing_Error(t *testing.T) {
	c, err := compileRename(&dsl.RenameProcessor{From: "attributes.a", To: "attributes.b"})
	require.NoError(t, err)

	d := newTestDoc(t)
	err = c.Execute(d)
	require.Error(t, err)
	assert.False(t, IsSkip(err))
}

func TestRename_Missing_IgnoreMissing_Skip(t *testing.T) {
	c, err := compileRename(&dsl.RenameProcessor{
		From: "attributes.a", To: "attributes.b",
		IgnoreMissing: ptrBool(true),
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	err = c.Execute(d)
	require.Error(t, err)
	assert.True(t, IsSkip(err))
}

func TestRename_TargetExists_NoOverride_Error(t *testing.T) {
	c, err := compileRename(&dsl.RenameProcessor{From: "attributes.a", To: "attributes.b"})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.a", document.StringValue("1")))
	require.NoError(t, d.Set("attributes.b", document.StringValue("2")))
	err = c.Execute(d)
	require.Error(t, err)
	assert.False(t, IsSkip(err))

	// Source preserved.
	v, _ := d.Get("attributes.a")
	assert.Equal(t, "1", v.Str())
}

func TestRename_TargetExists_Override(t *testing.T) {
	c, err := compileRename(&dsl.RenameProcessor{
		From: "attributes.a", To: "attributes.b",
		Override: ptrBool(true),
	})
	require.NoError(t, err)

	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.a", document.StringValue("1")))
	require.NoError(t, d.Set("attributes.b", document.StringValue("2")))
	require.NoError(t, c.Execute(d))

	_, oldOK := d.Get("attributes.a")
	v, _ := d.Get("attributes.b")
	assert.False(t, oldOK)
	assert.Equal(t, "1", v.Str())
}
