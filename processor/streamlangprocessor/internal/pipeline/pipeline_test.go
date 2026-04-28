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

package pipeline_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/pipeline"
)

// newLogDoc creates a fresh LogDocument backed by a brand-new pdata structure.
func newLogDoc(t *testing.T) document.Document {
	t.Helper()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	return document.NewLogDocument(rl, sl, lr)
}

func parseDSL(t *testing.T, raw any) dsl.DSL {
	t.Helper()
	d, err := dsl.Parse(raw)
	require.NoError(t, err)
	return d
}

func TestPipeline_HappyPath(t *testing.T) {
	d := parseDSL(t, []any{
		map[string]any{"action": "set", "to": "attributes.foo", "value": "bar"},
		map[string]any{"action": "uppercase", "from": "attributes.foo"},
	})
	c, err := pipeline.Compile(d)
	require.NoError(t, err)
	require.Equal(t, 2, c.Steps())

	doc := newLogDoc(t)
	stats, err := c.ExecuteBatch(context.Background(), []document.Document{doc}, nil, pipeline.FailureModeDrop, "")
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Input)
	assert.Equal(t, 0, stats.Dropped)

	v, ok := doc.Get("attributes.foo")
	require.True(t, ok)
	assert.Equal(t, "BAR", v.Str())
}

func TestPipeline_PerDocErrorIsolation(t *testing.T) {
	// rename without ignore_missing on a missing field => doc is dropped, but
	// other docs keep flowing.
	d := parseDSL(t, []any{
		map[string]any{"action": "set", "to": "attributes.foo", "value": "x"},
		map[string]any{"action": "rename", "from": "attributes.does_not_exist", "to": "attributes.dest"},
		map[string]any{"action": "set", "to": "attributes.bar", "value": "y"},
	})
	c, err := pipeline.Compile(d)
	require.NoError(t, err)

	good := newLogDoc(t)
	require.NoError(t, good.Set("attributes.does_not_exist", document.StringValue("present")))
	bad := newLogDoc(t) // missing the rename source

	stats, err := c.ExecuteBatch(context.Background(), []document.Document{good, bad}, nil, pipeline.FailureModeDrop, "")
	require.NoError(t, err)
	assert.Equal(t, 2, stats.Input)
	assert.Equal(t, 1, stats.Dropped)

	// good runs all three steps successfully:
	v1, ok := good.Get("attributes.foo")
	require.True(t, ok)
	assert.Equal(t, "x", v1.Str())
	v2, ok := good.Get("attributes.dest")
	require.True(t, ok)
	assert.Equal(t, "present", v2.Str())
	v3, ok := good.Get("attributes.bar")
	require.True(t, ok)
	assert.Equal(t, "y", v3.Str())

	// bad is marked dropped after the failing rename.
	assert.True(t, bad.IsDropped())
}

func TestPipeline_IgnoreFailureSwallowsError(t *testing.T) {
	d := parseDSL(t, []any{
		map[string]any{
			"action":         "rename",
			"from":           "attributes.missing",
			"to":             "attributes.dest",
			"ignore_failure": true,
		},
		map[string]any{"action": "set", "to": "attributes.after", "value": "ok"},
	})
	c, err := pipeline.Compile(d)
	require.NoError(t, err)

	doc := newLogDoc(t)
	stats, err := c.ExecuteBatch(context.Background(), []document.Document{doc}, nil, pipeline.FailureModeDrop, "")
	require.NoError(t, err)
	assert.Equal(t, 0, stats.Dropped)
	assert.False(t, doc.IsDropped())
	v, ok := doc.Get("attributes.after")
	require.True(t, ok)
	assert.Equal(t, "ok", v.Str())
}

func TestPipeline_PropagateMode(t *testing.T) {
	d := parseDSL(t, []any{
		map[string]any{"action": "rename", "from": "attributes.missing", "to": "attributes.dest"},
	})
	c, err := pipeline.Compile(d)
	require.NoError(t, err)
	doc := newLogDoc(t)
	_, err = c.ExecuteBatch(context.Background(), []document.Document{doc}, nil, pipeline.FailureModePropagate, "")
	require.Error(t, err)
}

func TestPipeline_ConditionGuardSkips(t *testing.T) {
	d := parseDSL(t, []any{
		map[string]any{
			"condition": map[string]any{
				"field": "attributes.severity",
				"eq":    "ERROR",
				"steps": []any{
					map[string]any{"action": "set", "to": "attributes.alert", "value": true},
				},
			},
		},
	})
	c, err := pipeline.Compile(d)
	require.NoError(t, err)

	matched := newLogDoc(t)
	require.NoError(t, matched.Set("attributes.severity", document.StringValue("ERROR")))
	unmatched := newLogDoc(t)
	require.NoError(t, unmatched.Set("attributes.severity", document.StringValue("INFO")))

	_, err = c.ExecuteBatch(context.Background(), []document.Document{matched, unmatched}, nil, pipeline.FailureModeDrop, "")
	require.NoError(t, err)

	v, ok := matched.Get("attributes.alert")
	require.True(t, ok)
	assert.Equal(t, true, v.Bool())

	_, ok = unmatched.Get("attributes.alert")
	assert.False(t, ok)
}

func TestPipeline_DropDocument(t *testing.T) {
	d := parseDSL(t, []any{
		map[string]any{
			"action": "drop_document",
			"where":  map[string]any{"field": "attributes.severity", "eq": "DEBUG"},
		},
		map[string]any{"action": "set", "to": "attributes.after_drop", "value": "x"},
	})
	c, err := pipeline.Compile(d)
	require.NoError(t, err)

	dbg := newLogDoc(t)
	require.NoError(t, dbg.Set("attributes.severity", document.StringValue("DEBUG")))
	keep := newLogDoc(t)
	require.NoError(t, keep.Set("attributes.severity", document.StringValue("INFO")))

	stats, err := c.ExecuteBatch(context.Background(), []document.Document{dbg, keep}, nil, pipeline.FailureModeDrop, "")
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Dropped)
	assert.True(t, dbg.IsDropped())
	assert.False(t, keep.IsDropped())

	// keep should have run the set after the drop_document step (it didn't match).
	v, ok := keep.Get("attributes.after_drop")
	require.True(t, ok)
	assert.Equal(t, "x", v.Str())
}
