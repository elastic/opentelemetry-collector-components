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

package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestApplyOTLPLogBodyConventions_MapBody(t *testing.T) {
	record := newTestLogRecord()
	body := record.Body().SetEmptyMap()
	body.PutStr("http.method", "GET")
	body.PutBool("request.success", true)
	body.PutInt("http.status_code", 200)
	body.PutDouble("ratio", 0.5)
	body.PutStr("message", "from_body_map")

	strSlice := body.PutEmptySlice("tags")
	strSlice.AppendEmpty().SetStr("a")
	strSlice.AppendEmpty().SetStr("b")

	intSlice := body.PutEmptySlice("codes")
	intSlice.AppendEmpty().SetInt(200)
	intSlice.AppendEmpty().SetInt(201)

	nested := body.PutEmptyMap("nested")
	nested.PutStr("ignore", "me")

	expectedMessage := record.Body().AsString()
	ApplyOTLPLogBodyConventions(record)

	assert.Equal(t, pcommon.ValueTypeStr, record.Body().Type())
	assert.Equal(t, expectedMessage, record.Body().Str())

	attrs := record.Attributes()
	assert.Equal(t, "GET", mustGet(t, attrs, "labels.http_method").Str())
	assert.Equal(t, "true", mustGet(t, attrs, "labels.request_success").Str())
	assert.Equal(t, float64(200), mustGet(t, attrs, "numeric_labels.http_status_code").Double())
	assert.Equal(t, float64(0.5), mustGet(t, attrs, "numeric_labels.ratio").Double())
	assert.Equal(t, "from_body_map", mustGet(t, attrs, "labels.message").Str())
	assert.Equal(t, []any{"a", "b"}, mustGet(t, attrs, "labels.tags").Slice().AsRaw())
	assert.Equal(t, []any{float64(200), float64(201)}, mustGet(t, attrs, "numeric_labels.codes").Slice().AsRaw())

	_, hasNested := attrs.Get("labels.nested")
	assert.False(t, hasNested)
}

func TestApplyOTLPLogBodyConventions_NonMapBody(t *testing.T) {
	record := newTestLogRecord()
	record.Body().SetStr("plain message")

	ApplyOTLPLogBodyConventions(record)

	assert.Equal(t, pcommon.ValueTypeStr, record.Body().Type())
	assert.Equal(t, "plain message", record.Body().Str())
	assert.Equal(t, 0, record.Attributes().Len())
}

func newTestLogRecord() plog.LogRecord {
	ld := plog.NewLogs()
	return ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
}

func mustGet(t *testing.T, attrs pcommon.Map, key string) pcommon.Value {
	t.Helper()
	value, ok := attrs.Get(key)
	require.True(t, ok, "missing expected attribute %q", key)
	return value
}
