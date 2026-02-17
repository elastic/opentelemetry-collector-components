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
)

func TestApplyOTLPLogAttributeConventions(t *testing.T) {
	t.Run("moves custom attributes to labels", func(t *testing.T) {
		tests := []struct {
			name      string
			setup     func(pcommon.Map)
			sourceKey string // original key — should be removed after the call
			wantKey   string // expected destination key
			wantRaw   any    // expected value: string, float64, or []any
		}{
			{
				name:      "string to labels",
				setup:     func(m pcommon.Map) { m.PutStr("http.method", "GET") },
				sourceKey: "http.method",
				wantKey:   "labels.http_method",
				wantRaw:   "GET",
			},
			{
				name:      "int to numeric_labels",
				setup:     func(m pcommon.Map) { m.PutInt("http.status_code", 200) },
				sourceKey: "http.status_code",
				wantKey:   "numeric_labels.http_status_code",
				wantRaw:   float64(200),
			},
			{
				name:      "second int to numeric_labels",
				setup:     func(m pcommon.Map) { m.PutInt("http.response_content_length", 1024) },
				sourceKey: "http.response_content_length",
				wantKey:   "numeric_labels.http_response_content_length",
				wantRaw:   float64(1024),
			},
			{
				name:      "bool to labels as string",
				setup:     func(m pcommon.Map) { m.PutBool("request.succeeded", true) },
				sourceKey: "request.succeeded",
				wantKey:   "labels.request_succeeded",
				wantRaw:   "true",
			},
			{
				name: "int slice to numeric_labels",
				setup: func(m pcommon.Map) {
					s := m.PutEmptySlice("http.codes")
					s.AppendEmpty().SetInt(200)
					s.AppendEmpty().SetInt(201)
				},
				sourceKey: "http.codes",
				wantKey:   "numeric_labels.http_codes",
				wantRaw:   []any{float64(200), float64(201)},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				attrs := pcommon.NewMap()
				tc.setup(attrs)

				ApplyOTLPLogAttributeConventions(attrs)

				got, ok := attrs.Get(tc.wantKey)
				require.True(t, ok, "expected %s to exist", tc.wantKey)

				switch want := tc.wantRaw.(type) {
				case string:
					assert.Equal(t, want, got.Str())
				case float64:
					assert.InDelta(t, want, got.Double(), 1e-9)
				case []any:
					assert.Equal(t, want, got.Slice().AsRaw())
				}

				_, exists := attrs.Get(tc.sourceKey)
				assert.False(t, exists, "source key %s should be removed", tc.sourceKey)
			})
		}
	})

	t.Run("keeps allowlisted attributes in place", func(t *testing.T) {
		keys := []string{
			"event.name",
			"exception.message",
			"labels.existing_label",
			"numeric_labels.existing_numeric",
			"data_stream.dataset",
			"data_stream.namespace",
			"data_stream.type",
			"elasticsearch.index",
		}

		for _, key := range keys {
			t.Run(key, func(t *testing.T) {
				attrs := pcommon.NewMap()
				attrs.PutStr(key, "sentinel")

				ApplyOTLPLogAttributeConventions(attrs)

				got, ok := attrs.Get(key)
				require.True(t, ok, "allowlisted key %s should be kept", key)
				assert.Equal(t, "sentinel", got.Str())
			})
		}
	})

	t.Run("removes unsupported map type without creating label", func(t *testing.T) {
		attrs := pcommon.NewMap()
		m := attrs.PutEmptyMap("http.request")
		m.PutStr("id", "req-1")

		ApplyOTLPLogAttributeConventions(attrs)

		_, exists := attrs.Get("http.request")
		assert.False(t, exists, "map attribute should be removed")
		_, exists = attrs.Get("labels.http_request")
		assert.False(t, exists, "map should not produce a label")
	})
}
