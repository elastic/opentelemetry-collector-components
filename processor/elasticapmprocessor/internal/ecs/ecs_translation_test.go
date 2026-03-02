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
	"strings"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
)

func TestTranslateResourceMetadata(t *testing.T) {
	cases := []struct {
		name       string
		inputKey   string
		inputVal   string
		wantKey    string
		wantAbsent string // if non-empty, assert this attribute key is removed after translation (e.g. sanitized key)
	}{
		{
			name:     "labels no reserved chars",
			inputKey: "labels.my_value",
			inputVal: "bar",
			wantKey:  "labels.my_value",
		},
		{
			name:       "labels dot replaced",
			inputKey:   "labels.other.value",
			inputVal:   "foo",
			wantKey:    "labels.other_value",
			wantAbsent: "labels.other.value",
		},
		{
			name:       "labels asterisk replaced",
			inputKey:   "labels.key*name",
			inputVal:   "baz",
			wantKey:    "labels.key_name",
			wantAbsent: "labels.key*name",
		},
		{
			name:       "labels double quote replaced",
			inputKey:   `labels.key"name`,
			inputVal:   "qux",
			wantKey:    "labels.key_name",
			wantAbsent: `labels.key"name`,
		},
		{
			name:       "labels mixed reserved chars",
			inputKey:   `labels.a.b*c"d`,
			inputVal:   "mix",
			wantKey:    "labels.a_b_c_d",
			wantAbsent: `labels.a.b*c"d`,
		},
		{
			name:     "numeric_labels no reserved chars",
			inputKey: "numeric_labels.clean",
			inputVal: "42",
			wantKey:  "numeric_labels.clean",
		},
		{
			name:       "numeric_labels multiple dots replaced",
			inputKey:   "numeric_labels.http.status.code",
			inputVal:   "200",
			wantKey:    "numeric_labels.http_status_code",
			wantAbsent: "numeric_labels.http.status.code",
		},
		{
			name:       "unsupported dotted key",
			inputKey:   "unsupported.key",
			inputVal:   "foo",
			wantKey:    "labels.unsupported_key",
			wantAbsent: "unsupported.key",
		},
		{
			name:       "unsupported flat key",
			inputKey:   "custom",
			inputVal:   "val",
			wantKey:    "labels.custom",
			wantAbsent: "custom",
		},
		{
			name:       "unsupported asterisk key",
			inputKey:   "some*attr",
			inputVal:   "star",
			wantKey:    "labels.some_attr",
			wantAbsent: "some*attr",
		},
		{
			name:       "unsupported double quote key",
			inputKey:   `some"attr`,
			inputVal:   "quote",
			wantKey:    "labels.some_attr",
			wantAbsent: `some"attr`,
		},
		{
			name:       "unsupported mixed reserved chars",
			inputKey:   `x.y*z"w`,
			inputVal:   "mix",
			wantKey:    "labels.x_y_z_w",
			wantAbsent: `x.y*z"w`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resource := pcommon.NewResource()
			attrs := resource.Attributes()
			attrs.PutStr(tc.inputKey, tc.inputVal)

			TranslateResourceMetadata(resource)

			v, ok := attrs.Get(tc.wantKey)
			if !ok {
				t.Fatalf("expected attribute %q to be present. all attrs %v", tc.wantKey, attrs.AsRaw())
			}
			if v.AsString() != tc.inputVal {
				t.Errorf("attribute %q value = %q, want %q", tc.wantKey, v.AsString(), tc.inputVal)
			}
			if tc.wantAbsent != "" {
				if _, ok := attrs.Get(tc.wantAbsent); ok {
					t.Errorf("expected attribute %q to be absent after sanitization", tc.wantAbsent)
				}
			}
		})
	}
}

// TestSetLabelAttributeValue verifies that setLabelAttributeValue stores
// supported value types under the correct labels.* / numeric_labels.* prefix
// and rejects unsupported types (Map, Bytes, Empty). This matches
// apm-data's setLabel behaviour (input/otlp/metadata.go).
func TestSetLabelAttributeValue(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		value     func() pcommon.Value
		isUpdated bool
		wantKey   string // expected destination key; empty when isUpdated is false
		wantRaw   any    // expected value: string, float64, or []any
	}{
		// --- Scalar types ---
		{
			name:      "string",
			key:       "str_key",
			value:     func() pcommon.Value { return pcommon.NewValueStr("hello") },
			isUpdated: true,
			wantKey:   "labels.str_key",
			wantRaw:   "hello",
		},
		{
			name:      "bool",
			key:       "bool_key",
			value:     func() pcommon.Value { return pcommon.NewValueBool(true) },
			isUpdated: true,
			wantKey:   "labels.bool_key",
			wantRaw:   "true",
		},
		{
			name:      "int",
			key:       "int_key",
			value:     func() pcommon.Value { return pcommon.NewValueInt(42) },
			isUpdated: true,
			wantKey:   "numeric_labels.int_key",
			wantRaw:   float64(42),
		},
		{
			name:      "double",
			key:       "double_key",
			value:     func() pcommon.Value { return pcommon.NewValueDouble(3.14) },
			isUpdated: true,
			wantKey:   "numeric_labels.double_key",
			wantRaw:   3.14,
		},
		{
			name: "string truncated",
			key:  "long_str_key",
			value: func() pcommon.Value {
				// Create a string longer than keywordLength (1024 runes)
				longStr := strings.Repeat("a", 1025)
				return pcommon.NewValueStr(longStr)
			},
			isUpdated: true,
			wantKey:   "labels.long_str_key",
			wantRaw:   strings.Repeat("a", 1024),
		},

		// --- Homogeneous slice types ---
		{
			name: "string slice",
			key:  "str_slice",
			value: func() pcommon.Value {
				v := pcommon.NewValueSlice()
				v.Slice().AppendEmpty().SetStr("a")
				v.Slice().AppendEmpty().SetStr("b")
				return v
			},
			isUpdated: true,
			wantKey:   "labels.str_slice",
			wantRaw:   []any{"a", "b"},
		},
		{
			name: "int slice",
			key:  "int_slice",
			value: func() pcommon.Value {
				v := pcommon.NewValueSlice()
				v.Slice().AppendEmpty().SetInt(1)
				v.Slice().AppendEmpty().SetInt(2)
				return v
			},
			isUpdated: true,
			wantKey:   "numeric_labels.int_slice",
			wantRaw:   []any{float64(1), float64(2)},
		},
		{
			name: "double slice",
			key:  "double_slice",
			value: func() pcommon.Value {
				v := pcommon.NewValueSlice()
				v.Slice().AppendEmpty().SetDouble(1.1)
				v.Slice().AppendEmpty().SetDouble(2.2)
				return v
			},
			isUpdated: true,
			wantKey:   "numeric_labels.double_slice",
			wantRaw:   []any{1.1, 2.2},
		},
		{
			name: "bool slice",
			key:  "bool_slice",
			value: func() pcommon.Value {
				v := pcommon.NewValueSlice()
				v.Slice().AppendEmpty().SetBool(true)
				v.Slice().AppendEmpty().SetBool(false)
				return v
			},
			isUpdated: true,
			wantKey:   "labels.bool_slice",
			wantRaw:   []any{"true", "false"},
		},
		{
			name: "string slice with truncated elements",
			key:  "long_str_slice",
			value: func() pcommon.Value {
				v := pcommon.NewValueSlice()
				// Add strings longer than keywordLength (1024 runes)
				v.Slice().AppendEmpty().SetStr(strings.Repeat("a", 1025))
				v.Slice().AppendEmpty().SetStr(strings.Repeat("b", 1500))
				return v
			},
			isUpdated: true,
			wantKey:   "labels.long_str_slice",
			wantRaw:   []any{strings.Repeat("a", 1024), strings.Repeat("b", 1024)},
		},

		// --- Unsupported types (should NOT be stored) ---
		{
			name:      "empty slice",
			key:       "empty",
			value:     func() pcommon.Value { return pcommon.NewValueSlice() },
			isUpdated: false,
		},
		{
			name: "map",
			key:  "map_key",
			value: func() pcommon.Value {
				v := pcommon.NewValueMap()
				v.Map().PutStr("nested", "value")
				return v
			},
			isUpdated: false,
		},
		{
			name: "bytes",
			key:  "bytes_key",
			value: func() pcommon.Value {
				v := pcommon.NewValueBytes()
				v.Bytes().Append(0x01, 0x02)
				return v
			},
			isUpdated: false,
		},
		{
			name:      "empty value",
			key:       "empty_key",
			value:     func() pcommon.Value { return pcommon.NewValueEmpty() },
			isUpdated: false,
		},
		{
			name: "slice with map element",
			key:  "map_slice",
			value: func() pcommon.Value {
				v := pcommon.NewValueSlice()
				v.Slice().AppendEmpty().SetEmptyMap().PutStr("a", "b")
				return v
			},
			isUpdated: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			attrs := pcommon.NewMap()
			setLabelAttributeValue(attrs, tc.key, tc.value())

			if !tc.isUpdated {
				assert.Equal(t, 0, attrs.Len(), "unsupported type should not add attributes")
				return
			}
			assert.NotEmpty(t, attrs)

			got, exists := attrs.Get(tc.wantKey)
			assert.True(t, exists, "expected %s to be set", tc.wantKey)

			switch want := tc.wantRaw.(type) {
			case string:
				assert.Equal(t, want, got.Str())
			case float64:
				assert.InDelta(t, want, got.Double(), 1e-9)
			case []any:
				assert.Equal(t, want, got.Slice().AsRaw())
			default:
				t.Fatalf("unsupported wantRaw type %T", tc.wantRaw)
			}
		})
	}
}
func TestApplyResourceConventions(t *testing.T) {
	testdata := map[string]struct {
		inputAttrs    map[string]string
		expectedAttrs map[string]string
	}{
		"k8s.node.name": {
			inputAttrs: map[string]string{
				string(semconv.K8SNodeNameKey): "node-1",
				string(semconv.HostNameKey):    "old-host.name",
				elasticattr.HostHostName:       "old-host.hostname",
			},
			expectedAttrs: map[string]string{
				"k8s.node.name": "node-1",
				"host.name":     "old-host.name",
				"host.hostname": "node-1",
			},
		},
		"k8s.pod.uid": {
			inputAttrs: map[string]string{
				string(semconv.K8SPodUIDKey): "pod-1",
				string(semconv.HostNameKey):  "old-host.name",
				elasticattr.HostHostName:     "old-host.hostname",
			},
			expectedAttrs: map[string]string{
				"k8s.pod.uid":   "pod-1",
				"host.name":     "old-host.name",
				"host.hostname": "",
			},
		},
		"k8s.pod.name": {
			inputAttrs: map[string]string{
				string(semconv.K8SPodNameKey): "pod-name-1",
				string(semconv.HostNameKey):   "old-host.name",
				elasticattr.HostHostName:      "old-host.hostname",
			},
			expectedAttrs: map[string]string{
				"k8s.pod.name":  "pod-name-1",
				"host.name":     "old-host.name",
				"host.hostname": "",
			},
		},
		"k8s.namespace.name": {
			inputAttrs: map[string]string{
				string(semconv.K8SNamespaceNameKey): "namespace-1",
				string(semconv.HostNameKey):         "old-host.name",
				elasticattr.HostHostName:            "old-host.hostname",
			},
			expectedAttrs: map[string]string{
				"k8s.namespace.name": "namespace-1",
				"host.name":          "old-host.name",
				"host.hostname":      "",
			},
		},
		"host.name empty": {
			inputAttrs: map[string]string{
				elasticattr.HostHostName: "host.hostname",
			},
			expectedAttrs: map[string]string{
				"host.name":     "host.hostname",
				"host.hostname": "host.hostname",
			},
		},
	}

	for _, td := range testdata {
		resource := pcommon.NewResource()
		attrs := resource.Attributes()
		for k, v := range td.inputAttrs {
			attrs.PutStr(k, v)
		}

		ApplyResourceConventions(resource)

		for k, expectedV := range td.expectedAttrs {
			actualV, ok := attrs.Get(k)
			if expectedV == "" && !ok {
				continue
			}
			if !ok {
				t.Errorf("expected attribute %s to be present", k)
				continue
			}
			if actualV.Str() != expectedV {
				t.Errorf("expected attribute %s to have value %v, got %v", k, expectedV, actualV.Str())
			}
		}
	}
}
