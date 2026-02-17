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
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
)

func TestApplyResourceConventions(t *testing.T) {
	testdata := map[string]struct {
		inputAttrs    map[string]string
		expectedAttrs map[string]string
	}{
		"k8s.node.name": {
			inputAttrs: map[string]string{
				string(semconv.K8SNodeNameKey): "node-1",
				string(semconv.HostNameKey):    "old-host.name",
				ecsHostHostname:                "old-host.hostname",
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
				ecsHostHostname:              "old-host.hostname",
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
				ecsHostHostname:               "old-host.hostname",
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
				ecsHostHostname:                     "old-host.hostname",
			},
			expectedAttrs: map[string]string{
				"k8s.namespace.name": "namespace-1",
				"host.name":          "old-host.name",
				"host.hostname":      "",
			},
		},
		"host.name empty": {
			inputAttrs: map[string]string{
				ecsHostHostname: "host.hostname",
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

func TestTranslateResourceMetadata(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(pcommon.Map)
		wantKey  string // expected label destination; empty when dropped
		wantRaw  any    // expected value: string, float64, or []any
		goneKeys []string
	}{
		{
			name:     "string to labels",
			setup:    func(m pcommon.Map) { m.PutStr("unsupported.string", "foo") },
			wantKey:  "labels.unsupported_string",
			wantRaw:  "foo",
			goneKeys: []string{"unsupported.string"},
		},
		{
			name:     "bool to labels",
			setup:    func(m pcommon.Map) { m.PutBool("unsupported.bool", true) },
			wantKey:  "labels.unsupported_bool",
			wantRaw:  "true",
			goneKeys: []string{"unsupported.bool"},
		},
		{
			name:     "int to numeric_labels",
			setup:    func(m pcommon.Map) { m.PutInt("unsupported.int", 42) },
			wantKey:  "numeric_labels.unsupported_int",
			wantRaw:  float64(42),
			goneKeys: []string{"unsupported.int"},
		},
		{
			name:     "double to numeric_labels",
			setup:    func(m pcommon.Map) { m.PutDouble("unsupported.double", 1.25) },
			wantKey:  "numeric_labels.unsupported_double",
			wantRaw:  1.25,
			goneKeys: []string{"unsupported.double"},
		},
		{
			name: "string slice to labels",
			setup: func(m pcommon.Map) {
				s := m.PutEmptySlice("unsupported.string.slice")
				s.AppendEmpty().SetStr("a")
				s.AppendEmpty().SetStr("b")
			},
			wantKey:  "labels.unsupported_string_slice",
			wantRaw:  []any{"a", "b"},
			goneKeys: []string{"unsupported.string.slice"},
		},
		{
			name: "int slice to numeric_labels",
			setup: func(m pcommon.Map) {
				s := m.PutEmptySlice("unsupported.int.slice")
				s.AppendEmpty().SetInt(10)
				s.AppendEmpty().SetInt(20)
			},
			wantKey:  "numeric_labels.unsupported_int_slice",
			wantRaw:  []any{float64(10), float64(20)},
			goneKeys: []string{"unsupported.int.slice"},
		},
		{
			name: "map dropped without label",
			setup: func(m pcommon.Map) {
				m.PutEmptyMap("unsupported.map").PutStr("k", "v")
			},
			goneKeys: []string{"unsupported.map", "labels.unsupported_map"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resource := pcommon.NewResource()
			tc.setup(resource.Attributes())

			TranslateResourceMetadata(resource)

			attrs := resource.Attributes()
			if tc.wantKey != "" {
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
			}
			for _, key := range tc.goneKeys {
				_, exists := attrs.Get(key)
				assert.False(t, exists, "key %s should have been removed", key)
			}
		})
	}
}
