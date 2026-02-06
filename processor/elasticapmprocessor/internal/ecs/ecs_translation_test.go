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
				t.Fatalf("expected attribute %q to be present", tc.wantKey)
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
