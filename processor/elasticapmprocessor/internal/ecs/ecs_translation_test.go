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

func TestApplyResourceConventions(t *testing.T) {
	hostHostnameKey := "host.hostname"
	testdata := map[string]struct {
		inputAttrs    map[string]string
		expectedAttrs map[string]string
	}{
		"k8s.node.name": {
			inputAttrs: map[string]string{
				string(semconv.K8SNodeNameKey): "node-1",
				string(semconv.HostNameKey):    "old-host.name",
				hostHostnameKey:                "old-host.hostname",
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
				hostHostnameKey:              "old-host.hostname",
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
				hostHostnameKey:               "old-host.hostname",
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
				hostHostnameKey:                     "old-host.hostname",
			},
			expectedAttrs: map[string]string{
				"k8s.namespace.name": "namespace-1",
				"host.name":          "old-host.name",
				"host.hostname":      "",
			},
		},
		"host.name empty": {
			inputAttrs: map[string]string{
				hostHostnameKey: "host.hostname",
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
