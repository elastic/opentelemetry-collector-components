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

func TestTranslateResourceMetadata_UnsupportedTypeConversions(t *testing.T) {
	resource := pcommon.NewResource()
	attrs := resource.Attributes()
	attrs.PutStr("unsupported.string", "foo")
	attrs.PutBool("unsupported.bool", true)
	attrs.PutInt("unsupported.int", 42)
	attrs.PutDouble("unsupported.double", 1.25)

	stringSlice := attrs.PutEmptySlice("unsupported.string.slice")
	stringSlice.AppendEmpty().SetStr("a")
	stringSlice.AppendEmpty().SetStr("b")

	intSlice := attrs.PutEmptySlice("unsupported.int.slice")
	intSlice.AppendEmpty().SetInt(10)
	intSlice.AppendEmpty().SetInt(20)

	mapValue := attrs.PutEmptyMap("unsupported.map")
	mapValue.PutStr("k", "v")

	TranslateResourceMetadata(resource)

	stringLabel, _ := attrs.Get("labels.unsupported_string")
	assert.Equal(t, "foo", stringLabel.Str())
	boolLabel, _ := attrs.Get("labels.unsupported_bool")
	assert.Equal(t, "true", boolLabel.Str())

	intLabel, _ := attrs.Get("numeric_labels.unsupported_int")
	assert.Equal(t, float64(42), intLabel.Double())
	doubleLabel, _ := attrs.Get("numeric_labels.unsupported_double")
	assert.Equal(t, 1.25, doubleLabel.Double())

	stringSliceLabel, _ := attrs.Get("labels.unsupported_string_slice")
	assert.Equal(t, []any{"a", "b"}, stringSliceLabel.Slice().AsRaw())
	intSliceLabel, _ := attrs.Get("numeric_labels.unsupported_int_slice")
	assert.Equal(t, []any{float64(10), float64(20)}, intSliceLabel.Slice().AsRaw())

	_, hasMapLabel := attrs.Get("labels.unsupported_map")
	assert.False(t, hasMapLabel)

	_, hasSourceString := attrs.Get("unsupported.string")
	assert.False(t, hasSourceString)
	_, hasSourceMap := attrs.Get("unsupported.map")
	assert.False(t, hasSourceMap)
}
