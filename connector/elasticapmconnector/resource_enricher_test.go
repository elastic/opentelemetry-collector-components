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

package elasticapmconnector

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestSetAgentName(t *testing.T) {
	for name, tc := range map[string]struct {
		attrs    map[string]string
		expected string
	}{
		"sdk name and language": {
			attrs: map[string]string{
				"telemetry.sdk.name":     "opentelemetry",
				"telemetry.sdk.language": "nodejs",
			},
			expected: "opentelemetry/nodejs",
		},
		"language only": {
			attrs: map[string]string{
				"telemetry.sdk.language": "go",
			},
			expected: "otlp/go",
		},
		"distro name": {
			attrs: map[string]string{
				"telemetry.sdk.name":     "opentelemetry",
				"telemetry.sdk.language": "java",
				"telemetry.distro.name":  "elastic",
			},
			expected: "opentelemetry/java/elastic",
		},
		"distro name without language": {
			attrs: map[string]string{
				"telemetry.sdk.name":    "opentelemetry",
				"telemetry.distro.name": "elastic",
			},
			expected: "opentelemetry/unknown/elastic",
		},
		"already set is preserved": {
			attrs: map[string]string{
				"agent.name":             "my-custom-agent",
				"telemetry.sdk.language": "go",
			},
			expected: "my-custom-agent",
		},
		"no telemetry attributes is a no-op": {
			attrs:    map[string]string{},
			expected: "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			resource := pcommon.NewResource()
			for k, v := range tc.attrs {
				resource.Attributes().PutStr(k, v)
			}

			setAgentName(resource)

			got, ok := resource.Attributes().Get("agent.name")
			if tc.expected == "" {
				assert.False(t, ok)
				return
			}
			assert.True(t, ok)
			assert.Equal(t, tc.expected, got.Str())
		})
	}
}
