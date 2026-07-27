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

package agentname

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDerive(t *testing.T) {
	for name, tc := range map[string]struct {
		sdkName     string
		sdkLanguage string
		distroName  string
		expected    string
	}{
		"sdk name and language": {
			sdkName:     "opentelemetry",
			sdkLanguage: "nodejs",
			expected:    "opentelemetry/nodejs",
		},
		"language only": {
			sdkLanguage: "go",
			expected:    "otlp/go",
		},
		"distro name": {
			sdkName:     "opentelemetry",
			sdkLanguage: "java",
			distroName:  "elastic",
			expected:    "opentelemetry/java/elastic",
		},
		"distro name without language": {
			sdkName:    "opentelemetry",
			distroName: "elastic",
			expected:   "opentelemetry/unknown/elastic",
		},
		"distro name without sdk name": {
			sdkLanguage: "java",
			distroName:  "elastic",
			expected:    "otlp/java/elastic",
		},
		"sdk name only": {
			sdkName:  "opentelemetry",
			expected: "opentelemetry",
		},
		"no telemetry attributes defaults to otlp": {
			expected: "otlp",
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, Derive(tc.sdkName, tc.sdkLanguage, tc.distroName))
		})
	}
}
