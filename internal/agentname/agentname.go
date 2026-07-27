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

// Package agentname derives the Elastic APM agent.name resource attribute
// from OTel SDK telemetry resource attributes.
package agentname // import "github.com/elastic/opentelemetry-collector-components/internal/agentname"

import "fmt"

// Derive computes the agent.name value from the three OTel SDK telemetry
// resource attributes. It returns "otlp" when none are present, matching
// the behaviour of classic Elastic APM agents that do not set agent.name.
//
// Callers are responsible for deciding whether to apply the result (e.g.
// skipping if agent.name is already present on the resource).
func Derive(sdkName, sdkLanguage, distroName string) string {
	agentName := "otlp"
	if sdkName != "" {
		agentName = sdkName
	}
	switch {
	case distroName != "":
		lang := "unknown"
		if sdkLanguage != "" {
			lang = sdkLanguage
		}
		agentName = fmt.Sprintf("%s/%s/%s", agentName, lang, distroName)
	case sdkLanguage != "":
		agentName = fmt.Sprintf("%s/%s", agentName, sdkLanguage)
	}
	return agentName
}
