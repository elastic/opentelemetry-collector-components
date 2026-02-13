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

package ecs // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/ecs"

import (
	"regexp"

	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv26 "go.opentelemetry.io/otel/semconv/v1.26.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

const (
	defaultUnknownAgentVersion = "unknown"
	defaultUnknownServiceName  = "unknown"
	defaultUnsetEnvironment    = "unset"
)

var serviceNameInvalidRegexp = regexp.MustCompile("[^a-zA-Z0-9 _-]")

// ApplyLogConventions applies defaults and sanitization for ECS log data flow.
func ApplyLogConventions(resource pcommon.Resource) {
	attrs := resource.Attributes()
	normalizeServiceName(attrs)
	setServiceLanguage(attrs)
	setDefaultServiceEnvironment(attrs)
	setDefaultAgentVersion(attrs)
}

func normalizeServiceName(attrs pcommon.Map) {
	serviceName, ok := attrs.Get(string(semconv.ServiceNameKey))
	if !ok || serviceName.Str() == "" {
		attrs.PutStr(string(semconv.ServiceNameKey), defaultUnknownServiceName)
		return
	}
	attrs.PutStr(string(semconv.ServiceNameKey), cleanServiceName(serviceName.Str()))
}

func cleanServiceName(name string) string {
	return serviceNameInvalidRegexp.ReplaceAllString(name, "_")
}

func setServiceLanguage(attrs pcommon.Map) {
	serviceLanguage, serviceLanguageOK := attrs.Get(ecsAttrServiceLanguageName)
	if serviceLanguageOK && serviceLanguage.Str() != "" {
		return
	}
	if sdkLanguage, ok := attrs.Get(string(semconv.TelemetrySDKLanguageKey)); ok && sdkLanguage.Str() != "" {
		attrs.PutStr(ecsAttrServiceLanguageName, sdkLanguage.Str())
		return
	}
	attrs.PutStr(ecsAttrServiceLanguageName, defaultUnknownServiceName)
}

func setDefaultServiceEnvironment(attrs pcommon.Map) {
	deploymentEnvironment, deploymentEnvironmentOK := attrs.Get(string(semconv26.DeploymentEnvironmentKey))
	if deploymentEnvironmentOK && deploymentEnvironment.Str() != "" {
		return
	}
	deploymentEnvironmentName, deploymentEnvironmentNameOK := attrs.Get(string(semconv.DeploymentEnvironmentNameKey))
	if deploymentEnvironmentNameOK && deploymentEnvironmentName.Str() != "" {
		return
	}
	attrs.PutStr(string(semconv26.DeploymentEnvironmentKey), defaultUnsetEnvironment)
}

func setDefaultAgentVersion(attrs pcommon.Map) {
	agentVersion, ok := attrs.Get(ecsAttrAgentVersion)
	if ok && agentVersion.Str() != "" {
		return
	}
	attrs.PutStr(ecsAttrAgentVersion, defaultUnknownAgentVersion)
}
