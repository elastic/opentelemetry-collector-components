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
	semconv26 "go.opentelemetry.io/otel/semconv/v1.26.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

func TestApplyLogConventions(t *testing.T) {
	t.Run("sets defaults and sanitizes service name", func(t *testing.T) {
		resource := pcommon.NewResource()
		resource.Attributes().PutStr(string(semconv.ServiceNameKey), "my.service.logs")

		ApplyLogConventions(resource)

		serviceName, _ := resource.Attributes().Get(string(semconv.ServiceNameKey))
		assert.Equal(t, "my_service_logs", serviceName.Str())

		serviceLanguage, _ := resource.Attributes().Get("service.language.name")
		assert.Equal(t, "unknown", serviceLanguage.Str())

		deploymentEnvironment, _ := resource.Attributes().Get(string(semconv26.DeploymentEnvironmentKey))
		assert.Equal(t, "unset", deploymentEnvironment.Str())

		agentVersion, _ := resource.Attributes().Get("agent.version")
		assert.Equal(t, "unknown", agentVersion.Str())
	})

	t.Run("respects existing values", func(t *testing.T) {
		resource := pcommon.NewResource()
		attrs := resource.Attributes()
		attrs.PutStr(string(semconv.ServiceNameKey), "")
		attrs.PutStr(string(semconv.TelemetrySDKLanguageKey), "go")
		attrs.PutStr(string(semconv.DeploymentEnvironmentNameKey), "prod")
		attrs.PutStr("agent.version", "1.2.3")

		ApplyLogConventions(resource)

		serviceName, _ := attrs.Get(string(semconv.ServiceNameKey))
		assert.Equal(t, "unknown", serviceName.Str())

		serviceLanguage, _ := attrs.Get("service.language.name")
		assert.Equal(t, "go", serviceLanguage.Str())

		_, hasDeploymentEnvironment := attrs.Get(string(semconv26.DeploymentEnvironmentKey))
		assert.False(t, hasDeploymentEnvironment)

		agentVersion, _ := attrs.Get("agent.version")
		assert.Equal(t, "1.2.3", agentVersion.Str())
	})
}
