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

package enrichments

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv25 "go.opentelemetry.io/otel/semconv/v1.25.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
)

// ecsResourceConfig returns the base ECS resource configuration shared by the
// signal-specific enrichers in processor.go.
func ecsResourceConfig() config.ResourceConfig {
	c := config.Enabled().Resource
	c.DefaultDeploymentEnvironment.Enabled = true
	c.ServiceName.Enabled = true
	c.HostOSType.Enabled = true
	return c
}

// ecsLogResourceConfig returns the ECS log resource configuration used by the
// log processor, including the post-agent default service language behavior.
func ecsLogResourceConfig() config.ResourceConfig {
	c := ecsResourceConfig()
	c.DefaultServiceLanguage.Enabled = true
	return c
}

func TestResourceEnrich(t *testing.T) {
	for _, tc := range []struct {
		name          string
		input         pcommon.Resource
		config        config.ResourceConfig
		enrichedAttrs map[string]any
	}{
		{
			name:          "all_disabled",
			input:         pcommon.NewResource(),
			enrichedAttrs: map[string]any{},
		},
		{
			name:   "empty",
			input:  pcommon.NewResource(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name:  "empty_default_deployment_environment_disabled",
			input: pcommon.NewResource(),
			config: func() config.ResourceConfig {
				c := ecsResourceConfig()
				c.DefaultDeploymentEnvironment.Enabled = false
				return c
			}(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:    "otlp",
				elasticattr.AgentVersion: "unknown",
			},
		},
		{
			name: "sdkname_set",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "customflavor")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "customflavor",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "default_service_language_set_for_non_intake_logs",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "opentelemetry")
				return res
			}(),
			config: ecsLogResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "opentelemetry",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv.TelemetrySDKLanguageKey):    "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "default_service_language_overwrites_empty_value_for_non_intake_logs",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "opentelemetry")
				res.Attributes().PutStr(string(semconv.TelemetrySDKLanguageKey), "")
				return res
			}(),
			config: ecsLogResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "opentelemetry",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv.TelemetrySDKLanguageKey):    "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "default_service_language_preserves_existing_value",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "opentelemetry")
				res.Attributes().PutStr(string(semconv.TelemetrySDKLanguageKey), "java")
				return res
			}(),
			config: ecsLogResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "opentelemetry/java",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "sdkname_distro_set",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "customflavor")
				res.Attributes().PutStr(string(semconv.TelemetryDistroNameKey), "elastic")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "customflavor/unknown/elastic",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "sdkname_distro_lang_set",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "customflavor")
				res.Attributes().PutStr(string(semconv.TelemetrySDKLanguageKey), "cpp")
				res.Attributes().PutStr(string(semconv.TelemetryDistroNameKey), "elastic")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "customflavor/cpp/elastic",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "lang_set",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKLanguageKey), "cpp")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp/cpp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "sdkname_lang_set",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "customflavor")
				res.Attributes().PutStr(string(semconv.TelemetrySDKLanguageKey), "cpp")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "customflavor/cpp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "sdkname_sdkver_set",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "customflavor")
				res.Attributes().PutStr(string(semconv.TelemetrySDKVersionKey), "9.999.9")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "customflavor",
				elasticattr.AgentVersion:                   "9.999.9",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "sdkname_sdkver_distroname_set",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "customflavor")
				res.Attributes().PutStr(string(semconv.TelemetrySDKVersionKey), "9.999.9")
				res.Attributes().PutStr(string(semconv.TelemetryDistroNameKey), "elastic")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "customflavor/unknown/elastic",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "sdkname_sdkver_distroname_distrover_set",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "customflavor")
				res.Attributes().PutStr(string(semconv.TelemetrySDKVersionKey), "9.999.9")
				res.Attributes().PutStr(string(semconv.TelemetryDistroNameKey), "elastic")
				res.Attributes().PutStr(string(semconv.TelemetryDistroVersionKey), "1.2.3")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "customflavor/unknown/elastic",
				elasticattr.AgentVersion:                   "1.2.3",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "host_name_override_with_k8s_node_name",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.HostNameKey), "test-host")
				res.Attributes().PutStr(string(semconv.K8SNodeNameKey), "k8s-node")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				string(semconv.HostNameKey):                "k8s-node",
				string(semconv.K8SNodeNameKey):             "k8s-node",
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.ServiceInstanceIDKey):     string("test-host"),
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "host_name_if_empty_set_from_k8s_node_name",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.K8SNodeNameKey), "k8s-node")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				string(semconv.HostNameKey):                "k8s-node",
				string(semconv.K8SNodeNameKey):             "k8s-node",
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			// Pre SemConv 1.27
			name: "deployment_environment_set",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv25.DeploymentEnvironmentKey), "prod")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				string(semconv25.DeploymentEnvironmentKey): "prod",
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
			},
		},
		{
			// SemConv 1.27+ with new `deployment.environment.name` field
			name: "deployment_environment_name_set",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.DeploymentEnvironmentNameKey), "prod")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				// To satisfy aliases defined in ES, we duplicate the value for both fields.
				string(semconv25.DeploymentEnvironmentKey):   "prod",
				string(semconv.DeploymentEnvironmentNameKey): "prod",
				elasticattr.AgentName:                        "otlp",
				elasticattr.AgentVersion:                     "unknown",
			},
		},
		{
			name: "deployment_environment_name_set_default_disabled",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.DeploymentEnvironmentNameKey), "prod")
				return res
			}(),
			config: func() config.ResourceConfig {
				c := ecsResourceConfig()
				c.DefaultDeploymentEnvironment.Enabled = false
				return c
			}(),
			enrichedAttrs: map[string]any{
				string(semconv25.DeploymentEnvironmentKey):   "prod",
				string(semconv.DeploymentEnvironmentNameKey): "prod",
				elasticattr.AgentName:                        "otlp",
				elasticattr.AgentVersion:                     "unknown",
			},
		},
		{
			// Mixed pre and post SemConv 1.27 versions (should be an edge case, but some EDOTs might do this).
			name: "deployment_environment_mixed",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.DeploymentEnvironmentNameKey), "prod")
				res.Attributes().PutStr(string(semconv25.DeploymentEnvironmentKey), "test")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				// If both are set, we don't touch those values and take them as they are.
				string(semconv25.DeploymentEnvironmentKey):   "test",
				string(semconv.DeploymentEnvironmentNameKey): "prod",
				elasticattr.AgentName:                        "otlp",
				elasticattr.AgentVersion:                     "unknown",
			},
		},
		{
			name: "service_instance_id_derived_from_container_id",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv25.ContainerIDKey), "container-id")
				res.Attributes().PutStr(string(semconv25.HostNameKey), "k8s-node")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				string(semconv25.ServiceInstanceIDKey):     "container-id",
				string(semconv.ContainerIDKey):             "container-id",
				string(semconv.HostNameKey):                "k8s-node",
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "service_instance_id_derived_from_host_name",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv25.HostNameKey), "k8s-node")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				string(semconv25.ServiceInstanceIDKey):     "k8s-node",
				string(semconv.HostNameKey):                "k8s-node",
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "service_instance_id_already_set",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.ServiceInstanceIDKey), "node-name")
				res.Attributes().PutStr(string(semconv25.ContainerIDKey), "container-id")
				res.Attributes().PutStr(string(semconv25.HostNameKey), "k8s-node")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				string(semconv25.ServiceInstanceIDKey):     "node-name",
				string(semconv.ContainerIDKey):             "container-id",
				string(semconv.HostNameKey):                "k8s-node",
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "service_name_sanitized_overwrites_existing_value",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.ServiceNameKey), "my/service")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				string(semconv.ServiceNameKey):             "my_service",
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "agent_version_disabled",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKVersionKey), "1.2.3")
				return res
			}(),
			config: func() config.ResourceConfig {
				c := ecsResourceConfig()
				c.AgentVersion.Enabled = false
				return c
			}(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name:   "agent_version_enabled_defaults_to_unknown",
			input:  pcommon.NewResource(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "agent_version_enabled_uses_sdk_version",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKVersionKey), "1.2.3")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "1.2.3",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "agent_version_enabled_uses_distro_version_over_sdk_version",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.TelemetrySDKVersionKey), "1.2.3")
				res.Attributes().PutStr(string(semconv.TelemetryDistroNameKey), "elastic")
				res.Attributes().PutStr(string(semconv.TelemetryDistroVersionKey), "4.5.6")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp/unknown/elastic",
				elasticattr.AgentVersion:                   "4.5.6",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "all_existing_attributes_preserved",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(elasticattr.AgentName, "existing-agent-name")
				res.Attributes().PutStr(elasticattr.AgentVersion, "existing-agent-version")
				res.Attributes().PutStr(string(semconv25.ServiceInstanceIDKey), "existing-service-instance-id")
				res.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "customflavor")
				res.Attributes().PutStr(string(semconv.TelemetrySDKVersionKey), "9.999.9")
				res.Attributes().PutStr(string(semconv25.ContainerIDKey), "container-id")
				res.Attributes().PutStr(string(semconv.HostNameKey), "host-name")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				// existing attributes are preserved (not overwritten)
				elasticattr.AgentName:                  "existing-agent-name",
				elasticattr.AgentVersion:               "existing-agent-version",
				string(semconv25.ServiceInstanceIDKey): "existing-service-instance-id",
				// source attributes remain unchanged
				string(semconv.TelemetrySDKNameKey):        "customflavor",
				string(semconv.TelemetrySDKVersionKey):     "9.999.9",
				string(semconv25.ContainerIDKey):           "container-id",
				string(semconv.HostNameKey):                "host-name",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "host_os_type_from_os_type_windows",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.OSTypeKey), "windows")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
				elasticattr.HostOSType:                     "windows",
			},
		},
		{
			name: "host_os_type_from_os_type_linux",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.OSTypeKey), "linux")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
				elasticattr.HostOSType:                     "linux",
			},
		},
		{
			name: "host_os_type_from_os_type_darwin",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.OSTypeKey), "darwin")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
				elasticattr.HostOSType:                     "macos",
			},
		},
		{
			name: "host_os_type_from_os_type_aix",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.OSTypeKey), "aix")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
				elasticattr.HostOSType:                     "unix",
			},
		},
		{
			name: "host_os_type_from_os_type_hpux",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.OSTypeKey), "hpux")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
				elasticattr.HostOSType:                     "unix",
			},
		},
		{
			name: "host_os_type_from_os_type_solaris",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.OSTypeKey), "solaris")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
				elasticattr.HostOSType:                     "unix",
			},
		},
		{
			name: "host_os_type_from_os_type_unknown",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.OSTypeKey), "unknown")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "host_os_type_from_os_name_android",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.OSNameKey), "Android")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
				elasticattr.HostOSType:                     "android",
			},
		},
		{
			name: "host_os_type_from_os_name_ios",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.OSNameKey), "iOS")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
				elasticattr.HostOSType:                     "ios",
			},
		},
		{
			name: "host_os_type_os_name_overrides_os_type",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.OSTypeKey), "linux")
				res.Attributes().PutStr(string(semconv.OSNameKey), "Android")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
				elasticattr.HostOSType:                     "android",
			},
		},
		{
			name: "host_os_type_not_set_when_no_mapping",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.OSNameKey), "Ubuntu")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
			},
		},
		{
			name: "host_os_type_already_set_preserved",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(elasticattr.HostOSType, "custom")
				res.Attributes().PutStr(string(semconv.OSTypeKey), "linux")
				return res
			}(),
			config: ecsResourceConfig(),
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:                      "otlp",
				elasticattr.AgentVersion:                   "unknown",
				string(semconv25.DeploymentEnvironmentKey): "unset",
				elasticattr.HostOSType:                     "custom",
			},
		},
		{
			name: "host_os_type_disabled",
			input: func() pcommon.Resource {
				res := pcommon.NewResource()
				res.Attributes().PutStr(string(semconv.OSTypeKey), "linux")
				return res
			}(),
			config: config.Enabled().Resource,
			enrichedAttrs: map[string]any{
				elasticattr.AgentName:    "otlp",
				elasticattr.AgentVersion: "unknown",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Merge existing resource attrs with the attrs added
			// by enrichment to get the expected attributes.
			expectedAttrs := tc.input.Attributes().AsRaw()
			for k, v := range tc.enrichedAttrs {
				expectedAttrs[k] = v
			}

			EnrichResource(tc.input, tc.config)

			assert.Empty(t, cmp.Diff(expectedAttrs, tc.input.Attributes().AsRaw()))
		})
	}
}
