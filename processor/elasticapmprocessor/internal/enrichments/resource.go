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
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv25 "go.opentelemetry.io/otel/semconv/v1.25.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/attribute"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
)

// EnrichResource derives and adds Elastic specific resource attributes.
func EnrichResource(resource pcommon.Resource, cfg config.ResourceConfig) {
	var c resourceEnrichmentContext
	c.Enrich(resource, cfg)
}

type resourceEnrichmentContext struct {
	hostName    string
	k8sNodeName string

	telemetrySDKName       string
	telemetrySDKLanguage   string
	telemetrySDKVersion    string
	telemetryDistroName    string
	telemetryDistroVersion string

	deploymentEnvironment     string
	deploymentEnvironmentName string

	serviceInstanceID string
	containerID       string
}

func (s *resourceEnrichmentContext) Enrich(resource pcommon.Resource, cfg config.ResourceConfig) {
	resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch k {
		case string(semconv.HostNameKey):
			s.hostName = v.Str()
		case string(semconv.K8SNodeNameKey):
			s.k8sNodeName = v.Str()
		case string(semconv.TelemetrySDKNameKey):
			s.telemetrySDKName = v.Str()
		case string(semconv.TelemetrySDKLanguageKey):
			s.telemetrySDKLanguage = v.Str()
		case string(semconv.TelemetrySDKVersionKey):
			s.telemetrySDKVersion = v.Str()
		case string(semconv.TelemetryDistroNameKey):
			s.telemetryDistroName = v.Str()
		case string(semconv.TelemetryDistroVersionKey):
			s.telemetryDistroVersion = v.Str()
		case string(semconv25.DeploymentEnvironmentKey):
			s.deploymentEnvironment = v.Str()
		case string(semconv.DeploymentEnvironmentNameKey):
			s.deploymentEnvironmentName = v.Str()
		case string(semconv25.ServiceInstanceIDKey):
			s.serviceInstanceID = v.Str()
		case string(semconv.ContainerIDKey):
			s.containerID = v.Str()
		}
		return true
	})

	// agent.name and version are set by classic Elastic APM Agents - if the value is present, we take it
	// otherwise the setAgent[Name|Version] functions are called to derive the values
	if cfg.AgentName.Enabled {
		s.setAgentName(resource)
	}
	if cfg.AgentVersion.Enabled {
		s.setAgentVersion(resource)
	}

	if cfg.OverrideHostName.Enabled {
		s.overrideHostNameWithK8sNodeName(resource)
	}
	if cfg.DeploymentEnvironment.Enabled {
		s.setDeploymentEnvironment(resource)
	}

	if cfg.ServiceInstanceID.Enabled {
		s.setServiceInstanceID(resource)
	}
}

// SemConv v1.27.0 deprecated `deployment.environment` and added `deployment.environment.name` in favor of it.
// In the `otel-data` ES plugin we alias `service.environment` to `deployment.environment`.
// ES currently doesn't allow aliases with multiple targets, so if the new field name is used (SemConv v1.27+),
// we duplicate the value and also send it with the old field name to make the alias work.
func (s *resourceEnrichmentContext) setDeploymentEnvironment(resource pcommon.Resource) {
	if s.deploymentEnvironmentName != "" && s.deploymentEnvironment == "" {
		attribute.PutStr(
			resource.Attributes(),
			string(semconv25.DeploymentEnvironmentKey),
			s.deploymentEnvironmentName,
		)
	}
}

func (s *resourceEnrichmentContext) setAgentName(resource pcommon.Resource) {
	agentName := "otlp"
	if s.telemetrySDKName != "" {
		agentName = s.telemetrySDKName
	}
	switch {
	case s.telemetryDistroName != "":
		agentLang := "unknown"
		if s.telemetrySDKLanguage != "" {
			agentLang = s.telemetrySDKLanguage
		}
		agentName = fmt.Sprintf(
			"%s/%s/%s",
			agentName,
			agentLang,
			s.telemetryDistroName,
		)
	case s.telemetrySDKLanguage != "":
		agentName = fmt.Sprintf(
			"%s/%s",
			agentName,
			s.telemetrySDKLanguage,
		)
	}
	attribute.PutStr(resource.Attributes(), elasticattr.AgentName, agentName)
}

func (s *resourceEnrichmentContext) setAgentVersion(resource pcommon.Resource) {
	agentVersion := "unknown"
	switch {
	case s.telemetryDistroName != "":
		// do not fallback to the Otel SDK version if we have a
		// distro name available as this would only cause confusion
		if s.telemetryDistroVersion != "" {
			agentVersion = s.telemetryDistroVersion
		}
	case s.telemetrySDKVersion != "":
		agentVersion = s.telemetrySDKVersion
	}
	attribute.PutStr(resource.Attributes(), elasticattr.AgentVersion, agentVersion)
}

func (s *resourceEnrichmentContext) overrideHostNameWithK8sNodeName(resource pcommon.Resource) {
	if s.k8sNodeName == "" {
		return
	}
	// Host name is set same as k8s node name. In case, both host name
	// and k8s node name are set then host name is overridden as this is
	// considered an invalid configuration/smell and k8s node name is
	// given higher preference.
	resource.Attributes().PutStr(
		string(semconv.HostNameKey),
		s.k8sNodeName,
	)
}

// setServiceInstanceID sets service.instance.id from container.id or host.name.
// This follows the existing APM logic for `service.node.name`.
func (s *resourceEnrichmentContext) setServiceInstanceID(resource pcommon.Resource) {
	switch {
	case s.containerID != "":
		s.serviceInstanceID = s.containerID
	case s.hostName != "":
		s.serviceInstanceID = s.hostName
	default:
		// no instance id could be derived
		return
	}
	attribute.PutStr(resource.Attributes(), string(semconv25.ServiceInstanceIDKey), s.serviceInstanceID)
}
