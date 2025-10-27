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
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv26 "go.opentelemetry.io/otel/semconv/v1.26.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

func TranslateResourceMetadata(resource pcommon.Resource) {
	attributes := resource.Attributes()

	attributes.Range(func(k string, v pcommon.Value) bool {
		if !isSupportedAttribute(k) {
			// The elasticapmintake receiver moves labels and numeric_labels into attributes and
			// already prefixes those with "labels." and "numeric_labels." respectively and also does de-dotting.
			// So for those, we don't want to double prefix - we just leave them as is.
			if strings.HasPrefix(k, "labels.") {
				attributes.PutStr(k, v.AsString())
			} else if strings.HasPrefix(k, "numeric_labels.") {
				attributes.PutDouble(k, v.Double())
			} else {
				// Other attributes that are not supported by ECS are moved to labels with a "labels." prefix.
				attributes.PutStr("labels."+replaceDots(k), v.AsString())
				attributes.Remove(k)
			}
		}
		return true
	})
}

func replaceDots(key string) string {
	return strings.ReplaceAll(key, ".", "_")
}

// This is based to what found in the apm-data repo
// (e.g https://github.com/elastic/apm-data/blob/main/input/otlp/metadata.go)
// plus other extra fields
func isSupportedAttribute(attr string) bool {
	switch attr {
	// service.*
	case string(semconv.ServiceNameKey),
		string(semconv.ServiceVersionKey),
		string(semconv.ServiceInstanceIDKey),
		string(semconv.ServiceNamespaceKey),
		"service.language.name",
		"service.language.version":
		return true

	// deployment.*
	case string(semconv26.DeploymentEnvironmentKey), string(semconv.DeploymentEnvironmentNameKey):
		return true

	// telemetry.sdk.*
	case string(semconv.TelemetrySDKNameKey),
		string(semconv.TelemetrySDKVersionKey),
		string(semconv.TelemetrySDKLanguageKey):
		return true

	// cloud.*
	case string(semconv.CloudProviderKey),
		string(semconv.CloudAccountIDKey),
		string(semconv.CloudRegionKey),
		string(semconv.CloudAvailabilityZoneKey),
		string(semconv.CloudPlatformKey):
		return true

	// container.*
	case string(semconv.ContainerNameKey),
		string(semconv.ContainerIDKey),
		string(semconv.ContainerImageNameKey),
		string(semconv.ContainerImageTagsKey),
		string(semconv.ContainerRuntimeKey):
		return true

	// k8s.*
	case string(semconv.K8SNamespaceNameKey),
		string(semconv.K8SNodeNameKey),
		string(semconv.K8SPodNameKey),
		string(semconv.K8SPodUIDKey):
		return true

	// host.*
	case string(semconv.HostNameKey),
		string(semconv.HostIDKey),
		string(semconv.HostTypeKey),
		string(semconv.HostArchKey),
		string(semconv.HostIPKey):
		return true

	// process.*
	case string(semconv.ProcessPIDKey),
		string(semconv.ProcessParentPIDKey),
		string(semconv.ProcessExecutableNameKey),
		string(semconv.ProcessCommandLineKey),
		string(semconv.ProcessExecutablePathKey),
		"process.runtime.name",
		"process.runtime.version",
		string(semconv.ProcessOwnerKey):
		return true

	// os.*
	case string(semconv.OSTypeKey),
		string(semconv.OSDescriptionKey),
		string(semconv.OSNameKey),
		string(semconv.OSVersionKey):
		return true

	// device.*
	case string(semconv.DeviceIDKey),
		string(semconv.DeviceModelIdentifierKey),
		string(semconv.DeviceModelNameKey),
		"device.manufacturer":
		return true

	// data_stream.*
	case "data_stream.dataset",
		"data_stream.namespace":
		return true

	// user.*
	case string(semconv.UserIDKey),
		string(semconv.UserEmailKey),
		string(semconv.UserNameKey):
		return true

	// user_agent.*
	case string(semconv.UserAgentOriginalKey):
		return true

	// network.*
	case string(semconv.NetworkConnectionTypeKey),
		string(semconv.NetworkConnectionSubtypeKey),
		string(semconv.NetworkCarrierNameKey),
		string(semconv.NetworkCarrierMccKey),
		string(semconv.NetworkCarrierMncKey),
		string(semconv.NetworkCarrierIccKey):
		return true

	// client.*
	case string(semconv.ClientAddressKey),
		string(semconv.ClientPortKey):
		return true

	// source.*
	case string(semconv.SourceAddressKey),
		string(semconv.SourcePortKey):
		return true

	// faas.*
	case string(semconv.FaaSInstanceKey),
		string(semconv.FaaSNameKey),
		string(semconv.FaaSVersionKey),
		string(semconv.FaaSTriggerKey),
		string(semconv.FaaSColdstartKey):
		return true

	// Legacy OpenCensus attributes
	case "opencensus.exporterversion":
		return true

	// APM Agent enrichment
	case "agent.name",
		"agent.version":
		return true
	}

	return false
}
