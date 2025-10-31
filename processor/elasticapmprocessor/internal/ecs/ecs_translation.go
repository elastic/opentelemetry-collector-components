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

// Supported ECS resource attributes
const (
	ecsAttrServiceLanguageName       = "service.language.name"
	ecsAttrServiceLanguageVersion    = "service.language.version"
	ecsAttrServiceFrameworkName      = "service.framework.name"
	ecsAttrServiceFrameworkVersion   = "service.framework.version"
	ecsAttrServiceRuntimeName        = "service.runtime.name"
	ecsAttrServiceRuntimeVersion     = "service.runtime.version"
	ecsAttrServiceOriginID           = "service.origin.id"
	ecsAttrServiceOriginName         = "service.origin.name"
	ecsAttrServiceOriginVersion      = "service.origin.version"
	ecsAttrServiceTargetName         = "service.target.name"
	ecsAttrServiceTargetType         = "service.target.type"
	ecsAttrCloudOriginAccountID      = "cloud.origin.account.id"
	ecsAttrCloudOriginProvider       = "cloud.origin.provider"
	ecsAttrCloudOriginRegion         = "cloud.origin.region"
	ecsAttrCloudOriginServiceName    = "cloud.origin.service.name"
	ecsAttrCloudAccountName          = "cloud.account.name"
	ecsAttrCloudInstanceID           = "cloud.instance.id"
	ecsAttrCloudInstanceName         = "cloud.instance.name"
	ecsAttrCloudMachineType          = "cloud.machine.type"
	ecsAttrCloudProjectID            = "cloud.project.id"
	ecsAttrCloudProjectName          = "cloud.project.name"
	ecsAttrContainerImageTag         = "container.image.tag"
	ecsAttrHostOSPlatform            = "host.os.platform"
	ecsAttrProcessRuntimeName        = "process.runtime.name"
	ecsAttrProcessRuntimeVersion     = "process.runtime.version"
	ecsAttrDeviceManufacturer        = "device.manufacturer"
	ecsAttrDataStreamDataset         = "data_stream.dataset"
	ecsAttrDataStreamNamespace       = "data_stream.namespace"
	ecsAttrUserDomain                = "user.domain"
	ecsAttrSourceNATIP               = "source.nat.ip"
	ecsAttrDestinationIP             = "destination.ip"
	ecsAttrFaaSTriggerRequestID      = "faas.trigger.request.id"
	ecsAttrFaaSExecution             = "faas.execution"
	ecsAttrOpenCensusExporterVersion = "opencensus.exporterversion"
	ecsAttrAgentName                 = "agent.name"
	ecsAttrAgentVersion              = "agent.version"
	ecsAttrAgentEphemeralID          = "agent.ephemeral_id"
	ecsAttrAgentActivationMethod     = "agent.activation_method"
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

// isSupportedAttribute returns true if the resource attribute is
// supported by ECS and can be mapped directly.
// Supported fields can include OTEL SemConv attributes or ECS specific attributes.
// Fields are based on those found in the below areas:
// 1. apm-data: https://github.com/elastic/apm-data/blob/main/input/otlp/metadata.go
// 2. elasticapmintake receiver: https://github.com/elastic/opentelemetry-collector-components/tree/main/receiver/elasticapmintakereceiver/internal/mappers
func isSupportedAttribute(attr string) bool {
	switch attr {
	// service.*
	case string(semconv.ServiceNameKey),
		string(semconv.ServiceVersionKey),
		string(semconv.ServiceInstanceIDKey),
		string(semconv.ServiceNamespaceKey),
		ecsAttrServiceLanguageName,
		ecsAttrServiceLanguageVersion,
		ecsAttrServiceFrameworkName,
		ecsAttrServiceFrameworkVersion,
		ecsAttrServiceRuntimeName,
		ecsAttrServiceRuntimeVersion,
		ecsAttrServiceOriginID,
		ecsAttrServiceOriginName,
		ecsAttrServiceOriginVersion,
		ecsAttrServiceTargetName,
		ecsAttrServiceTargetType:
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
		string(semconv.CloudPlatformKey),
		ecsAttrCloudOriginAccountID,
		ecsAttrCloudOriginProvider,
		ecsAttrCloudOriginRegion,
		ecsAttrCloudOriginServiceName,
		ecsAttrCloudAccountName,
		ecsAttrCloudInstanceID,
		ecsAttrCloudInstanceName,
		ecsAttrCloudMachineType,
		ecsAttrCloudProjectID,
		ecsAttrCloudProjectName:
		return true

	// container.*
	case string(semconv.ContainerNameKey),
		string(semconv.ContainerIDKey),
		string(semconv.ContainerImageNameKey),
		ecsAttrContainerImageTag,
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
		string(semconv.HostIPKey),
		ecsAttrHostOSPlatform:
		return true

	// process.*
	case string(semconv.ProcessPIDKey),
		string(semconv.ProcessParentPIDKey),
		string(semconv.ProcessExecutableNameKey),
		string(semconv.ProcessCommandLineKey),
		string(semconv.ProcessExecutablePathKey),
		ecsAttrProcessRuntimeName,
		ecsAttrProcessRuntimeVersion,
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
		ecsAttrDeviceManufacturer:
		return true

	// data_stream.*
	case ecsAttrDataStreamDataset,
		ecsAttrDataStreamNamespace:
		return true

	// user.*
	case string(semconv.UserIDKey),
		string(semconv.UserEmailKey),
		string(semconv.UserNameKey),
		ecsAttrUserDomain:
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
		string(semconv.SourcePortKey),
		ecsAttrSourceNATIP:
		return true

	// destination.*
	case ecsAttrDestinationIP:
		return true

	// faas.*
	case string(semconv.FaaSInstanceKey),
		string(semconv.FaaSNameKey),
		string(semconv.FaaSVersionKey),
		string(semconv.FaaSTriggerKey),
		string(semconv.FaaSColdstartKey),
		ecsAttrFaaSTriggerRequestID,
		ecsAttrFaaSExecution:
		return true

	// Legacy OpenCensus attributes
	case ecsAttrOpenCensusExporterVersion:
		return true

	// APM Agent enrichment
	case ecsAttrAgentName,
		ecsAttrAgentVersion,
		ecsAttrAgentEphemeralID,
		ecsAttrAgentActivationMethod:
		return true
	}

	return false
}
