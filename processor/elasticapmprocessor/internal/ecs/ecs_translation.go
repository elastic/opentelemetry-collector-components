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
	"strconv"
	"strings"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv26 "go.opentelemetry.io/otel/semconv/v1.26.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// Supported ECS resource attributes
const (
	keywordLength                    = 1024
	ecsAttrOpenCensusExporterVersion = "opencensus.exporterversion"
)

// TranslateResourceMetadata normalizes resource attributes.
// Sanitizes existing labels and numeric_labels keys.
// Moves unsupported attributes to labels with a "labels." prefix (key sanitized),
// and leaves supported ECS attributes unchanged.
func TranslateResourceMetadata(resource pcommon.Resource) {
	attributes := resource.Attributes()

	attributes.Range(func(k string, v pcommon.Value) bool {
		if isLabelAttribute(k) {
			sanitized := sanitizeLabelAttributeKey(k)
			if sanitized != k {
				v.CopyTo(attributes.PutEmpty(sanitized))
				attributes.Remove(k)
			}
		} else if !isSupportedAttribute(k) {
			// Other attributes that are not supported by ECS are moved to labels with a "labels." prefix.
			setLabelAttributeValue(attributes, sanitizeLabelKey(k), v)
			attributes.Remove(k)
		}
		return true
	})
}

// sanitizeLabelAttributeKey sanitizes the key portion of a label attribute,
// preserving the "labels." or "numeric_labels." prefix.
func sanitizeLabelAttributeKey(attr string) string {
	if strings.HasPrefix(attr, "labels.") {
		return "labels." + sanitizeLabelKey(strings.TrimPrefix(attr, "labels."))
	}
	if strings.HasPrefix(attr, "numeric_labels.") {
		return "numeric_labels." + sanitizeLabelKey(strings.TrimPrefix(attr, "numeric_labels."))
	}
	return attr
}

// sanitizeLabelKey sanitizes a label key, replacing the reserved characters
// '.', '*' and '"' with '_'. This matches the apm-server behavior.
// This matches the logic in the apm-data library here:
// https://github.com/elastic/apm-data/blob/e3e170b/model/modeljson/labels.go.
func sanitizeLabelKey(k string) string {
	if strings.ContainsAny(k, ".*\"") {
		return strings.Map(replaceReservedLabelKeyRune, k)
	}
	return k
}

func replaceReservedLabelKeyRune(r rune) rune {
	switch r {
	case '.', '*', '"':
		return '_'
	}
	return r
}

// isLabelAttribute returns true if the resource attribute is already a prefixed label.
// The elasticapmintake receiver moves labels and numeric_labels into attributes and
// already prefixes those with "labels." and "numeric_labels." respectively and also does de-dotting.
// So for those, we don't want to double prefix - we just leave them as is.
func isLabelAttribute(attr string) bool {
	return strings.HasPrefix(attr, "labels.") || strings.HasPrefix(attr, "numeric_labels.")
}

// truncate returns s truncated at n runes, and the number of runes in the resulting string (<= n).
func truncate(s string) string {
	var j int
	for i := range s {
		if j == keywordLength {
			return s[:i]
		}
		j++
	}
	return s
}

// setLabelAttributeValue maps a value into labels.* / numeric_labels.*.
// Elasticsearch label mappings only support flat scalar values and
// homogeneous arrays thereof; Map, Bytes, and empty types cannot be
// stored and are intentionally dropped. This matches the behaviour of
// apm-data's setLabel (input/otlp/metadata.go) which also silently
// ignores these types.
func setLabelAttributeValue(attributes pcommon.Map, key string, value pcommon.Value) {
	switch value.Type() {
	case pcommon.ValueTypeStr:
		attributes.PutStr("labels."+key, truncate(value.Str()))
	case pcommon.ValueTypeBool:
		attributes.PutStr("labels."+key, strconv.FormatBool(value.Bool()))
	case pcommon.ValueTypeInt:
		attributes.PutDouble("numeric_labels."+key, float64(value.Int()))
	case pcommon.ValueTypeDouble:
		attributes.PutDouble("numeric_labels."+key, value.Double())
	case pcommon.ValueTypeSlice:
		slice := value.Slice()
		if slice.Len() == 0 {
			return
		}
		switch slice.At(0).Type() {
		case pcommon.ValueTypeStr:
			target := attributes.PutEmptySlice("labels." + key)
			for i := 0; i < slice.Len(); i++ {
				item := slice.At(i)
				if item.Type() == pcommon.ValueTypeStr {
					target.AppendEmpty().SetStr(truncate(item.Str()))
				}
			}
		case pcommon.ValueTypeBool:
			target := attributes.PutEmptySlice("labels." + key)
			for i := 0; i < slice.Len(); i++ {
				item := slice.At(i)
				if item.Type() == pcommon.ValueTypeBool {
					target.AppendEmpty().SetStr(strconv.FormatBool(item.Bool()))
				}
			}
		case pcommon.ValueTypeDouble:
			target := attributes.PutEmptySlice("numeric_labels." + key)
			for i := 0; i < slice.Len(); i++ {
				item := slice.At(i)
				if item.Type() == pcommon.ValueTypeDouble {
					target.AppendEmpty().SetDouble(item.Double())
				}
			}
		case pcommon.ValueTypeInt:
			target := attributes.PutEmptySlice("numeric_labels." + key)
			for i := 0; i < slice.Len(); i++ {
				item := slice.At(i)
				if item.Type() == pcommon.ValueTypeInt {
					target.AppendEmpty().SetDouble(float64(item.Int()))
				}
			}
		default:
		}
	case pcommon.ValueTypeMap, pcommon.ValueTypeBytes, pcommon.ValueTypeEmpty:
		return
	}
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
		elasticattr.ServiceLanguageName,
		elasticattr.ServiceLanguageVersion,
		elasticattr.ServiceFrameworkName,
		elasticattr.ServiceFrameworkVersion,
		elasticattr.ServiceRuntimeName,
		elasticattr.ServiceRuntimeVersion,
		elasticattr.ServiceOriginID,
		elasticattr.ServiceOriginName,
		elasticattr.ServiceOriginVersion,
		elasticattr.ServiceTargetName,
		elasticattr.ServiceTargetType:
		return true

	// deployment.*
	case string(semconv26.DeploymentEnvironmentKey), string(semconv.DeploymentEnvironmentNameKey):
		return true

	// telemetry.sdk.*
	case string(semconv.TelemetrySDKNameKey),
		string(semconv.TelemetrySDKVersionKey),
		string(semconv.TelemetrySDKLanguageKey),
		string(semconv.TelemetryDistroNameKey),
		string(semconv.TelemetryDistroVersionKey):
		return true

	// cloud.*
	case string(semconv.CloudProviderKey),
		string(semconv.CloudAccountIDKey),
		string(semconv.CloudRegionKey),
		string(semconv.CloudAvailabilityZoneKey),
		string(semconv.CloudPlatformKey),
		elasticattr.CloudOriginAccountID,
		elasticattr.CloudOriginProvider,
		elasticattr.CloudOriginRegion,
		elasticattr.CloudOriginServiceName,
		elasticattr.CloudAccountName,
		elasticattr.CloudInstanceID,
		elasticattr.CloudInstanceName,
		elasticattr.CloudMachineType,
		elasticattr.CloudProjectID,
		elasticattr.CloudProjectName:
		return true

	// container.*
	case string(semconv.ContainerNameKey),
		string(semconv.ContainerIDKey),
		string(semconv.ContainerImageNameKey),
		elasticattr.ContainerImageTag,
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
		elasticattr.HostHostName, // legacy hostname key for backwards compatibility
		string(semconv.HostIDKey),
		string(semconv.HostTypeKey),
		string(semconv.HostArchKey),
		string(semconv.HostIPKey),
		elasticattr.HostOSPlatform:
		return true

	// process.*
	case string(semconv.ProcessPIDKey),
		string(semconv.ProcessParentPIDKey),
		string(semconv.ProcessExecutableNameKey),
		string(semconv.ProcessCommandLineKey),
		string(semconv.ProcessExecutablePathKey),
		elasticattr.ProcessRuntimeName,
		elasticattr.ProcessRuntimeVersion,
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
		elasticattr.DeviceManufacturer:
		return true

	// data_stream.*
	case elasticattr.DataStreamDataset,
		elasticattr.DataStreamNamespace:
		return true

	// user.*
	case string(semconv.UserIDKey),
		string(semconv.UserEmailKey),
		string(semconv.UserNameKey),
		elasticattr.UserDomain:
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
		elasticattr.SourceNATIP:
		return true

	// destination.*
	case elasticattr.DestinationIP:
		return true

	// faas.*
	case string(semconv.FaaSInstanceKey),
		string(semconv.FaaSNameKey),
		string(semconv.FaaSVersionKey),
		string(semconv.FaaSTriggerKey),
		string(semconv.FaaSColdstartKey),
		elasticattr.FaaSTriggerRequestID,
		elasticattr.FaaSExecution:
		return true

	// Legacy OpenCensus attributes
	case ecsAttrOpenCensusExporterVersion:
		return true

	// APM Agent enrichment
	case elasticattr.AgentName,
		elasticattr.AgentVersion,
		elasticattr.AgentEphemeralID,
		elasticattr.AgentActivationMethod:
		return true

	// Metrics
	case elasticattr.MetricsetName:
		return true
	}

	return false
}

func ApplyResourceConventions(resource pcommon.Resource) {
	setHostnameFromKubernetes(resource)
}

// setHostnameFromKubernetes sets the host.hostname attribute based on kubernetes attributes for backwards compatibility with MIS and APM Server.
func setHostnameFromKubernetes(resource pcommon.Resource) {
	attrs := resource.Attributes()

	hostName, hostNameExists := attrs.Get(string(semconv.HostNameKey))
	k8sNodeName, k8sNodeNameExists := attrs.Get(string(semconv.K8SNodeNameKey))
	k8sPodName, k8sPodNameExists := attrs.Get(string(semconv.K8SPodNameKey))
	k8sPodUID, k8sPodUIDExists := attrs.Get(string(semconv.K8SPodUIDKey))
	k8sNamespace, k8sNamespaceExists := attrs.Get(string(semconv.K8SNamespaceNameKey))

	if k8sNodeNameExists && k8sNodeName.Str() != "" {
		// kubernetes.node.name is set: set host.hostname to its value
		attrs.PutStr(elasticattr.HostHostName, k8sNodeName.Str())
	} else if (k8sPodNameExists && k8sPodName.Str() != "") ||
		(k8sPodUIDExists && k8sPodUID.Str() != "") ||
		(k8sNamespaceExists && k8sNamespace.Str() != "") {
		// kubernetes.* is set but kubernetes.node.name is not: don't set host.hostname
		attrs.Remove(elasticattr.HostHostName)
	}

	// If host.name is not set but host.hostname is, use hostname as name
	hostHostname, hostHostnameExists := attrs.Get(elasticattr.HostHostName)
	if (!hostNameExists || hostName.Str() == "") && hostHostnameExists && hostHostname.Str() != "" {
		attrs.PutStr(string(semconv.HostNameKey), hostHostname.Str())
	}
}
