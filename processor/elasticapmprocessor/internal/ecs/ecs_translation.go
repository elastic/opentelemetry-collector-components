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

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/sanitize"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv26 "go.opentelemetry.io/otel/semconv/v1.26.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// Supported ECS resource attributes
const (
	ecsAttrOpenCensusExporterVersion = "opencensus.exporterversion"
)

type attrAction uint8

const (
	attrActionFallback attrAction = iota
	attrActionPreserve
	attrActionTruncateAndPreserve
)

// TranslateResourceMetadata normalizes resource attributes.
// Sanitizes existing labels and numeric_labels keys.
// Moves unsupported attributes to labels with a "labels." prefix (key sanitized),
// and leaves supported ECS attributes unchanged.
func TranslateResourceMetadata(resource pcommon.Resource) {
	translateAttributes(resource.Attributes(), classifyResourceAttribute)
}

// TranslateLogRecordAttributes applies the apm-data OTLP fallback behaviour for
// log record attributes in ECS mode: known semantic fields are preserved, while
// unsupported attributes are moved to labels.* / numeric_labels.* with a
// sanitized key.
func TranslateLogRecordAttributes(attributes pcommon.Map) {
	translateAttributes(attributes, classifyLogRecordAttribute)
}

// TranslateMetricDataPointAttributes applies the apm-data OTLP metric fallback
// for raw metric datapoint attributes in ECS mode. Existing labels.* /
// numeric_labels.* keys are sanitized in place, metric-specific special cases
// are preserved, and everything else is moved to labels.* / numeric_labels.*.
func TranslateMetricDataPointAttributes(attributes pcommon.Map) {
	translateAttributes(attributes, classifyMetricDataPointAttribute)
}

func translateAttributes(attributes pcommon.Map, classify func(string, pcommon.Value) attrAction) {
	attributes.Range(func(k string, v pcommon.Value) bool {
		if sanitize.IsLabelAttribute(k) {
			sanitized := sanitize.HandleLabelAttributeKey(k)
			if sanitized != k {
				v.CopyTo(attributes.PutEmpty(sanitized))
				attributes.Remove(k)
			}
			return true
		}

		switch classify(k, v) {
		case attrActionPreserve:
			return true
		case attrActionTruncateAndPreserve:
			truncated := sanitize.Truncate(v.Str())
			if truncated != v.Str() {
				attributes.PutStr(k, truncated)
			}
			return true
		default:
			// Attributes not supported by ECS are moved to labels with a
			// labels./numeric_labels. prefix depending on their value type.
			setLabelAttributeValue(attributes, sanitize.HandleAttributeKey(k), v)
			attributes.Remove(k)
			return true
		}
	})
}

// shouldPreserveAndTruncateIfString determines if the value should be preserved, optionally truncating strings before preserving.
func shouldPreserveAndTruncateIfString(value pcommon.Value) attrAction {
	if value.Type() == pcommon.ValueTypeStr {
		return attrActionTruncateAndPreserve
	}
	return attrActionPreserve
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
		attributes.PutStr("labels."+key, sanitize.Truncate(value.Str()))
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
					target.AppendEmpty().SetStr(sanitize.Truncate(item.Str()))
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

// classifyLogRecordAttribute is based on the OTLP log-record attribute switch
// in apm-data/input/otlp/logs.go, which preserves exception.*, event.name,
// event.domain, session.id, network.connection.type, and data_stream.* as
// first-class fields and sends everything else through setLabel(replaceDots(k), ...).
//
// Unlike resource metadata and some metric special cases, apm-data does not
// truncate these preserved log-record fields. Truncation for log attributes only
// happens on the fallback label path via setLabel, which is mirrored here by
// setLabelAttributeValue.
//
// This allowlist also keeps processor-added fields like processor.event,
// error.id, and data_stream.type so they survive the collector-side translation pass.
func classifyLogRecordAttribute(attr string, _ pcommon.Value) attrAction {
	switch attr {
	case string(semconv26.ExceptionEscapedKey),
		string(semconv.ExceptionMessageKey),
		string(semconv.ExceptionStacktraceKey),
		string(semconv.ExceptionTypeKey),
		string(semconv.NetworkConnectionTypeKey),
		elasticattr.DataStreamDataset,
		elasticattr.DataStreamNamespace,
		elasticattr.DataStreamType,
		elasticattr.ErrorID,
		"event.domain",
		"event.name",
		elasticattr.ProcessorEvent,
		elasticattr.SessionID:
		return attrActionPreserve
	}

	return attrActionFallback
}

// classifyMetricDataPointAttribute mirrors the apm-data OTLP metric
// datapoint handling where a small set of fields are preserved as first-class
// values and the rest fall back to labels.* / numeric_labels.*.
//
// The collector preserves data_stream.type in addition to the apm-data
// data_stream dataset/namespace handling, since datapoint-level routing depends
// on the full data_stream triple before export.
func classifyMetricDataPointAttribute(attr string, value pcommon.Value) attrAction {
	switch attr {
	case elasticattr.DataStreamDataset,
		elasticattr.DataStreamNamespace,
		elasticattr.DataStreamType,
		elasticattr.EventDataset,
		"event.module",
		"system.process.cpu.start_time",
		"system.process.state":
		return attrActionPreserve
	case "system.process.cmdline",
		"system.filesystem.mount_point",
		string(semconv.UserNameKey):
		return shouldPreserveAndTruncateIfString(value)
	}

	return attrActionFallback
}

// classifyResourceAttribute returns the action required for a supported ECS
// resource attribute. Supported fields can include OTEL SemConv attributes or
// ECS specific attributes.
//
// Fields are based on those found in the below areas:
// 1. apm-data: https://github.com/elastic/apm-data/blob/main/input/otlp/metadata.go
// 2. elasticapmintake receiver: https://github.com/elastic/opentelemetry-collector-components/tree/main/receiver/elasticapmintakereceiver/internal/mappers
//
// Where apm-data truncates a preserved resource string when populating the APM
// event, we mirror that here with attrActionTruncateAndPreserve so resource
// translation stays single-pass.
func classifyResourceAttribute(attr string, value pcommon.Value) attrAction {
	switch attr {
	// service.*
	case string(semconv.ServiceNameKey):
		return attrActionPreserve
	case string(semconv.ServiceVersionKey),
		string(semconv.ServiceInstanceIDKey):
		return shouldPreserveAndTruncateIfString(value)
	case string(semconv.ServiceNamespaceKey),
		elasticattr.ServiceFrameworkName,
		elasticattr.ServiceFrameworkVersion,
		elasticattr.ServiceOriginID,
		elasticattr.ServiceOriginName,
		elasticattr.ServiceOriginVersion,
		elasticattr.ServiceTargetName,
		elasticattr.ServiceTargetType:
		return attrActionPreserve

	// deployment.*
	case string(semconv26.DeploymentEnvironmentKey), string(semconv.DeploymentEnvironmentNameKey):
		return shouldPreserveAndTruncateIfString(value)

	// telemetry.sdk.*
	case string(semconv.TelemetrySDKNameKey),
		string(semconv.TelemetrySDKVersionKey),
		string(semconv.TelemetrySDKLanguageKey):
		return shouldPreserveAndTruncateIfString(value)
	case string(semconv.TelemetryDistroNameKey),
		string(semconv.TelemetryDistroVersionKey):
		return attrActionPreserve

	// cloud.*
	case string(semconv.CloudProviderKey),
		string(semconv.CloudAccountIDKey),
		string(semconv.CloudRegionKey),
		string(semconv.CloudAvailabilityZoneKey),
		string(semconv.CloudPlatformKey):
		return shouldPreserveAndTruncateIfString(value)
	case elasticattr.CloudOriginAccountID,
		elasticattr.CloudOriginProvider,
		elasticattr.CloudOriginRegion,
		elasticattr.CloudOriginServiceName,
		elasticattr.CloudAccountName,
		elasticattr.CloudInstanceID,
		elasticattr.CloudInstanceName,
		elasticattr.CloudMachineType,
		elasticattr.CloudProjectID,
		elasticattr.CloudProjectName:
		return attrActionPreserve

	// container.*
	case string(semconv.ContainerNameKey),
		string(semconv.ContainerIDKey),
		string(semconv.ContainerImageNameKey),
		elasticattr.ContainerImageTag,
		string(semconv.ContainerRuntimeKey):
		return shouldPreserveAndTruncateIfString(value)
	case string(semconv.ContainerImageTagsKey):
		return attrActionPreserve

	// k8s.*
	case string(semconv.K8SNamespaceNameKey),
		string(semconv.K8SNodeNameKey),
		string(semconv.K8SPodNameKey),
		string(semconv.K8SPodUIDKey):
		return shouldPreserveAndTruncateIfString(value)

	// host.*
	case string(semconv.HostNameKey),
		string(semconv.HostIDKey),
		string(semconv.HostTypeKey),
		string(semconv.HostArchKey):
		return shouldPreserveAndTruncateIfString(value)
	case elasticattr.HostHostName,
		string(semconv.HostIPKey),
		elasticattr.HostOSType:
		return attrActionPreserve

	// process.*
	case string(semconv.ProcessCommandLineKey),
		string(semconv.ProcessExecutablePathKey),
		string(semconv.ProcessRuntimeNameKey),
		string(semconv.ProcessRuntimeVersionKey),
		string(semconv.ProcessOwnerKey):
		return shouldPreserveAndTruncateIfString(value)
	case string(semconv.ProcessPIDKey),
		string(semconv.ProcessParentPIDKey),
		string(semconv.ProcessExecutableNameKey):
		return attrActionPreserve

	// os.*
	case string(semconv.OSTypeKey),
		string(semconv.OSDescriptionKey),
		string(semconv.OSNameKey),
		string(semconv.OSVersionKey):
		return shouldPreserveAndTruncateIfString(value)

	// device.*
	case string(semconv.DeviceIDKey),
		string(semconv.DeviceModelIdentifierKey),
		string(semconv.DeviceModelNameKey),
		elasticattr.DeviceManufacturer:
		return shouldPreserveAndTruncateIfString(value)

	// data_stream.*
	case elasticattr.DataStreamDataset,
		elasticattr.DataStreamNamespace:
		return attrActionPreserve

	// user.*
	case string(semconv.UserIDKey),
		string(semconv.UserEmailKey),
		string(semconv.UserNameKey),
		elasticattr.UserDomain:
		return attrActionPreserve

	// user_agent.*
	case string(semconv.UserAgentOriginalKey):
		return attrActionPreserve

	// network.*
	case string(semconv.NetworkConnectionTypeKey),
		string(semconv.NetworkConnectionSubtypeKey),
		string(semconv.NetworkCarrierNameKey),
		string(semconv.NetworkCarrierMccKey),
		string(semconv.NetworkCarrierMncKey),
		string(semconv.NetworkCarrierIccKey):
		return attrActionPreserve

	// client.*
	case string(semconv.ClientAddressKey),
		string(semconv.ClientPortKey):
		return attrActionPreserve

	// source.*
	case string(semconv.SourceAddressKey),
		string(semconv.SourcePortKey),
		elasticattr.SourceNATIP:
		return attrActionPreserve

	// destination.*
	case elasticattr.DestinationIP:
		return attrActionPreserve

	// faas.*
	case string(semconv.FaaSInstanceKey),
		string(semconv.FaaSNameKey),
		string(semconv.FaaSVersionKey),
		string(semconv.FaaSTriggerKey),
		string(semconv.FaaSColdstartKey),
		elasticattr.FaaSTriggerRequestID,
		elasticattr.FaaSExecution:
		return attrActionPreserve

	// Legacy OpenCensus attributes
	case ecsAttrOpenCensusExporterVersion:
		return attrActionPreserve

	// APM Agent enrichment
	case elasticattr.AgentName,
		elasticattr.AgentVersion,
		elasticattr.AgentEphemeralID,
		elasticattr.AgentActivationMethod:
		return attrActionPreserve

	// Metrics
	case elasticattr.MetricsetName:
		return attrActionPreserve
	}

	return attrActionFallback
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
