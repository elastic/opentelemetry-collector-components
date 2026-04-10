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

// TranslateResourceMetadata normalizes resource attributes.
// Sanitizes existing labels and numeric_labels keys.
// Moves unsupported attributes to labels with a "labels." prefix (key sanitized),
// and leaves supported ECS attributes unchanged.
func TranslateResourceMetadata(resource pcommon.Resource) {
	attributes := resource.Attributes()
	attributes.Range(func(k string, v pcommon.Value) bool {
		if sanitizeExistingLabelAttribute(attributes, k, v) {
			return true
		}

		switch k {
		case elasticattr.AgentActivationMethod,
			elasticattr.AgentEphemeralID,
			elasticattr.AgentName,
			elasticattr.AgentVersion,
			elasticattr.CloudAccountName,
			elasticattr.CloudInstanceID,
			elasticattr.CloudInstanceName,
			elasticattr.CloudMachineType,
			elasticattr.CloudOriginAccountID,
			elasticattr.CloudOriginProvider,
			elasticattr.CloudOriginRegion,
			elasticattr.CloudOriginServiceName,
			elasticattr.CloudProjectID,
			elasticattr.CloudProjectName,
			elasticattr.DataStreamDataset,
			elasticattr.DataStreamNamespace,
			elasticattr.DataStreamType,
			elasticattr.DestinationIP,
			elasticattr.FaaSExecution,
			elasticattr.FaaSTriggerRequestID,
			elasticattr.HostHostName,
			elasticattr.HostOSType,
			elasticattr.MetricsetName,
			elasticattr.ServiceFrameworkName,
			elasticattr.ServiceFrameworkVersion,
			elasticattr.ServiceOriginID,
			elasticattr.ServiceOriginName,
			elasticattr.ServiceOriginVersion,
			elasticattr.ServiceTargetName,
			elasticattr.ServiceTargetType,
			elasticattr.SourceNATIP,
			elasticattr.UserDomain,
			string(semconv.ClientAddressKey),
			string(semconv.ClientPortKey),
			string(semconv.ContainerImageTagsKey),
			string(semconv.FaaSColdstartKey),
			string(semconv.FaaSInstanceKey),
			string(semconv.FaaSNameKey),
			string(semconv.FaaSTriggerKey),
			string(semconv.FaaSVersionKey),
			string(semconv.HostIPKey),
			string(semconv.NetworkCarrierIccKey),
			string(semconv.NetworkCarrierMccKey),
			string(semconv.NetworkCarrierMncKey),
			string(semconv.NetworkCarrierNameKey),
			string(semconv.NetworkConnectionSubtypeKey),
			string(semconv.NetworkConnectionTypeKey),
			string(semconv.ProcessExecutableNameKey),
			string(semconv.ProcessParentPIDKey),
			string(semconv.ProcessPIDKey),
			string(semconv.ServiceNameKey),
			string(semconv.ServiceNamespaceKey),
			string(semconv.SourceAddressKey),
			string(semconv.SourcePortKey),
			string(semconv.TelemetryDistroNameKey),
			string(semconv.TelemetryDistroVersionKey),
			string(semconv.UserAgentOriginalKey),
			string(semconv.UserEmailKey),
			string(semconv.UserIDKey),
			string(semconv.UserNameKey),
			ecsAttrOpenCensusExporterVersion:
			return true
		case elasticattr.ContainerImageTag,
			elasticattr.DeviceManufacturer,
			string(semconv.CloudAccountIDKey),
			string(semconv.CloudAvailabilityZoneKey),
			string(semconv.CloudPlatformKey),
			string(semconv.CloudProviderKey),
			string(semconv.CloudRegionKey),
			string(semconv.ContainerIDKey),
			string(semconv.ContainerImageNameKey),
			string(semconv.ContainerNameKey),
			string(semconv.ContainerRuntimeKey),
			string(semconv26.DeploymentEnvironmentKey),
			string(semconv.DeploymentEnvironmentNameKey),
			string(semconv.DeviceIDKey),
			string(semconv.DeviceModelIdentifierKey),
			string(semconv.DeviceModelNameKey),
			string(semconv.HostArchKey),
			string(semconv.HostIDKey),
			string(semconv.HostNameKey),
			string(semconv.HostTypeKey),
			string(semconv.K8SNamespaceNameKey),
			string(semconv.K8SNodeNameKey),
			string(semconv.K8SPodNameKey),
			string(semconv.K8SPodUIDKey),
			string(semconv.OSDescriptionKey),
			string(semconv.OSNameKey),
			string(semconv.OSTypeKey),
			string(semconv.OSVersionKey),
			string(semconv.ProcessCommandLineKey),
			string(semconv.ProcessExecutablePathKey),
			string(semconv.ProcessOwnerKey),
			string(semconv.ProcessRuntimeNameKey),
			string(semconv.ProcessRuntimeVersionKey),
			string(semconv.ServiceInstanceIDKey),
			string(semconv.ServiceVersionKey),
			string(semconv.TelemetrySDKLanguageKey),
			string(semconv.TelemetrySDKNameKey),
			string(semconv.TelemetrySDKVersionKey):
			truncatePreservedStringAttribute(attributes, k, v)
			return true
		default:
			fallbackToLabelAttribute(attributes, k, v)
			return true
		}
	})
}

// TranslateLogRecordAttributes applies the apm-data OTLP fallback behaviour for
// log record attributes in ECS mode: known semantic fields are preserved, while
// unsupported attributes are moved to labels.* / numeric_labels.* with a
// sanitized key.
func TranslateLogRecordAttributes(attributes pcommon.Map) {
	attributes.Range(func(k string, v pcommon.Value) bool {
		if sanitizeExistingLabelAttribute(attributes, k, v) {
			return true
		}

		switch k {
		case elasticattr.DataStreamDataset,
			elasticattr.DataStreamNamespace,
			elasticattr.DataStreamType,
			elasticattr.ErrorID,
			elasticattr.ProcessorEvent,
			elasticattr.SessionID,
			string(semconv26.ExceptionEscapedKey),
			string(semconv.ExceptionMessageKey),
			string(semconv.ExceptionStacktraceKey),
			string(semconv.ExceptionTypeKey),
			string(semconv.NetworkConnectionTypeKey),
			"event.domain",
			"event.name":
			return true
		default:
			fallbackToLabelAttribute(attributes, k, v)
			return true
		}
	})
}

// TranslateSpanAttributes applies the apm-data OTLP span fallback behaviour for
// ECS mode spans. The preserved attributes and fallback-to-label cases are based
// on the OTLP span translation in apm-data's input/otlp/traces.go:
// https://github.com/elastic/apm-data/blob/7da222dcc0320f9c812c5d72f65f830c838aae11/input/otlp/traces.go
//
// Attributes required by span enrichment or exporter-side ECS conversions are
// preserved, while unsupported attributes are moved to labels.* /
// numeric_labels.* with a sanitized key.
func TranslateSpanAttributes(attributes pcommon.Map) {
	attributes.Range(func(k string, v pcommon.Value) bool {
		if sanitizeExistingLabelAttribute(attributes, k, v) {
			return true
		}

		switch k {
		case elasticattr.DataStreamDataset,
			elasticattr.DataStreamNamespace,
			elasticattr.DataStreamType,
			elasticattr.TransactionType,
			"code.stacktrace",
			"db.name",
			"db.namespace",
			"db.query.text",
			"db.statement",
			"db.system",
			"db.system.name",
			"db.user",
			"gen_ai.provider.name",
			"gen_ai.system",
			"http.flavor",
			"http.method",
			"http.request.method",
			"http.response.body.size",
			"http.response.status_code",
			"http.scheme",
			"http.status_code",
			"http.target",
			"http.url",
			"http.user_agent",
			"messaging.destination.name",
			"messaging.destination.temporary",
			"messaging.operation",
			"messaging.operation.name",
			"messaging.system",
			"net.host.name",
			"net.peer.name",
			"net.peer.port",
			"network.carrier.icc",
			"network.carrier.mcc",
			"network.carrier.mnc",
			"network.carrier.name",
			"network.connection.subtype",
			"network.connection.type",
			"peer.service",
			"rpc.grpc.status_code",
			"rpc.method",
			"rpc.response.status_code",
			"rpc.service",
			"rpc.system",
			"rpc.system.name",
			"server.address",
			"server.port",
			"service.peer.name",
			"session.id",
			"type",
			"url.domain",
			"url.full",
			"url.path",
			"url.port",
			"url.query",
			"url.scheme",
			"user_agent.name",
			"user_agent.original",
			"user_agent.version":
			return true
		default:
			fallbackToLabelAttribute(attributes, k, v)
			return true
		}
	})
}

// TranslateMetricDataPointAttributes applies the apm-data OTLP metric fallback
// for raw metric datapoint attributes in ECS mode. Existing labels.* /
// numeric_labels.* keys are sanitized in place, metric-specific special cases
// are preserved, and everything else is moved to labels.* / numeric_labels.*.
func TranslateMetricDataPointAttributes(attributes pcommon.Map) {
	attributes.Range(func(k string, v pcommon.Value) bool {
		if sanitizeExistingLabelAttribute(attributes, k, v) {
			return true
		}

		switch k {
		case elasticattr.DataStreamDataset,
			elasticattr.DataStreamNamespace,
			elasticattr.DataStreamType,
			elasticattr.EventDataset,
			"event.module",
			"system.process.cpu.start_time",
			"system.process.state":
			return true
		case string(semconv.UserNameKey),
			"system.filesystem.mount_point",
			"system.process.cmdline":
			truncatePreservedStringAttribute(attributes, k, v)
			return true
		default:
			fallbackToLabelAttribute(attributes, k, v)
			return true
		}
	})
}

func sanitizeExistingLabelAttribute(attributes pcommon.Map, key string, value pcommon.Value) bool {
	if !sanitize.IsLabelAttribute(key) {
		return false
	}
	sanitized := sanitize.HandleLabelAttributeKey(key)
	if sanitized != key {
		value.CopyTo(attributes.PutEmpty(sanitized))
		attributes.Remove(key)
	}
	return true
}

func truncatePreservedStringAttribute(attributes pcommon.Map, key string, value pcommon.Value) {
	if value.Type() != pcommon.ValueTypeStr {
		return
	}
	truncated := sanitize.Truncate(value.Str())
	if truncated != value.Str() {
		attributes.PutStr(key, truncated)
	}
}

func fallbackToLabelAttribute(attributes pcommon.Map, key string, value pcommon.Value) {
	setLabelAttributeValue(attributes, sanitize.HandleAttributeKey(key), value)
	attributes.Remove(key)
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
