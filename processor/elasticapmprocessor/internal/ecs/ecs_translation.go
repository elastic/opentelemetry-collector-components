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
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/strutil"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv12 "go.opentelemetry.io/otel/semconv/v1.12.0"
	semconv16 "go.opentelemetry.io/otel/semconv/v1.16.0"
	semconv25 "go.opentelemetry.io/otel/semconv/v1.25.0"
	semconv26 "go.opentelemetry.io/otel/semconv/v1.26.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	semconv37 "go.opentelemetry.io/otel/semconv/v1.37.0"
	semconv39 "go.opentelemetry.io/otel/semconv/v1.39.0"
)

// Supported ECS resource attributes
const (
	ecsAttrOpenCensusExporterVersion = "opencensus.exporterversion"
)

// kv is a key-value pair for collecting deferred attribute insertions after a RemoveIf pass.
type kv struct {
	k string
	v pcommon.Value
}

// TranslateResourceMetadata normalizes resource attributes.
// Moves unsupported attributes to labels.* / numeric_labels.* (key sanitized),
// and leaves supported ECS attributes unchanged.
//
// When sanitizeExistingLabels is true (APM intake path), existing labels.* /
// numeric_labels.* keys have only their suffix sanitized (reserved characters
// replaced). All other unsupported attributes are fully re-normalized.
// When false (OTel path), all unsupported attributes — including any that
// already carry a labels.* prefix — are treated as raw keys and re-normalized
// from scratch.
func TranslateResourceMetadata(resource pcommon.Resource, sanitizeExistingLabels bool) {
	attributes := resource.Attributes()
	var toAppend []kv
	attributes.RemoveIf(func(k string, v pcommon.Value) bool {
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
			"telemetry.sdk.elastic_export_timestamp",
			string(semconv.UserAgentOriginalKey),
			string(semconv.UserEmailKey),
			string(semconv.UserIDKey),
			string(semconv.UserNameKey),
			ecsAttrOpenCensusExporterVersion:
			return false
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
			return false
		default:
			if sanitizeExistingLabels {
				for _, prefix := range []string{"labels.", "numeric_labels."} {
					if strings.HasPrefix(k, prefix) {
						sanitized, changed := sanitizeExistingLabelKey(k, prefix)
						if changed {
							newV := pcommon.NewValueEmpty()
							v.CopyTo(newV)
							toAppend = append(toAppend, kv{k: sanitized, v: newV})
							return true
						}
						return false
					}
				}
			}
			if label := getLabelAttributeValue(k, v); label.k != "" {
				toAppend = append(toAppend, label)
			}
			return true
		}
	})
	for _, l := range toAppend {
		l.v.CopyTo(attributes.PutEmpty(l.k))
	}
}

// RemapLogRecordAttributesToECSLabels applies the apm-data OTLP fallback behaviour for
// log record attributes in ECS mode: known semantic fields are preserved, while
// unsupported attributes are moved to labels.* / numeric_labels.* with a
// sanitized key.
func RemapLogRecordAttributesToECSLabels(attributes pcommon.Map) {
	var toAppend []kv
	attributes.RemoveIf(func(k string, v pcommon.Value) bool {
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
			return false
		default:
			if label := getLabelAttributeValue(k, v); label.k != "" {
				toAppend = append(toAppend, label)
			}
			return true
		}
	})
	for _, l := range toAppend {
		l.v.CopyTo(attributes.PutEmpty(l.k))
	}
}

// RemapSpanAttributesToECSLabels applies the apm-data OTLP span fallback behaviour for
// ECS mode spans. The preserved attributes and fallback-to-label cases are based
// on the OTLP span translation in apm-data's input/otlp/traces.go:
// https://github.com/elastic/apm-data/blob/7da222dcc0320f9c812c5d72f65f830c838aae11/input/otlp/traces.go
//
// Attributes required by span enrichment or exporter-side ECS conversions are
// preserved, while unsupported attributes are moved to labels.* /
// numeric_labels.* with a sanitized key.
func RemapSpanAttributesToECSLabels(attributes pcommon.Map) {
	var toAppend []kv
	attributes.RemoveIf(func(k string, v pcommon.Value) bool {
		switch k {
		// data_stream.*
		case elasticattr.DataStreamDataset,
			elasticattr.DataStreamNamespace,
			elasticattr.DataStreamType,
			elasticattr.ServiceTargetName,
			elasticattr.ServiceTargetType,
			elasticattr.SpanDestinationServiceName,
			elasticattr.SpanDestinationServiceType,
			elasticattr.SpanDestinationServiceResource,
			// miscellaneous
			elasticattr.EventOutcome,
			elasticattr.ProcessorEvent,
			elasticattr.SessionID,
			elasticattr.TransactionType,
			"type",
			"code.stacktrace",
			// db.*
			"sql.query",
			"db.type",
			"db.instance",
			"db.elasticsearch.cluster.name",
			string(semconv25.DBNameKey),
			string(semconv37.DBNamespaceKey),
			string(semconv37.DBQueryTextKey),
			string(semconv25.DBStatementKey),
			string(semconv25.DBSystemKey),
			string(semconv37.DBSystemNameKey),
			string(semconv25.DBUserKey),
			// gen_ai.*
			string(semconv37.GenAIProviderNameKey),
			string(semconv.GenAISystemKey),
			// http.*
			string(semconv25.HTTPFlavorKey),
			string(semconv25.HTTPMethodKey),
			string(semconv25.HTTPRequestMethodKey),
			string(semconv.HTTPResponseBodySizeKey),
			string(semconv25.HTTPResponseStatusCodeKey),
			string(semconv25.HTTPSchemeKey),
			string(semconv25.HTTPStatusCodeKey),
			string(semconv25.HTTPTargetKey),
			string(semconv12.HTTPHostKey),
			string(semconv25.HTTPURLKey),
			string(semconv25.HTTPUserAgentKey),
			// messaging.*
			"message_bus.destination",
			string(semconv25.MessagingDestinationNameKey),
			string(semconv25.MessagingDestinationTemporaryKey),
			string(semconv25.MessagingOperationKey),
			string(semconv37.MessagingOperationNameKey),
			string(semconv25.MessagingSystemKey),
			string(semconv26.MessagingOperationTypeKey),
			string(semconv16.MessagingTempDestinationKey),
			string(semconv16.MessagingDestinationKey),
			// net.*
			string(semconv25.NetHostNameKey),
			string(semconv25.NetPeerNameKey),
			string(semconv25.NetPeerPortKey),
			string(semconv12.NetPeerIPKey),
			string(semconv25.NetSockPeerAddrKey),
			string(semconv.NetworkPeerAddressKey),
			// peer.*
			"peer.address",
			"peer.hostname",
			"peer.ipv4",
			"peer.ipv6",
			"peer.port",
			// network.*
			string(semconv.NetworkCarrierIccKey),
			string(semconv.NetworkCarrierMccKey),
			string(semconv.NetworkCarrierMncKey),
			string(semconv.NetworkCarrierNameKey),
			string(semconv.NetworkConnectionSubtypeKey),
			string(semconv.NetworkConnectionTypeKey),
			// rpc.*
			string(semconv25.PeerServiceKey),
			string(semconv25.RPCGRPCStatusCodeKey),
			string(semconv39.RPCMethodKey),
			string(semconv39.RPCResponseStatusCodeKey),
			string(semconv25.RPCServiceKey),
			string(semconv25.RPCSystemKey),
			string(semconv39.RPCSystemNameKey),
			// server.*
			string(semconv25.ServerAddressKey),
			string(semconv25.ServerPortKey),
			// service.*
			string(semconv39.ServicePeerNameKey),
			// url.*
			string(semconv25.URLDomainKey),
			string(semconv25.URLFullKey),
			string(semconv25.URLPathKey),
			string(semconv25.URLPortKey),
			string(semconv25.URLQueryKey),
			string(semconv25.URLSchemeKey),
			// user_agent.*
			string(semconv.UserAgentNameKey),
			string(semconv.UserAgentOriginalKey),
			string(semconv.UserAgentVersionKey):
			return false
		default:
			if label := getLabelAttributeValue(k, v); label.k != "" {
				toAppend = append(toAppend, label)
			}
			return true
		}
	})
	for _, l := range toAppend {
		l.v.CopyTo(attributes.PutEmpty(l.k))
	}
}

// RemapMetricDataPointAttributesToECSLabels applies the apm-data OTLP metric fallback
// for raw metric datapoint attributes in ECS mode. Metric-specific special cases
// are preserved, and everything else is moved to labels.* / numeric_labels.*.
func RemapMetricDataPointAttributesToECSLabels(attributes pcommon.Map) {
	var toAppend []kv
	attributes.RemoveIf(func(k string, v pcommon.Value) bool {
		switch k {
		case elasticattr.DataStreamDataset,
			elasticattr.DataStreamNamespace,
			elasticattr.DataStreamType,
			elasticattr.EventDataset,
			"event.module",
			"system.process.cpu.start_time",
			"system.process.state":
			return false
		case string(semconv.UserNameKey),
			"system.filesystem.mount_point",
			"system.process.cmdline":
			truncatePreservedStringAttribute(attributes, k, v)
			return false
		default:
			if label := getLabelAttributeValue(k, v); label.k != "" {
				toAppend = append(toAppend, label)
			}
			return true
		}
	})
	for _, l := range toAppend {
		l.v.CopyTo(attributes.PutEmpty(l.k))
	}
}

func truncatePreservedStringAttribute(attributes pcommon.Map, key string, value pcommon.Value) {
	if value.Type() != pcommon.ValueTypeStr {
		return
	}
	truncated := strutil.Truncate(value.Str())
	if truncated != value.Str() {
		attributes.PutStr(key, truncated)
	}
}

func getLabelAttributeValue(key string, value pcommon.Value) kv {
	sanitizedKey := strings.Map(replaceReservedLabelKeyRune, key)
	switch value.Type() {
	case pcommon.ValueTypeStr:
		return kv{k: "labels." + sanitizedKey, v: pcommon.NewValueStr(strutil.Truncate(value.Str()))}
	case pcommon.ValueTypeBool:
		return kv{k: "labels." + sanitizedKey, v: pcommon.NewValueStr(strconv.FormatBool(value.Bool()))}
	case pcommon.ValueTypeInt:
		return kv{k: "numeric_labels." + sanitizedKey, v: pcommon.NewValueDouble(float64(value.Int()))}
	case pcommon.ValueTypeDouble:
		return kv{k: "numeric_labels." + sanitizedKey, v: pcommon.NewValueDouble(value.Double())}
	case pcommon.ValueTypeSlice:
		slice := value.Slice()
		if slice.Len() == 0 {
			return kv{}
		}
		switch slice.At(0).Type() {
		// TODO(lahsivjar): Can we assume all are same type and just use pcommon.Value#CopyTo?
		case pcommon.ValueTypeStr:
			lv := pcommon.NewValueEmpty()
			sl := lv.SetEmptySlice()
			for i := 0; i < slice.Len(); i++ {
				item := slice.At(i)
				if item.Type() == pcommon.ValueTypeStr {
					sl.AppendEmpty().SetStr(strutil.Truncate(item.Str()))
				}
			}
			return kv{k: "labels." + sanitizedKey, v: lv}
		case pcommon.ValueTypeBool:
			lv := pcommon.NewValueEmpty()
			sl := lv.SetEmptySlice()
			for i := 0; i < slice.Len(); i++ {
				item := slice.At(i)
				if item.Type() == pcommon.ValueTypeBool {
					sl.AppendEmpty().SetStr(strconv.FormatBool(item.Bool()))
				}
			}
			return kv{k: "labels." + sanitizedKey, v: lv}
		case pcommon.ValueTypeDouble:
			lv := pcommon.NewValueEmpty()
			sl := lv.SetEmptySlice()
			for i := 0; i < slice.Len(); i++ {
				item := slice.At(i)
				if item.Type() == pcommon.ValueTypeDouble {
					sl.AppendEmpty().SetDouble(item.Double())
				}
			}
			return kv{k: "numeric_labels." + sanitizedKey, v: lv}
		case pcommon.ValueTypeInt:
			lv := pcommon.NewValueEmpty()
			sl := lv.SetEmptySlice()
			for i := 0; i < slice.Len(); i++ {
				item := slice.At(i)
				if item.Type() == pcommon.ValueTypeInt {
					sl.AppendEmpty().SetDouble(float64(item.Int()))
				}
			}
			return kv{k: "numeric_labels." + sanitizedKey, v: lv}
		}
	case pcommon.ValueTypeMap, pcommon.ValueTypeBytes, pcommon.ValueTypeEmpty:
		// ES label mappings only support flat scalars and homogeneous arrays;
		// apm-data's setLabel also silently drops these types.
	}
	return kv{}
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

// sanitizeExistingLabelKey sanitizes reserved characters from the suffix of a
// labels.* or numeric_labels.* key. prefix must include the trailing dot
// (e.g. "labels." or "numeric_labels."). Returns the sanitized key and
// whether it changed. If k does not start with prefix, k is returned unchanged.
func sanitizeExistingLabelKey(k, prefix string) (string, bool) {
	if !strings.HasPrefix(k, prefix) {
		return k, false
	}
	suffix := k[len(prefix):]
	sanitized := strings.Map(replaceReservedLabelKeyRune, suffix)
	if sanitized == suffix {
		return k, false
	}
	return prefix + sanitized, true
}

func replaceReservedLabelKeyRune(r rune) rune {
	switch r {
	case '.', '*', '"':
		return '_'
	}
	return r
}
