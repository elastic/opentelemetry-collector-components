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

// This file contains all the mapping from IntakeV2 fields to OTel Semantic Convention

package mappers // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/mappers"

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv22 "go.opentelemetry.io/otel/semconv/v1.22.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/elastic/apm-data/model/modelpb"
)

// TranslateToOtelResourceAttributes translates resource attributes from the Elastic APM model to SemConv resource attributes
func TranslateToOtelResourceAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Service != nil {
		putNonEmptyStr(attributes, string(semconv.ServiceNameKey), event.Service.Name)
		putNonEmptyStr(attributes, string(semconv.ServiceVersionKey), event.Service.Version)
		if event.Service.Language != nil && event.Service.Language.Name != "" {
			attributes.PutStr(string(semconv.TelemetrySDKLanguageKey), translateElasticServiceLanguageToOtelSdkLanguage(event.Service.Language.Name))
		}
		attributes.PutStr(string(semconv.TelemetrySDKNameKey), "ElasticAPM")
		if event.Service.Environment != "" {
			// elasticsearchexporter currently uses v1.22.0 of the OTel SemConv, so we need to include the v1.22.0 attribute
			attributes.PutStr(string(semconv22.DeploymentEnvironmentKey), event.Service.Environment)
			attributes.PutStr(string(semconv.DeploymentEnvironmentNameKey), event.Service.Environment)
		}
		if event.Service.Node != nil {
			putNonEmptyStr(attributes, string(semconv.ServiceInstanceIDKey), event.Service.Node.Name)
		}
	}
	if event.Host != nil {
		putNonEmptyStr(attributes, string(semconv.HostNameKey), event.Host.Name)
		putNonEmptyStr(attributes, string(semconv.HostIDKey), event.Host.Id)
		putNonEmptyStr(attributes, string(semconv.HostArchKey), event.Host.Architecture)
		if event.Host.Os != nil {
			putNonEmptyStr(attributes, string(semconv.OSNameKey), event.Host.Os.Name)
			putNonEmptyStr(attributes, string(semconv.OSVersionKey), event.Host.Os.Version)
		}
	}

	// UserAgent fields are only expected to be available for error and transaction events.
	// Translating here since fields should be present at the resource level.
	// https://opentelemetry.io/docs/specs/semconv/registry/attributes/user-agent
	if event.UserAgent != nil {
		putNonEmptyStr(attributes, string(semconv.UserAgentOriginalKey), event.UserAgent.Original)
	}

	translateCloudAttributes(event, attributes)
	translateContainerAndKubernetesAttributes(event, attributes)
	translateProcessUserNetworkAttributes(event, attributes)
	translateFaasAttributes(event, attributes)
}

// SemConv defines a well known list of values of telemetry.sdk.language: https://opentelemetry.io/docs/specs/semconv/attributes-registry/telemetry/
// The classic Elastic APM Agents report values that may not be in the SemConv well known list.
// This method maps those values to the closest SemConv well known value.
func translateElasticServiceLanguageToOtelSdkLanguage(language string) string {
	language_lower_case := strings.ToLower(language)
	switch language_lower_case {
	case "c#":
		return "dotnet"
	default:
		return language_lower_case
	}
}

// TranslateIntakeV2TransactionToOTelAttributes translates transaction attributes from the Elastic APM model to SemConv attributes
func TranslateIntakeV2TransactionToOTelAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	translateHttpAttributes(event, attributes)
	translateUrlAttributes(event, attributes)

	if event.Transaction.Message != nil {
		attributes.PutStr(string(semconv.MessagingDestinationNameKey), event.Transaction.Message.QueueName)
	}
}

// TranslateIntakeV2SpanToOTelAttributes translates span attributes from the Elastic APM model to SemConv attributes
func TranslateIntakeV2SpanToOTelAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	translateHttpAttributes(event, attributes)
	translateUrlAttributes(event, attributes)

	if event.Span == nil {
		return
	}

	if event.Span.Db != nil {
		attributes.PutStr(string(semconv.DBSystemKey), event.Span.Db.Type)
		attributes.PutStr(string(semconv.DBNamespaceKey), event.Span.Db.Instance)
		attributes.PutStr(string(semconv.DBQueryTextKey), event.Span.Db.Statement)
	}
	if event.Span.Message != nil {
		// Elastic APM span.subtype does not 100% overlap with https://opentelemetry.io/docs/specs/semconv/attributes-registry/messaging/#messaging-system
		// E.g. azureservicebus in Elastic APM vs servicebus in SemConv
		attributes.PutStr(string(semconv.MessagingSystemKey), event.Span.Subtype)
		// No 100% overlap either
		attributes.PutStr(string(semconv.MessagingOperationNameKey), event.Span.Action)

		if event.Span.Message.QueueName != "" {
			attributes.PutStr(string(semconv.MessagingDestinationNameKey), event.Span.Message.QueueName)
		}
	}

	if event.Destination != nil {
		if event.Destination.Address != "" {
			attributes.PutStr(string(semconv.DestinationAddressKey), event.Destination.Address)
		}
		if event.Destination.Port != 0 {
			attributes.PutInt(string(semconv.DestinationPortKey), int64(event.Destination.Port))
		}
	}
}

// TranslateIntakeV2LogToOTelAttributes translates log/error attributes from the Elastic APM model to SemConv attributes
// Note: error events contain additional context that requires otel semconv attributes, logs are not expected to have
// this additional context. Both events are treated the same here for consistency.
func TranslateIntakeV2LogToOTelAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	translateHttpAttributes(event, attributes)
	translateUrlAttributes(event, attributes)
}

func translateCloudAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Cloud != nil {
		putNonEmptyStr(attributes, string(semconv.CloudProviderKey), event.Cloud.Provider)
		putNonEmptyStr(attributes, string(semconv.CloudRegionKey), event.Cloud.Region)
		putNonEmptyStr(attributes, string(semconv.CloudAvailabilityZoneKey), event.Cloud.AvailabilityZone)
		putNonEmptyStr(attributes, string(semconv.CloudAccountIDKey), event.Cloud.AccountId)
		putNonEmptyStr(attributes, string(semconv.CloudPlatformKey), event.Cloud.ServiceName)
	}
}

func translateContainerAndKubernetesAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	// Container fields
	if event.Container != nil {
		putNonEmptyStr(attributes, string(semconv.ContainerIDKey), event.Container.Id)
		putNonEmptyStr(attributes, string(semconv.ContainerNameKey), event.Container.Name)
		putNonEmptyStr(attributes, string(semconv.ContainerRuntimeKey), event.Container.Runtime)
		putNonEmptyStr(attributes, string(semconv.ContainerImageNameKey), event.Container.ImageName)
		putNonEmptyStr(attributes, string(semconv.ContainerImageTagsKey), event.Container.ImageTag)
	}

	// Kubernetes fields
	if event.Kubernetes != nil {
		putNonEmptyStr(attributes, string(semconv.K8SNamespaceNameKey), event.Kubernetes.Namespace)
		putNonEmptyStr(attributes, string(semconv.K8SNodeNameKey), event.Kubernetes.NodeName)
		putNonEmptyStr(attributes, string(semconv.K8SPodNameKey), event.Kubernetes.PodName)
		putNonEmptyStr(attributes, string(semconv.K8SPodUIDKey), event.Kubernetes.PodUid)
	}
}

func translateProcessUserNetworkAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	// Process fields
	if event.Process != nil {
		if event.Process.Pid != 0 {
			attributes.PutInt(string(semconv.ProcessPIDKey), int64(event.Process.Pid))
		}
		if event.Process.Ppid != 0 {
			attributes.PutInt(string(semconv.ProcessParentPIDKey), int64(event.Process.Ppid))
		}
		putNonEmptyStr(attributes, string(semconv.ProcessExecutableNameKey), event.Process.Title)
		if len(event.Process.Argv) > 0 {
			commandLineArgs := attributes.PutEmptySlice(string(semconv.ProcessCommandLineKey))
			commandLineArgs.EnsureCapacity(len(event.Process.Argv))
			for _, arg := range event.Process.Argv {
				commandLineArgs.AppendEmpty().SetStr(arg)
			}
		}
		putNonEmptyStr(attributes, string(semconv.ProcessExecutablePathKey), event.Process.Executable)
	}

	// translate user fields defined here: https://opentelemetry.io/docs/specs/semconv/registry/attributes/user
	if event.User != nil {
		putNonEmptyStr(attributes, string(semconv.UserIDKey), event.User.Id)
		putNonEmptyStr(attributes, string(semconv.UserEmailKey), event.User.Email)
		putNonEmptyStr(attributes, string(semconv.UserNameKey), event.User.Name)
	}

	// translate network fields defined here: https://opentelemetry.io/docs/specs/semconv/registry/attributes/network
	if event.Network != nil {
		if event.Network.Connection != nil {
			putNonEmptyStr(attributes, string(semconv.NetworkConnectionTypeKey), event.Network.Connection.Type)
			putNonEmptyStr(attributes, string(semconv.NetworkConnectionSubtypeKey), event.Network.Connection.Subtype)
		}
		if event.Network.Carrier != nil {
			putNonEmptyStr(attributes, string(semconv.NetworkCarrierNameKey), event.Network.Carrier.Name)
			putNonEmptyStr(attributes, string(semconv.NetworkCarrierMccKey), event.Network.Carrier.Mcc)
			putNonEmptyStr(attributes, string(semconv.NetworkCarrierMncKey), event.Network.Carrier.Mnc)
			putNonEmptyStr(attributes, string(semconv.NetworkCarrierIccKey), event.Network.Carrier.Icc)
		}
	}

	if event.Client != nil {
		translateIPAddress(string(semconv.ClientAddressKey), event.Client.Ip, attributes)
		if event.Client.Port != 0 {
			attributes.PutInt(string(semconv.ClientPortKey), int64(event.Client.Port))
		}
	}

	if event.Source != nil {
		translateIPAddress(string(semconv.SourceAddressKey), event.Source.Ip, attributes)
		if event.Source.Port != 0 {
			attributes.PutInt(string(semconv.SourcePortKey), int64(event.Source.Port))
		}
	}
}

func translateIPAddress(key string, ip *modelpb.IP, attributes pcommon.Map) {
	if ip != nil {
		ipAddr := modelpb.IP2Addr(ip)
		putNonEmptyStr(attributes, key, ipAddr.String())
	}
}

func translateFaasAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Faas != nil {
		putNonEmptyStr(attributes, string(semconv.FaaSInstanceKey), event.Faas.Id)
		putNonEmptyStr(attributes, string(semconv.FaaSNameKey), event.Faas.Name)
		putNonEmptyStr(attributes, string(semconv.FaaSVersionKey), event.Faas.Version)
		putNonEmptyStr(attributes, string(semconv.FaaSTriggerKey), event.Faas.TriggerType)
		putPtrBool(attributes, string(semconv.FaaSColdstartKey), event.Faas.ColdStart)
	}
}

func translateHttpAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Http != nil {
		if event.Http.Request != nil {
			attributes.PutStr(string(semconv.HTTPRequestMethodKey), event.Http.Request.Method)
		}
		if event.Http.Response != nil {
			if event.Http.Response.StatusCode != 0 {
				attributes.PutInt(string(semconv.HTTPResponseStatusCodeKey), int64(event.Http.Response.StatusCode))
			}
			putPtrInt(attributes, string(semconv.HTTPResponseBodySizeKey), event.Http.Response.EncodedBodySize)
		}
	}
}

// translateUrlAttributes sets URL semconv attributes that are defined below:
// https://opentelemetry.io/docs/specs/semconv/registry/attributes/url/
func translateUrlAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Url == nil {
		return
	}
	putNonEmptyStr(attributes, string(semconv.URLOriginalKey), event.Url.Original)
	putNonEmptyStr(attributes, string(semconv.URLSchemeKey), event.Url.Scheme)
	putNonEmptyStr(attributes, string(semconv.URLFullKey), event.Url.Full)
	putNonEmptyStr(attributes, string(semconv.URLDomainKey), event.Url.Domain)
	putNonEmptyStr(attributes, string(semconv.URLPathKey), event.Url.Path)
	putNonEmptyStr(attributes, string(semconv.URLQueryKey), event.Url.Query)
	putNonEmptyStr(attributes, string(semconv.URLFragmentKey), event.Url.Fragment)
	if event.Url.Port != 0 {
		attributes.PutInt(string(semconv.URLPortKey), int64(event.Url.Port))
	}
}
