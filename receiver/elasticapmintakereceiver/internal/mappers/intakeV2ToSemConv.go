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
		attributes.PutStr(string(semconv.ServiceNameKey), event.Service.Name)
		attributes.PutStr(string(semconv.ServiceVersionKey), event.Service.Version)
		if event.Service.Language != nil && event.Service.Language.Name != "" {
			attributes.PutStr(string(semconv.TelemetrySDKLanguageKey), translateElasticServiceLanguageToOtelSdkLanguage(event.Service.Language.Name))
		}
		attributes.PutStr(string(semconv.TelemetrySDKNameKey), "ElasticAPM")
		if event.Service.Environment != "" {
			// elasticsearchexporter currently uses v1.22.0 of the OTel SemConv, so we need to include the v1.22.0 attribute
			attributes.PutStr(string(semconv22.DeploymentEnvironmentKey), event.Service.Environment)
			attributes.PutStr(string(semconv.DeploymentEnvironmentNameKey), event.Service.Environment)
		}
		if event.Service.Node != nil && event.Service.Node.Name != "" {
			attributes.PutStr(string(semconv.ServiceInstanceIDKey), event.Service.Node.Name)
		}
	}
	if event.Host != nil {
		if event.Host.Name != "" {
			attributes.PutStr(string(semconv.HostNameKey), event.Host.Name)
		}
		if event.Host.Id != "" {
			attributes.PutStr(string(semconv.HostIDKey), event.Host.Id)
		}
		if event.Host.Architecture != "" {
			attributes.PutStr(string(semconv.HostArchKey), event.Host.Architecture)
		}
		if event.Host.Os != nil && event.Host.Os.Name != "" {
			attributes.PutStr(string(semconv.OSNameKey), event.Host.Os.Name)
			if event.Host.Os.Version != "" {
				attributes.PutStr(string(semconv.OSVersionKey), event.Host.Os.Version)
			}
		}
	}

	// UserAgent fields are only expected to be available for error and transaction events.
	// Translating here since fields should be present at the resource level.
	// https://opentelemetry.io/docs/specs/semconv/registry/attributes/user-agent
	if event.UserAgent != nil {
		if event.UserAgent.Original != "" {
			attributes.PutStr(string(semconv.UserAgentOriginalKey), event.UserAgent.Original)
		}
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
		if event.Cloud.Provider != "" {
			attributes.PutStr(string(semconv.CloudProviderKey), event.Cloud.Provider)
		}
		if event.Cloud.Region != "" {
			attributes.PutStr(string(semconv.CloudRegionKey), event.Cloud.Region)
		}
		if event.Cloud.AvailabilityZone != "" {
			attributes.PutStr(string(semconv.CloudAvailabilityZoneKey), event.Cloud.AvailabilityZone)
		}
		if event.Cloud.AccountId != "" {
			attributes.PutStr(string(semconv.CloudAccountIDKey), event.Cloud.AccountId)
		}
		if event.Cloud.ServiceName != "" {
			attributes.PutStr(string(semconv.CloudPlatformKey), event.Cloud.ServiceName)
		}
	}
}

func translateContainerAndKubernetesAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	// Container fields
	if event.Container != nil {
		if event.Container.Id != "" {
			attributes.PutStr(string(semconv.ContainerIDKey), event.Container.Id)
		}
		if event.Container.Name != "" {
			attributes.PutStr(string(semconv.ContainerNameKey), event.Container.Name)
		}
		if event.Container.Runtime != "" {
			attributes.PutStr(string(semconv.ContainerRuntimeKey), event.Container.Runtime)
		}
		if event.Container.ImageName != "" {
			attributes.PutStr(string(semconv.ContainerImageNameKey), event.Container.ImageName)
		}
		if event.Container.ImageTag != "" {
			attributes.PutStr(string(semconv.ContainerImageTagsKey), event.Container.ImageTag)
		}
	}

	// Kubernetes fields
	if event.Kubernetes != nil {
		if event.Kubernetes.Namespace != "" {
			attributes.PutStr(string(semconv.K8SNamespaceNameKey), event.Kubernetes.Namespace)
		}
		if event.Kubernetes.NodeName != "" {
			attributes.PutStr(string(semconv.K8SNodeNameKey), event.Kubernetes.NodeName)
		}
		if event.Kubernetes.PodName != "" {
			attributes.PutStr(string(semconv.K8SPodNameKey), event.Kubernetes.PodName)
		}
		if event.Kubernetes.PodUid != "" {
			attributes.PutStr(string(semconv.K8SPodUIDKey), event.Kubernetes.PodUid)
		}
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
		if event.Process.Title != "" {
			// Title is the process title. It can be the same as process name.
			attributes.PutStr(string(semconv.ProcessExecutableNameKey), event.Process.Title)
		}
		if len(event.Process.Argv) > 0 {
			commandLineArgs := attributes.PutEmptySlice(string(semconv.ProcessCommandLineKey))
			commandLineArgs.EnsureCapacity(len(event.Process.Argv))
			for _, arg := range event.Process.Argv {
				commandLineArgs.AppendEmpty().SetStr(arg)
			}
		}
		if event.Process.Executable != "" {
			attributes.PutStr(string(semconv.ProcessExecutablePathKey), event.Process.Executable)
		}
	}

	// translate user fields defined here: https://opentelemetry.io/docs/specs/semconv/registry/attributes/user
	if event.User != nil {
		if event.User.Id != "" {
			attributes.PutStr(string(semconv.UserIDKey), event.User.Id)
		}
		if event.User.Email != "" {
			attributes.PutStr(string(semconv.UserEmailKey), event.User.Email)
		}
		if event.User.Name != "" {
			attributes.PutStr(string(semconv.UserNameKey), event.User.Name)
		}
	}

	// translate network fields defined here: https://opentelemetry.io/docs/specs/semconv/registry/attributes/network
	if event.Network != nil {
		if event.Network.Connection != nil {
			if event.Network.Connection.Type != "" {
				attributes.PutStr(string(semconv.NetworkConnectionTypeKey), event.Network.Connection.Type)
			}
			if event.Network.Connection.Subtype != "" {
				attributes.PutStr(string(semconv.NetworkConnectionSubtypeKey), event.Network.Connection.Subtype)
			}
		}
		if event.Network.Carrier != nil {
			if event.Network.Carrier.Name != "" {
				attributes.PutStr(string(semconv.NetworkCarrierNameKey), event.Network.Carrier.Name)
			}
			if event.Network.Carrier.Mcc != "" {
				attributes.PutStr(string(semconv.NetworkCarrierMccKey), event.Network.Carrier.Mcc)
			}
			if event.Network.Carrier.Mnc != "" {
				attributes.PutStr(string(semconv.NetworkCarrierMncKey), event.Network.Carrier.Mnc)
			}
			if event.Network.Carrier.Icc != "" {
				attributes.PutStr(string(semconv.NetworkCarrierIccKey), event.Network.Carrier.Icc)
			}
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
		if ipAddr.String() != "" {
			attributes.PutStr(key, ipAddr.String())
		}
	}
}

func translateFaasAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Faas != nil {
		if event.Faas.Id != "" {
			attributes.PutStr(string(semconv.FaaSInstanceKey), event.Faas.Id)
		}
		if event.Faas.Name != "" {
			attributes.PutStr(string(semconv.FaaSNameKey), event.Faas.Name)
		}
		if event.Faas.Version != "" {
			attributes.PutStr(string(semconv.FaaSVersionKey), event.Faas.Version)
		}
		if event.Faas.TriggerType != "" {
			attributes.PutStr(string(semconv.FaaSTriggerKey), event.Faas.TriggerType)
		}
		if event.Faas.ColdStart != nil {
			attributes.PutBool(string(semconv.FaaSColdstartKey), *event.Faas.ColdStart)
		}
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
			if event.Http.Response.EncodedBodySize != nil {
				attributes.PutInt(string(semconv.HTTPResponseBodySizeKey), int64(*event.Http.Response.EncodedBodySize))
			}
		}
	}
}

// translateUrlAttributes sets URL semconv attributes that are defined below:
// https://opentelemetry.io/docs/specs/semconv/registry/attributes/url/
func translateUrlAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Url == nil {
		return
	}
	if event.Url.Original != "" {
		attributes.PutStr(string(semconv.URLOriginalKey), event.Url.Original)
	}
	if event.Url.Scheme != "" {
		attributes.PutStr(string(semconv.URLSchemeKey), event.Url.Scheme)
	}
	if event.Url.Full != "" {
		attributes.PutStr(string(semconv.URLFullKey), event.Url.Full)
	}
	if event.Url.Domain != "" {
		attributes.PutStr(string(semconv.URLDomainKey), event.Url.Domain)
	}
	if event.Url.Path != "" {
		attributes.PutStr(string(semconv.URLPathKey), event.Url.Path)
	}
	if event.Url.Query != "" {
		attributes.PutStr(string(semconv.URLQueryKey), event.Url.Query)
	}
	if event.Url.Fragment != "" {
		attributes.PutStr(string(semconv.URLFragmentKey), event.Url.Fragment)
	}
	if event.Url.Port != 0 {
		attributes.PutInt(string(semconv.URLPortKey), int64(event.Url.Port))
	}
}
