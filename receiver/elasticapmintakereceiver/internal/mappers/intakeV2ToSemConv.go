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

	"github.com/elastic/apm-data/model/modelpb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv22 "go.opentelemetry.io/collector/semconv/v1.22.0"
	semconv "go.opentelemetry.io/collector/semconv/v1.27.0"
)

// Translates resource attributes from the Elastic APM model to SemConv resource attributes
func TranslateToOtelResourceAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutStr(semconv.AttributeServiceName, event.Service.Name)
	attributes.PutStr(semconv.AttributeServiceVersion, event.Service.Version)
	if event.Service.Language != nil && event.Service.Language.Name != "" {
		attributes.PutStr(semconv.AttributeTelemetrySDKLanguage, translateElasticServiceLanguageToOtelSdkLanguage(event.Service.Language.Name))
	}
	attributes.PutStr(semconv.AttributeTelemetrySDKName, "ElasticAPM")
	if event.Service.Environment != "" {
		// elasticsearchexporter currently uses v1.22.0 of the OTel SemConv, so we need to include the v1.22.0 attribute
		attributes.PutStr(semconv22.AttributeDeploymentEnvironment, event.Service.Environment)
		attributes.PutStr(semconv.AttributeDeploymentEnvironmentName, event.Service.Environment)
	}
	if event.Service.Node != nil && event.Service.Node.Name != "" {
		attributes.PutStr(semconv.AttributeServiceInstanceID, event.Service.Node.Name)
	}
	if event.Host != nil {
		if event.Host.Name != "" {
			attributes.PutStr(semconv.AttributeHostName, event.Host.Name)
		}
		if event.Host.Id != "" {
			attributes.PutStr(semconv.AttributeHostID, event.Host.Id)
		}
		if event.Host.Architecture != "" {
			attributes.PutStr(semconv.AttributeHostArch, event.Host.Architecture)
		}
		if event.Host.Os.Name != "" {
			attributes.PutStr(semconv.AttributeOSName, event.Host.Os.Name)
			if event.Host.Os.Version != "" {
				attributes.PutStr(semconv.AttributeOSVersion, event.Host.Os.Version)
			}
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

// Translates transaction attributes from the Elastic APM model to SemConv attributes
func TranslateIntakeV2TransactionToOTelAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	setHttpAttributes(event, attributes)

	if event.Span.Message != nil {
		attributes.PutStr(semconv.AttributeMessagingDestinationName, event.Transaction.Message.QueueName)
		attributes.PutStr(semconv.AttributeMessagingRabbitmqDestinationRoutingKey, event.Transaction.Message.RoutingKey)

		// This may need to be unified, see AttributeMessagingSystem for spans
		attributes.PutStr(semconv.AttributeMessagingSystem, event.Service.Framework.Name)
	}
}

// Translates span attributes from the Elastic APM model to SemConv attributes
func TranslateIntakeV2SpanToOTelAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Http != nil {

		setHttpAttributes(event, attributes)

		if event.Url != nil && event.Url.Full != "" {
			attributes.PutStr(semconv.AttributeURLFull, event.Url.Full)
		}
	}
	if event.Span.Db != nil {
		attributes.PutStr(semconv.AttributeDBSystem, event.Span.Db.Type)
		attributes.PutStr(semconv.AttributeDBNamespace, event.Span.Db.Instance)
		attributes.PutStr(semconv.AttributeDBQueryText, event.Span.Db.Statement)
	}
	if event.Span.Message != nil {
		// Elastic APM span.subtype does not 100% overlap with https://opentelemetry.io/docs/specs/semconv/attributes-registry/messaging/#messaging-system
		// E.g. azureservicebus in Elastic APM vs servicebus in SemConv
		attributes.PutStr(semconv.AttributeMessagingSystem, event.Span.Subtype)
		// No 100% overlap either
		attributes.PutStr(semconv.AttributeMessagingOperationName, event.Span.Action)

		if event.Span.Message.QueueName != "" {
			attributes.PutStr(semconv.AttributeMessagingDestinationName, event.Span.Message.QueueName)
		}
		if event.Span.Message.RoutingKey != "" {
			attributes.PutStr(semconv.AttributeMessagingRabbitmqDestinationRoutingKey, event.Span.Message.RoutingKey)
		}
	}
}

func translateCloudAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Cloud != nil {
		if event.Cloud.Provider != "" {
			attributes.PutStr(semconv.AttributeCloudProvider, event.Cloud.Provider)
		}
		if event.Cloud.Region != "" {
			attributes.PutStr(semconv.AttributeCloudRegion, event.Cloud.Region)
		}
		if event.Cloud.AvailabilityZone != "" {
			attributes.PutStr(semconv.AttributeCloudAvailabilityZone, event.Cloud.AvailabilityZone)
		}
		if event.Cloud.AccountId != "" {
			attributes.PutStr(semconv.AttributeCloudAccountID, event.Cloud.AccountId)
		}
		if event.Cloud.ServiceName != "" {
			attributes.PutStr(semconv.AttributeCloudPlatform, event.Cloud.ServiceName)
		}
	}
}

func translateContainerAndKubernetesAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	// Container fields
	if event.Container != nil {
		if event.Container.Id != "" {
			attributes.PutStr(semconv.AttributeContainerID, event.Container.Id)
		}
		if event.Container.Name != "" {
			attributes.PutStr(semconv.AttributeContainerName, event.Container.Name)
		}
		if event.Container.Runtime != "" {
			attributes.PutStr(semconv.AttributeContainerRuntime, event.Container.Runtime)
		}
		if event.Container.ImageName != "" {
			attributes.PutStr(semconv.AttributeContainerImageName, event.Container.ImageName)
		}
		if event.Container.ImageTag != "" {
			attributes.PutStr(semconv.AttributeContainerImageTags, event.Container.ImageTag)
		}
	}

	// Kubernetes fields
	if event.Kubernetes != nil {
		if event.Kubernetes.Namespace != "" {
			attributes.PutStr(semconv.AttributeK8SNamespaceName, event.Kubernetes.Namespace)
		}
		if event.Kubernetes.NodeName != "" {
			attributes.PutStr(semconv.AttributeK8SNodeName, event.Kubernetes.NodeName)
		}
		if event.Kubernetes.PodName != "" {
			attributes.PutStr(semconv.AttributeK8SPodName, event.Kubernetes.PodName)
		}
		if event.Kubernetes.PodUid != "" {
			attributes.PutStr(semconv.AttributeK8SPodUID, event.Kubernetes.PodUid)
		}
	}
}

func translateProcessUserNetworkAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	// Process fields
	if event.Process != nil {
		if event.Process.Pid != 0 {
			attributes.PutInt(semconv.AttributeProcessPID, int64(event.Process.Pid))
		}
		if event.Process.Ppid != 0 {
			attributes.PutInt(semconv.AttributeProcessParentPID, int64(event.Process.Ppid))
		}
		if event.Process.Title != "" {
			// Title is the process title. It can be the same as process name.
			attributes.PutStr(semconv.AttributeProcessExecutableName, event.Process.Title)
		}
		if len(event.Process.Argv) > 0 {
			attributes.PutStr(semconv.AttributeProcessCommandLine, strings.Join(event.Process.Argv, " "))
		}
		if event.Process.Executable != "" {
			attributes.PutStr(semconv.AttributeProcessExecutablePath, event.Process.Executable)
		}
	}

	// User fields
	if event.User != nil {
		if event.User.Id != "" {
			attributes.PutStr(semconv.AttributeUserID, event.User.Id)
		}
		if event.User.Email != "" {
			attributes.PutStr(semconv.AttributeUserEmail, event.User.Email)
		}
	}

	if event.Network != nil && event.Network.Connection != nil && event.Network.Connection.Type != "" {
		attributes.PutStr(semconv.AttributeNetworkConnectionType, event.Network.Connection.Type)
	}

	if event.Client != nil && event.Client.Ip != nil && event.Client.Ip.String() != "" {
		attributes.PutStr(semconv.AttributeClientAddress, event.Client.Ip.String())
	}
}

func translateFaasAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Faas != nil {
		if event.Faas.Id != "" {
			attributes.PutStr(semconv.AttributeFaaSInstance, event.Faas.Id)
		}
		if event.Faas.Name != "" {
			attributes.PutStr(semconv.AttributeFaaSName, event.Faas.Name)
		}
		if event.Faas.Version != "" {
			attributes.PutStr(semconv.AttributeFaaSVersion, event.Faas.Version)
		}
		if event.Faas.TriggerType != "" {
			attributes.PutStr(semconv.AttributeFaaSTrigger, event.Faas.TriggerType)
		}
		if event.Faas.ColdStart != nil {
			attributes.PutBool(semconv.AttributeFaaSColdstart, *event.Faas.ColdStart)
		}
	}
}

func setHttpAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Http != nil {
		if event.Http.Request != nil {
			attributes.PutStr(semconv.AttributeHTTPRequestMethod, event.Http.Request.Method)
			if event.Url != nil && event.Url.Full != "" {
				attributes.PutStr(semconv.AttributeURLFull, event.Url.Full)
			}
		}
		if event.Http.Response != nil {
			attributes.PutInt(semconv.AttributeHTTPResponseStatusCode, int64(event.Http.Response.StatusCode))
		}
	}
}
