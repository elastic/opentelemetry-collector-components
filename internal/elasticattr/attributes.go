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

package elasticattr

const (
	// resource s
	AgentName              = "agent.name"
	AgentVersion           = "agent.version"
	AgentEphemeralID       = "agent.ephemeral_id"
	AgentActivationMethod  = "agent.activation_method"
	CloudOriginAccountID   = "cloud.origin.account.id"
	CloudOriginProvider    = "cloud.origin.provider"
	CloudOriginRegion      = "cloud.origin.region"
	CloudOriginServiceName = "cloud.origin.service.name"
	CloudAccountName       = "cloud.account.name"
	CloudInstanceID        = "cloud.instance.id"
	CloudInstanceName      = "cloud.instance.name"
	CloudMachineType       = "cloud.machine.type"
	CloudProjectID         = "cloud.project.id"
	CloudProjectName       = "cloud.project.name"
	ContainerImageTag      = "container.image.tag"
	DeviceManufacturer     = "device.manufacturer"
	DataStreamDataset      = "data_stream.dataset"
	DataStreamNamespace    = "data_stream.namespace"
	DestinationIP          = "destination.ip"
	SourceNATIP            = "source.nat.ip"
	FaaSExecution          = "faas.execution"
	FaaSTriggerRequestID   = "faas.trigger.request_id"
	HostHostName           = "host.hostname"
	HostOSPlatform         = "host.os.platform"
	ProcessRuntimeName     = "process.runtime.name"
	ProcessRuntimeVersion  = "process.runtime.version"
	ServiceLanguageName    = "service.language.name"
	ServiceLanguageVersion = "service.language.version"
	ServiceRuntimeName     = "service.runtime.name"
	ServiceRuntimeVersion  = "service.runtime.version"
	ServiceOriginID        = "service.origin.id"
	ServiceOriginName      = "service.origin.name"
	ServiceOriginVersion   = "service.origin.version"
	UserDomain             = "user.domain"

	// scope s
	ServiceFrameworkName    = "service.framework.name"
	ServiceFrameworkVersion = "service.framework.version"

	// span s
	TimestampUs                    = "timestamp.us"
	ProcessorEvent                 = "processor.event"
	TransactionSampled             = "transaction.sampled"
	TransactionID                  = "transaction.id"
	TransactionRoot                = "transaction.root"
	TransactionName                = "transaction.name"
	TransactionType                = "transaction.type"
	TransactionDurationUs          = "transaction.duration.us"
	TransactionResult              = "transaction.result"
	TransactionRepresentativeCount = "transaction.representative_count"
	TransactionMessageQueueName    = "transaction.message.queue.name"
	SpanID                         = "span.id"
	SpanName                       = "span.name"
	SpanAction                     = "span.action"
	SpanType                       = "span.type"
	SpanSubtype                    = "span.subtype"
	SpanMessageQueueName           = "span.message.queue.name"
	EventOutcome                   = "event.outcome"
	EventKind                      = "event.kind"
	SuccessCount                   = "event.success_count"
	ServiceTargetType              = "service.target.type"
	ServiceTargetName              = "service.target.name"
	SpanDestinationServiceResource = "span.destination.service.resource"
	SpanDurationUs                 = "span.duration.us"
	SpanRepresentativeCount        = "span.representative_count"
	ChildIDs                       = "child.id"

	// span event s
	ParentID              = "parent.id"
	ErrorID               = "error.id"
	ErrorExceptionHandled = "error.exception.handled"
	ErrorGroupingKey      = "error.grouping_key"
	ErrorGroupingName     = "error.grouping_name"
	ErrorType             = "error.type"
)
