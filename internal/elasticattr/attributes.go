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

package elasticattr // import "github.com/elastic/opentelemetry-collector-components/internal/elasticattr"

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
	DataStreamType         = "data_stream.type"
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
	TimestampUs                                            = "timestamp.us"
	ProcessorEvent                                         = "processor.event"
	TransactionSampled                                     = "transaction.sampled"
	TransactionID                                          = "transaction.id"
	TransactionRoot                                        = "transaction.root"
	TransactionName                                        = "transaction.name"
	TransactionType                                        = "transaction.type"
	TransactionDurationUs                                  = "transaction.duration.us"
	TransactionResult                                      = "transaction.result"
	TransactionRepresentativeCount                         = "transaction.representative_count"
	TransactionMessageQueueName                            = "transaction.message.queue.name"
	TransactionCustom                                      = "transaction.custom"
	TransactionUserExperienceCumulativeLayoutShift         = "transaction.experience.cls"
	TransactionUserExperienceFirstInputDelay               = "transaction.experience.fid"
	TransactionUserExperienceLongTaskCount                 = "transaction.experience.longtask.count"
	TransactionUserExperienceLongTaskMax                   = "transaction.experience.longtask.max"
	TransactionUserExperienceLongTaskSum                   = "transaction.experience.longtask.sum"
	TransactionUserExperienceTotalBlockingTime             = "transaction.experience.tbt"
	TransactionSpanCountStarted                            = "transaction.span_count.started"
	TransactionSpanCountDropped                            = "transaction.span_count.dropped"
	TransactionMarks                                       = "transaction.marks"
	TransactionProfilerStackTraceIDs                       = "transaction.profiler_stack_trace_ids"
	TransactionDroppedSpansStats                           = "transaction.dropped_spans_stats"
	TransactionDroppedSpansStatsDestinationServiceResource = "destination_service_resource"
	TransactionDroppedSpansStatsOutcome                    = "outcome"
	TransactionDroppedSpansStatsDurationCount              = "duration.count"
	TransactionDroppedSpansStatsDurationSumUs              = "duration.sum.us"
	SpanID                                                 = "span.id"
	SpanName                                               = "span.name"
	SpanAction                                             = "span.action"
	SpanType                                               = "span.type"
	SpanSubtype                                            = "span.subtype"
	SpanMessageQueueName                                   = "span.message.queue.name"
	SpanDBLink                                             = "span.db.link"
	SpanDBRowsAffected                                     = "span.db.rows_affected"
	SpanDBUserName                                         = "span.db.user.name"
	SpanCompositeSumUs                                     = "span.composite.sum.us"
	SpanCompositeCompressionStrategy                       = "span.composite.compression_strategy"
	SpanCompositeCount                                     = "span.composite.count"
	SpanDestinationServiceName                             = "span.destination.service.name"
	SpanDestinationServiceType                             = "span.destination.service.type"
	SpanDestinationServiceResource                         = "span.destination.service.resource"
	SpanDurationUs                                         = "span.duration.us"
	SpanRepresentativeCount                                = "span.representative_count"
	ChildIDs                                               = "child.id"
	SpanChildID                                            = "child.id" // alias for ChildIDs
	SpanStacktrace                                         = "span.stacktrace"
	SpanStacktraceFrameAbsPath                             = "abs_path"
	SpanStacktraceFrameClassname                           = "classname"
	SpanStacktraceFrameFilename                            = "filename"
	SpanStacktraceFrameFunction                            = "function"
	SpanStacktraceFrameLineNumber                          = "line.number"
	SpanStacktraceFrameLineColumn                          = "line.column"
	SpanStacktraceFrameLineContext                         = "line.context"
	SpanStacktraceFrameModule                              = "module"
	SpanStacktraceFrameContextPre                          = "context.pre"
	SpanStacktraceFrameContextPost                         = "context.post"
	SpanStacktraceFrameLibraryFrame                        = "library_frame"
	SpanStacktraceExcludeFromGrouping                      = "exclude_from_grouping"
	SpanStacktraceFrameVars                                = "vars"
	EventOutcome                                           = "event.outcome"
	EventKind                                              = "event.kind"
	EventAction                                            = "event.action"
	EventDataset                                           = "event.dataset"
	EventDomain                                            = "event.domain"
	EventCategory                                          = "event.category"
	EventType                                              = "event.type"
	SuccessCount                                           = "event.success_count"
	ServiceTargetType                                      = "service.target.type"
	ServiceTargetName                                      = "service.target.name"

	// span event s
	ParentID              = "parent.id"
	ErrorID               = "error.id"
	ErrorExceptionHandled = "error.exception.handled"
	ErrorGroupingKey      = "error.grouping_key"
	ErrorGroupingName     = "error.grouping_name"
	ErrorType             = "error.type"
	ErrorCustom           = "error.custom"
	ErrorMessage          = "error.message"
	ErrorStackTrace       = "error.stack_trace"
	ErrorCulprit          = "error.culprit"
	ErrorLogMessage       = "error.log.message"
	ErrorLogLevel         = "error.log.level"
	ErrorLogParamMessage  = "error.log.param_message"
	ErrorLogLoggerName    = "error.log.logger_name"
	ErrorLogStackTrace    = "error.log.stacktrace"

	// Error exception attributes
	ErrorException                              = "error.exception"
	ErrorExceptionCode                          = "code"
	ErrorExceptionMessage                       = "message"
	ErrorExceptionType                          = "type"
	ErrorExceptionModule                        = "module"
	ErrorExceptionAttributes                    = "attributes"
	ErrorExceptionStacktrace                    = "stacktrace"
	ErrorExceptionParent                        = "parent"
	ErrorExceptionStacktraceAbsPath             = "abs_path"
	ErrorExceptionStacktraceFilename            = "filename"
	ErrorExceptionStacktraceClassname           = "classname"
	ErrorExceptionStacktraceFunction            = "function"
	ErrorExceptionStacktraceModule              = "module"
	ErrorExceptionStacktraceLineNumber          = "line.number"
	ErrorExceptionStacktraceLineColumn          = "line.column"
	ErrorExceptionStacktraceLineContext         = "line.context"
	ErrorExceptionStacktraceContextPre          = "context.pre"
	ErrorExceptionStacktraceContextPost         = "context.post"
	ErrorExceptionStacktraceLibraryFrame        = "library_frame"
	ErrorExceptionStacktraceExcludeFromGrouping = "exclude_from_grouping"
	ErrorExceptionStacktraceVars                = "vars"

	// ErrorExceptionHandled is used within exception objects (value: "handled")
	// For top-level error.exception.handled, use the constant above (value: "error.exception.handled")
	ErrorExceptionHandledField = "handled"

	// HTTP attributes
	HTTPVersion                 = "http.version"
	HTTPRequestHeaders          = "http.request.headers"
	HTTPRequestEnv              = "http.request.env"
	HTTPRequestCookies          = "http.request.cookies"
	HTTPRequestBodyOriginal     = "http.request.body.original"
	HTTPRequestID               = "http.request.id"
	HTTPRequestReferrer         = "http.request.referrer"
	HTTPResponseFinished        = "http.response.finished"
	HTTPResponseHeaders         = "http.response.headers"
	HTTPResponseHeadersSent     = "http.response.headers_sent"
	HTTPResponseDecodedBodySize = "http.response.decoded_body_size"
	HTTPResponseTransferSize    = "http.response.transfer_size"

	// Message attributes
	MessageRoutingKey    = "message.routing_key"
	MessageBody          = "message.body"
	MessageAgeMs         = "message.age.ms"
	MessageHeadersPrefix = "message.headers"

	// Log attributes
	LogLogger         = "log.logger"
	LogOriginFunction = "log.origin.function"
	LogOriginFileLine = "log.origin.file.line"
	LogOriginFileName = "log.origin.file.name"

	// Process attributes
	ProcessThreadName = "process.thread.name"

	// Session attributes
	SessionID       = "session.id"
	SessionSequence = "session.sequence"
)
