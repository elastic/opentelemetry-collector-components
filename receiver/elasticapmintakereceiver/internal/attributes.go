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

package attr // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal"

// These constants hold attribute names that are defined by the Elastic APM data model and do not match
// any SemConv attribute. These fields are not used by the UI, and store information related to a specific span type
const (
	SpanDBLink                       = "span.db.link"
	SpanDBRowsAffected               = "span.db.rows_affected"
	SpanDBUserName                   = "span.db.user_name"
	SpanCompositeSum                 = "span.composite.sum"
	SpanCompositeCompressionStrategy = "span.composite.compression_strategy"
	SpanCompositeCount               = "span.composite.count"
	SpanRepresentativeCount          = "span.representative_count"

	SpanStacktrace                  = "span.stacktrace"
	SpanStacktraceFrameAbsPath      = "abs_path"
	SpanStacktraceFrameClassname    = "classname"
	SpanStacktraceFrameFilename     = "filename"
	SpanStacktraceFrameFunction     = "function"
	SpanStacktraceFrameLineNumber   = "line.number"
	SpanStacktraceFrameLineColumn   = "line.column"
	SpanStacktraceFrameLineContext  = "line.context"
	SpanStacktraceFrameModule       = "module"
	SpanStacktraceFrameContextPre   = "context.pre"
	SpanStacktraceFrameContextPost  = "context.post"
	SpanStacktraceFrameLibraryFrame = "library_frame"
	SpanStacktraceFrameVars         = "vars"

	MessageBody          = "message.body"
	MessageAgeMs         = "message.age.ms"
	MessageHeadersPrefix = "message.headers"

	HTTPRequestBody             = "http.request.body"
	HTTPRequestID               = "http.request.id"
	HTTPRequestReferrer         = "http.request.referrer"
	HTTPResponseDecodedBodySize = "http.response.decoded_body_size"
	HTTPResponseEncodedBodySize = "http.response.encoded_body_size"
	HTTPResponseTransferSize    = "http.response.transfer_size"

	CloudProjectID   = "cloud.project.id"
	CloudProjectName = "cloud.project.name"

	TriggerRequestId = "faas.trigger.request.id"
	FaaSExecution    = "faas.execution"

	AgentEphemeralId      = "agent.ephemeral_id"
	AgentActivationMethod = "agent.activation_method"

	ServiceLanguageName     = "service.language.name"
	ServiceLanguageVersion  = "service.language.version"
	ServiceFrameworkName    = "service.framework.name"
	ServiceFrameworkVersion = "service.framework.version"
	ServiceRuntimeName      = "service.runtime.name"
	ServiceRuntimeVersion   = "service.runtime.version"
	ServiceOriginId         = "service.origin.id"
	ServiceOriginName       = "service.origin.name"
	ServiceOriginVersion    = "service.origin.version"
	ServiceTargetName       = "service.target.name"
	ServiceTargetType       = "service.target.type"

	HostOSPlatform = "host.os.platform"

	LogLogger         = "log.logger"
	LogOriginFunction = "log.origin.function"
	LogOriginFileLine = "log.origin.file.line"
	LogOriginFileName = "log.origin.file.name"

	EventAction   = "event.action"
	EventDataset  = "event.dataset"
	EventCategory = "event.category"
	EventType     = "event.type"
	EventKind     = "event.kind"

	ProcessThreadName = "process.thread.name"
	ProcessTitle      = "process.title"

	SessionID = "session.id"

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

	ErrorCustom = "error.custom"
)
