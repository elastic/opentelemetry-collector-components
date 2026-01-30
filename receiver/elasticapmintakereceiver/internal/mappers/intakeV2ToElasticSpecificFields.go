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

// This file contains mappings where we move intakeV2 fields into Attributes and Resource attributes on OTel events
// These fields are not covered by SemConv and are specific to Elastic

package mappers // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/mappers"

import (
	"fmt"
	"net/netip"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/elastic/apm-data/model/modelpb"
	attr "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal"
)

var compressionStrategyText = map[modelpb.CompressionStrategy]string{
	modelpb.CompressionStrategy_COMPRESSION_STRATEGY_EXACT_MATCH: "exact_match",
	modelpb.CompressionStrategy_COMPRESSION_STRATEGY_SAME_KIND:   "same_kind",
}

// SetElasticSpecificFieldsForSpan sets fields on spans that are not defined by OTel.
// Unlike fields from IntakeV2ToDerivedFields.go, these fields are not used by the UI
// and store information about a specific span type.
func SetElasticSpecificFieldsForSpan(event *modelpb.APMEvent, attributesMap pcommon.Map) {
	setHTTP(event.Http, attributesMap)

	if len(event.ChildIds) > 0 {
		childIDs := attributesMap.PutEmptySlice(attr.SpanChildID)
		childIDs.EnsureCapacity(len(event.ChildIds))
		for _, id := range event.ChildIds {
			childIDs.AppendEmpty().SetStr(id)
		}
	}

	if event.Span == nil {
		return
	}

	if event.Span.Db != nil {
		putNonEmptyStr(attributesMap, attr.SpanDBLink, event.Span.Db.Link)
		putPtrUint32(attributesMap, attr.SpanDBRowsAffected, event.Span.Db.RowsAffected)
		putNonEmptyStr(attributesMap, attr.SpanDBUserName, event.Span.Db.UserName)
	}

	setMessage("span", event.Span.Message, attributesMap)

	if event.Span.Composite != nil {
		compressionStrategy, ok := compressionStrategyText[event.Span.Composite.CompressionStrategy]
		if ok {
			attributesMap.PutStr(attr.SpanCompositeCompressionStrategy, compressionStrategy)
		}
		attributesMap.PutInt(attr.SpanCompositeCount, int64(event.Span.Composite.Count))

		sumDuration := time.Duration(event.Span.Composite.Sum * float64(time.Millisecond))
		attributesMap.PutInt(attr.SpanCompositeSumUs, sumDuration.Microseconds())
	}

	if event.Span.DestinationService != nil {
		attributesMap.PutStr(attr.SpanDestinationServiceName, event.Span.DestinationService.Name)
		attributesMap.PutStr(attr.SpanDestinationServiceType, event.Span.DestinationService.Type)
	}

	attributesMap.PutDouble(attr.SpanRepresentativeCount, event.Span.RepresentativeCount)

	setStackTraceList(attr.SpanStacktrace, attributesMap, event.Span.Stacktrace)
}

// setHTTP sets HTTP fields. Applicable only for error, span, or transaction events
func setHTTP(http *modelpb.HTTP, attributesMap pcommon.Map) {
	if http == nil {
		return
	}

	putNonEmptyStr(attributesMap, attr.HTTPVersion, http.Version)

	if http.Request != nil {
		setHTTPHeadersMap(attr.HTTPRequestHeaders, attributesMap, http.Request.Headers)
		setKeyValueSliceMap(attr.HTTPRequestEnv, attributesMap, http.Request.Env)
		setKeyValueSliceMap(attr.HTTPRequestCookies, attributesMap, http.Request.Cookies)

		if http.Request.Body != nil {
			// add http body as an object since it is required by the APM index template
			// see: https://github.com/elastic/elasticsearch/blob/714c077b11363f168e261ad43cff0b5b74556b7f/x-pack/plugin/apm-data/src/main/resources/component-templates/traces-apm%40mappings.yaml#L30
			bodyValue := attributesMap.PutEmpty(attr.HTTPRequestBodyOriginal)
			insertValue(bodyValue, http.Request.Body)
		}
		putNonEmptyStr(attributesMap, attr.HTTPRequestID, http.Request.Id)
		putNonEmptyStr(attributesMap, attr.HTTPRequestReferrer, http.Request.Referrer)
	}

	if http.Response != nil {
		setHTTPHeadersMap(attr.HTTPResponseHeaders, attributesMap, http.Response.Headers)

		putPtrBool(attributesMap, attr.HTTPResponseFinished, http.Response.Finished)
		putPtrBool(attributesMap, attr.HTTPResponseHeadersSent, http.Response.HeadersSent)
		putPtrUint64(attributesMap, attr.HTTPResponseDecodedBodySize, http.Response.DecodedBodySize)
		putPtrUint64(attributesMap, attr.HTTPResponseTransferSize, http.Response.TransferSize)
	}
}

// setKeyValueSliceMap adds a list of KeyValue pairs as a map attribute with the given prefix.
func setKeyValueSliceMap(prefix string, attributesMap pcommon.Map, kvList []*modelpb.KeyValue) {
	if len(kvList) == 0 {
		return
	}

	kvMap := attributesMap.PutEmptyMap(prefix)
	kvMap.EnsureCapacity(len(kvList))
	for _, entry := range kvList {
		if entry != nil && entry.Key != "" && entry.Value != nil {
			mapEntry := kvMap.PutEmpty(entry.Key)
			insertValue(mapEntry, entry.Value)
		}
	}
}

// setHTTPHeadersMap sets HTTP headers to attributes map.
// The headers will be a map of string to array of strings.
func setHTTPHeadersMap(prefix string, attributesMap pcommon.Map, headers []*modelpb.HTTPHeader) {
	if len(headers) == 0 {
		return
	}

	headerMap := attributesMap.PutEmptyMap(prefix)
	headerMap.EnsureCapacity(len(headers))
	for _, header := range headers {
		if header != nil && header.Key != "" && len(header.Value) > 0 {
			headerValues := headerMap.PutEmptySlice(header.Key)
			headerValues.EnsureCapacity(len(header.Value))
			for _, v := range header.Value {
				headerValues.AppendEmpty().SetStr(v)
			}
		}
	}
}

// setMessage sets message fields from either a span or transaction message.
func setMessage(prefix string, m *modelpb.Message, attributesMap pcommon.Map) {
	if m == nil {
		return
	}
	putNonEmptyStr(attributesMap, fmt.Sprintf("%s.%s", prefix, attr.MessageRoutingKey), m.RoutingKey)
	putNonEmptyStr(attributesMap, fmt.Sprintf("%s.%s", prefix, attr.MessageBody), m.Body)
	putPtrUint64(attributesMap, fmt.Sprintf("%s.%s", prefix, attr.MessageAgeMs), m.AgeMillis)
	for _, header := range m.Headers {
		headerKey := fmt.Sprintf("%s.%s.%s", prefix, attr.MessageHeadersPrefix, header.Key)
		headerValues := attributesMap.PutEmptySlice(headerKey)
		headerValues.EnsureCapacity(len(header.Value))
		for _, v := range header.Value {
			headerValues.AppendEmpty().SetStr(v)
		}
	}
}

// setStackTraceList maps stacktrace frames to attributes map.
// The stacktrace will be a list of objects (maps), each map representing a frame.
func setStackTraceList(key string, attributesMap pcommon.Map, stacktrace []*modelpb.StacktraceFrame) {
	if len(stacktrace) == 0 {
		return
	}

	stacktraceSlice := attributesMap.PutEmptySlice(key)
	stacktraceSlice.EnsureCapacity(len(stacktrace))
	for _, frame := range stacktrace {
		frameMap := stacktraceSlice.AppendEmpty().SetEmptyMap()

		setKeyValueMap(attr.SpanStacktraceFrameVars, frameMap, frame.Vars)

		putPtrUint32(frameMap, attr.SpanStacktraceFrameLineNumber, frame.Lineno)
		putPtrUint32(frameMap, attr.SpanStacktraceFrameLineColumn, frame.Colno)
		putNonEmptyStr(frameMap, attr.SpanStacktraceFrameFilename, frame.Filename)
		putNonEmptyStr(frameMap, attr.SpanStacktraceFrameClassname, frame.Classname)
		putNonEmptyStr(frameMap, attr.SpanStacktraceFrameLineContext, frame.ContextLine)
		putNonEmptyStr(frameMap, attr.SpanStacktraceFrameModule, frame.Module)
		putNonEmptyStr(frameMap, attr.SpanStacktraceFrameFunction, frame.Function)
		putNonEmptyStr(frameMap, attr.SpanStacktraceFrameAbsPath, frame.AbsPath)

		if len(frame.PreContext) > 0 {
			preSlice := frameMap.PutEmptySlice(attr.SpanStacktraceFrameContextPre)
			preSlice.EnsureCapacity(len(frame.PreContext))
			for _, pre := range frame.PreContext {
				preSlice.AppendEmpty().SetStr(pre)
			}
		}
		if len(frame.PostContext) > 0 {
			postSlice := frameMap.PutEmptySlice(attr.SpanStacktraceFrameContextPost)
			postSlice.EnsureCapacity(len(frame.PostContext))
			for _, post := range frame.PostContext {
				postSlice.AppendEmpty().SetStr(post)
			}
		}

		if frame.LibraryFrame {
			frameMap.PutBool(attr.SpanStacktraceFrameLibraryFrame, frame.LibraryFrame)
		}
		// Note: ExcludeFromGrouping does not have an 'omitempty' json tag in the apm-data model
		// so we always set it to match the existing behavior.
		// The flag is also not available in input data, so it will always be false.
		frameMap.PutBool(attr.SpanStacktraceExcludeFromGrouping, frame.ExcludeFromGrouping)
	}
}

// setKeyValueMap maps a list of KeyValue pairs to a map attribute with the given key name.
// Ignores pairs with an empty key.
func setKeyValueMap(mapKeyName string, attributesMap pcommon.Map, kvList []*modelpb.KeyValue) {
	if len(kvList) == 0 {
		return
	}
	kvMap := attributesMap.PutEmptyMap(mapKeyName)
	kvMap.EnsureCapacity(len(kvList))
	for _, kv := range kvList {
		if len(kv.Key) > 0 {
			insertValue(kvMap.PutEmpty(kv.Key), kv.Value)
		}
	}
}

// insertValue inserts a value into the provided destination which can represent
// any pcommon.Value such as a map, slice, string, number, or bool.
// Ignores empty src values.
func insertValue(dest pcommon.Value, src *structpb.Value) {
	if src == nil {
		return
	}
	switch v := src.Kind.(type) {
	case *structpb.Value_StringValue:
		dest.SetStr(v.StringValue)
	case *structpb.Value_NumberValue:
		dest.SetDouble(v.NumberValue)
	case *structpb.Value_BoolValue:
		dest.SetBool(v.BoolValue)
	case *structpb.Value_StructValue:
		if v.StructValue == nil {
			return
		}
		destMap := dest.SetEmptyMap()
		destMap.EnsureCapacity(len(v.StructValue.Fields))
		for key, value := range v.StructValue.Fields {
			insertValue(destMap.PutEmpty(key), value)
		}
	case *structpb.Value_ListValue:
		if v.ListValue == nil {
			return
		}
		destSlice := dest.SetEmptySlice()
		destSlice.EnsureCapacity(len(v.ListValue.Values))
		for _, value := range v.ListValue.Values {
			insertValue(destSlice.AppendEmpty(), value)
		}
	}
}

// SetElasticSpecificFieldsForTransaction sets fields for transactions that are not defined by OTel.
// Unlike fields from IntakeV2ToDerivedFields.go, these fields are not used by the UI
// and store information about a specific span type.
func SetElasticSpecificFieldsForTransaction(event *modelpb.APMEvent, attributesMap pcommon.Map) {
	setHTTP(event.Http, attributesMap)

	if event.Transaction == nil {
		return
	}

	if event.Transaction.SpanCount != nil {
		putPtrUint32(attributesMap, attr.TransactionSpanCountStarted, event.Transaction.SpanCount.Started)
		putPtrUint32(attributesMap, attr.TransactionSpanCountDropped, event.Transaction.SpanCount.Dropped)
	}

	if event.Transaction.UserExperience != nil {
		attributesMap.PutDouble(attr.TransactionUserExperienceCumulativeLayoutShift, event.Transaction.UserExperience.CumulativeLayoutShift)
		attributesMap.PutDouble(attr.TransactionUserExperienceFirstInputDelay, event.Transaction.UserExperience.FirstInputDelay)
		attributesMap.PutDouble(attr.TransactionUserExperienceTotalBlockingTime, event.Transaction.UserExperience.TotalBlockingTime)

		if event.Transaction.UserExperience.LongTask != nil {
			attributesMap.PutInt(attr.TransactionUserExperienceLongTaskCount, int64(event.Transaction.UserExperience.LongTask.Count))
			attributesMap.PutDouble(attr.TransactionUserExperienceLongTaskMax, event.Transaction.UserExperience.LongTask.Max)
			attributesMap.PutDouble(attr.TransactionUserExperienceLongTaskSum, event.Transaction.UserExperience.LongTask.Sum)
		}
	}

	setKeyValueMap(attr.TransactionCustom, attributesMap, event.Transaction.Custom)
	setProfilerStackTraceIDs(event.Transaction.ProfilerStackTraceIds, attributesMap)

	// DroppedSpansStats is only indexed for metric documents. See apm-data json encoding:
	// https://github.com/elastic/apm-data/blob/e9e8f6955fdf65ffff444db65fce745f5bbc8d43/model/modeljson/transaction.pb.json.go#L66
	// TODO: Verify if this field should be mapped since it may not be used by any Elastic OTEL components.
	if event.Metricset != nil {
		setDroppedSpansStatsList(event.Transaction.DroppedSpansStats, attributesMap)
	}

	setTransactionMarks(event.Transaction.Marks, attributesMap)
	setMessage("transaction", event.Transaction.Message, attributesMap)

	// add session attributes which hold optional transaction session information for RUM
	if event.Session != nil {
		putNonEmptyStr(attributesMap, attr.SessionID, event.Session.Id)
		if event.Session.Sequence != 0 {
			attributesMap.PutInt(attr.SessionSequence, int64(event.Session.Sequence))
		}
	}
}

func setProfilerStackTraceIDs(ids []string, attributesMap pcommon.Map) {
	if len(ids) == 0 {
		return
	}

	slice := attributesMap.PutEmptySlice(attr.TransactionProfilerStackTraceIDs)
	slice.EnsureCapacity(len(ids))
	for _, id := range ids {
		slice.AppendEmpty().SetStr(id)
	}
}

func setDroppedSpansStatsList(stats []*modelpb.DroppedSpanStats, attributesMap pcommon.Map) {
	if len(stats) == 0 {
		return
	}

	statsSlice := attributesMap.PutEmptySlice(attr.TransactionDroppedSpansStats)
	statsSlice.EnsureCapacity(len(stats))
	for _, stat := range stats {
		if stat == nil {
			continue
		}

		statMap := statsSlice.AppendEmpty().SetEmptyMap()
		putNonEmptyStr(statMap, attr.TransactionDroppedSpansStatsDestinationServiceResource, stat.DestinationServiceResource)
		putNonEmptyStr(statMap, attr.TransactionDroppedSpansStatsOutcome, stat.Outcome)
		if stat.Duration != nil {
			statMap.PutInt(attr.TransactionDroppedSpansStatsDurationCount, int64(stat.Duration.Count))
			statMap.PutInt(attr.TransactionDroppedSpansStatsDurationSumUs, int64(stat.Duration.Sum))
		}
	}
}

// setTransactionMarks maps transaction marks to attributes map.
// The marks will be a map of objects (maps), each map representing a mark with measurements.
// Empty marks will be ignored.
func setTransactionMarks(marks map[string]*modelpb.TransactionMark, attributesMap pcommon.Map) {
	if len(marks) == 0 {
		return
	}

	allMarksMap := attributesMap.PutEmptyMap(attr.TransactionMarks)
	allMarksMap.EnsureCapacity(len(marks))
	for markName, mark := range marks {
		if mark == nil || len(mark.Measurements) == 0 {
			continue
		}
		markMap := allMarksMap.PutEmptyMap(markName)
		markMap.EnsureCapacity(len(mark.Measurements))
		for measurementName, measurement := range mark.Measurements {
			markMap.PutDouble(measurementName, measurement)
		}
	}
}

// SetElasticSpecificResourceAttributes maps APM event fields to OTel attributes at the resource level.
// The majority of the APM event fields are from the APM metadata model, so this mapping is applicable
// to all event types (OTel  signals).
// Some APM events may contain fields that are APM metadata e.g error.context.service.framework will override
// the framework provided in the metadata. The apm-data library handles the override, so this function simply
// sets the resource attribute.
// These fields are not defined by OTel.
// Unlike fields from IntakeV2ToDerivedFields.go, these fields are not used by the UI.
func SetElasticSpecificResourceAttributes(event *modelpb.APMEvent, attributesMap pcommon.Map) {
	if event.Cloud != nil {
		if event.Cloud.Origin != nil {
			putNonEmptyStr(attributesMap, attr.CloudOriginAccountID, event.Cloud.Origin.AccountId)
			putNonEmptyStr(attributesMap, attr.CloudOriginProvider, event.Cloud.Origin.Provider)
			putNonEmptyStr(attributesMap, attr.CloudOriginRegion, event.Cloud.Origin.Region)
			putNonEmptyStr(attributesMap, attr.CloudOriginServiceName, event.Cloud.Origin.ServiceName)
		}
		putNonEmptyStr(attributesMap, attr.CloudAccountName, event.Cloud.AccountName)
		putNonEmptyStr(attributesMap, attr.CloudInstanceID, event.Cloud.InstanceId)
		putNonEmptyStr(attributesMap, attr.CloudInstanceName, event.Cloud.InstanceName)
		putNonEmptyStr(attributesMap, attr.CloudMachineType, event.Cloud.MachineType)
		putNonEmptyStr(attributesMap, attr.CloudProjectID, event.Cloud.ProjectId)
		putNonEmptyStr(attributesMap, attr.CloudProjectName, event.Cloud.ProjectName)
	}

	if event.Faas != nil {
		putNonEmptyStr(attributesMap, attr.FaaSTriggerRequestID, event.Faas.TriggerRequestId)
		putNonEmptyStr(attributesMap, attr.FaaSExecution, event.Faas.Execution)
	}

	if event.Agent != nil {
		putNonEmptyStr(attributesMap, attr.AgentEphemeralID, event.Agent.EphemeralId)
		putNonEmptyStr(attributesMap, attr.AgentActivationMethod, event.Agent.ActivationMethod)
	}

	if event.Service != nil {
		if event.Service.Framework != nil {
			putNonEmptyStr(attributesMap, attr.ServiceFrameworkName, event.Service.Framework.Name)
			putNonEmptyStr(attributesMap, attr.ServiceFrameworkVersion, event.Service.Framework.Version)
		}
		if event.Service.Runtime != nil {
			putNonEmptyStr(attributesMap, attr.ServiceRuntimeName, event.Service.Runtime.Name)
			putNonEmptyStr(attributesMap, attr.ServiceRuntimeVersion, event.Service.Runtime.Version)
		}
		if event.Service.Origin != nil {
			putNonEmptyStr(attributesMap, attr.ServiceOriginID, event.Service.Origin.Id)
			putNonEmptyStr(attributesMap, attr.ServiceOriginName, event.Service.Origin.Name)
			putNonEmptyStr(attributesMap, attr.ServiceOriginVersion, event.Service.Origin.Version)
		}
	}

	if event.Host != nil {
		if event.Host.Os != nil {
			putNonEmptyStr(attributesMap, attr.HostOSPlatform, event.Host.Os.Platform)
		}
		putNonEmptyStr(attributesMap, attr.HostHostname, event.Host.Hostname)
	}

	if event.Source != nil {
		if event.Source.Nat != nil && event.Source.Nat.Ip != nil {
			ip := modelpb.IP2Addr(event.Source.Nat.Ip)
			putNonEmptyStr(attributesMap, attr.SourceNatIP, ip.String())
		}
	}

	if event.User != nil {
		putNonEmptyStr(attributesMap, attr.UserDomain, event.User.Domain)
	}

	if event.Destination != nil {
		if event.Destination.Address != "" {
			if ip, err := netip.ParseAddr(event.Destination.Address); err == nil {
				attributesMap.PutStr(attr.DestinationIP, ip.String())
			}
		}
	}

	setLabels(event, attributesMap)
}

// setLabels sets single value label fields from the APMEvent Labels and NumericLabels fields.
// Labels are added as attributes with appropriate key prefixes: "labels." and "numeric_labels.".
// Allows key names with spaces to match existing behavior.
// Ignored empty keys and values.
//
// The apm data library logic will take care of overwriting metadata labels with event labels when decoding
// the input to modelpb.APMEvent, so we simply copy all labels from the event here.
func setLabels(event *modelpb.APMEvent, attributesMap pcommon.Map) {
	for key, labelValue := range event.Labels {
		if key != "" && labelValue != nil && labelValue.Value != "" {
			attrKey := "labels." + key
			attributesMap.PutStr(attrKey, labelValue.Value)
		}

		if key != "" && labelValue != nil && len(labelValue.Values) > 0 {
			attrKey := "labels." + key
			labelValues := attributesMap.PutEmptySlice(attrKey)
			labelValues.EnsureCapacity(len(labelValue.Values))
			for _, v := range labelValue.Values {
				labelValues.AppendEmpty().SetStr(v)
			}
		}
	}

	for key, numericLabelValue := range event.NumericLabels {
		if key != "" && numericLabelValue != nil && numericLabelValue.Value != 0 {
			attrKey := "numeric_labels." + key
			attributesMap.PutDouble(attrKey, numericLabelValue.Value)
		}
	}
}

// SetElasticSpecificFieldsForLog sets fields that are not defined by OTel.
// Unlike fields from IntakeV2ToDerivedFields.go, these fields are not used by the UI.
func SetElasticSpecificFieldsForLog(event *modelpb.APMEvent, attributesMap pcommon.Map) {
	setHTTP(event.Http, attributesMap)

	// processor.event is not set for logs
	if event.Log != nil {
		putNonEmptyStr(attributesMap, attr.LogLogger, event.Log.Logger)

		if event.Log.Origin != nil {
			putNonEmptyStr(attributesMap, attr.LogOriginFunction, event.Log.Origin.FunctionName)
			if event.Log.Origin.File != nil {
				if event.Log.Origin.File.Line != 0 {
					attributesMap.PutInt(attr.LogOriginFileLine, int64(event.Log.Origin.File.Line))
				}
				putNonEmptyStr(attributesMap, attr.LogOriginFileName, event.Log.Origin.File.Name)
			}
		}
	}

	if event.Event != nil {
		putNonEmptyStr(attributesMap, attr.EventAction, event.Event.Action)
		putNonEmptyStr(attributesMap, attr.EventDataset, event.Event.Dataset)
		putNonEmptyStr(attributesMap, attr.EventCategory, event.Event.Category)
		putNonEmptyStr(attributesMap, attr.EventType, event.Event.Type)
	}

	if event.Error == nil {
		attributesMap.PutStr(attr.EventKind, "event")
	}

	if event.Process != nil {
		if event.Process.Thread != nil {
			// only the process thread name is available at the log level
			putNonEmptyStr(attributesMap, attr.ProcessThreadName, event.Process.Thread.Name)
		}
	}

	if event.Error != nil {
		setKeyValueMap(attr.ErrorCustom, attributesMap, event.Error.Custom)

		if event.Error.Log != nil {
			putNonEmptyStr(attributesMap, attr.ErrorLogMessage, event.Error.Log.Message)
			putNonEmptyStr(attributesMap, attr.ErrorLogLevel, event.Error.Log.Level)
			putNonEmptyStr(attributesMap, attr.ErrorLogParamMessage, event.Error.Log.ParamMessage)
			putNonEmptyStr(attributesMap, attr.ErrorLogLoggerName, event.Error.Log.LoggerName)
			setStackTraceList(attr.ErrorLogStackTrace, attributesMap, event.Error.Log.Stacktrace)
		}
	}
}
