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
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
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
		childIDs := attributesMap.PutEmptySlice(elasticattr.SpanChildID)
		childIDs.EnsureCapacity(len(event.ChildIds))
		for _, id := range event.ChildIds {
			childIDs.AppendEmpty().SetStr(id)
		}
	}

	if event.Span == nil {
		return
	}

	if event.Span.Db != nil {
		putNonEmptyStr(attributesMap, elasticattr.SpanDBLink, event.Span.Db.Link)
		putPtrInt(attributesMap, elasticattr.SpanDBRowsAffected, event.Span.Db.RowsAffected)
		putNonEmptyStr(attributesMap, elasticattr.SpanDBUserName, event.Span.Db.UserName)
	}

	setMessage("span", event.Span.Message, attributesMap)

	if event.Span.Composite != nil {
		compressionStrategy, ok := compressionStrategyText[event.Span.Composite.CompressionStrategy]
		if ok {
			attributesMap.PutStr(elasticattr.SpanCompositeCompressionStrategy, compressionStrategy)
		}
		attributesMap.PutInt(elasticattr.SpanCompositeCount, int64(event.Span.Composite.Count))

		sumDuration := time.Duration(event.Span.Composite.Sum * float64(time.Millisecond))
		attributesMap.PutInt(elasticattr.SpanCompositeSumUs, sumDuration.Microseconds())
	}

	if event.Span.DestinationService != nil {
		attributesMap.PutStr(elasticattr.SpanDestinationServiceName, event.Span.DestinationService.Name)
		attributesMap.PutStr(elasticattr.SpanDestinationServiceType, event.Span.DestinationService.Type)
	}

	attributesMap.PutDouble(elasticattr.SpanRepresentativeCount, event.Span.RepresentativeCount)

	setStackTraceList(elasticattr.SpanStacktrace, attributesMap, event.Span.Stacktrace)
}

// setHTTP sets HTTP fields. Applicable only for error, span, or transaction events
func setHTTP(http *modelpb.HTTP, attributesMap pcommon.Map) {
	if http == nil {
		return
	}

	putNonEmptyStr(attributesMap, elasticattr.HTTPVersion, http.Version)

	if http.Request != nil {
		setHTTPHeadersMap(elasticattr.HTTPRequestHeaders, attributesMap, http.Request.Headers)
		setKeyValueSliceMap(elasticattr.HTTPRequestEnv, attributesMap, http.Request.Env)
		setKeyValueSliceMap(elasticattr.HTTPRequestCookies, attributesMap, http.Request.Cookies)

		if http.Request.Body != nil {
			// add http body as an object since it is required by the APM index template
			// see: https://github.com/elastic/elasticsearch/blob/714c077b11363f168e261ad43cff0b5b74556b7f/x-pack/plugin/apm-data/src/main/resources/component-templates/traces-apm%40mappings.yaml#L30
			bodyValue := attributesMap.PutEmpty(elasticattr.HTTPRequestBodyOriginal)
			insertValue(bodyValue, http.Request.Body)
		}
		putNonEmptyStr(attributesMap, elasticattr.HTTPRequestID, http.Request.Id)
		putNonEmptyStr(attributesMap, elasticattr.HTTPRequestReferrer, http.Request.Referrer)
	}

	if http.Response != nil {
		setHTTPHeadersMap(elasticattr.HTTPResponseHeaders, attributesMap, http.Response.Headers)

		putPtrBool(attributesMap, elasticattr.HTTPResponseFinished, http.Response.Finished)
		putPtrBool(attributesMap, elasticattr.HTTPResponseHeadersSent, http.Response.HeadersSent)
		putPtrInt(attributesMap, elasticattr.HTTPResponseDecodedBodySize, http.Response.DecodedBodySize)
		putPtrInt(attributesMap, elasticattr.HTTPResponseTransferSize, http.Response.TransferSize)
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
	putNonEmptyStr(attributesMap, fmt.Sprintf("%s.%s", prefix, elasticattr.MessageRoutingKey), m.RoutingKey)
	putNonEmptyStr(attributesMap, fmt.Sprintf("%s.%s", prefix, elasticattr.MessageBody), m.Body)
	putPtrInt(attributesMap, fmt.Sprintf("%s.%s", prefix, elasticattr.MessageAgeMs), m.AgeMillis)
	for _, header := range m.Headers {
		headerKey := fmt.Sprintf("%s.%s.%s", prefix, elasticattr.MessageHeadersPrefix, header.Key)
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

		setKeyValueMap(elasticattr.SpanStacktraceFrameVars, frameMap, frame.Vars)

		putPtrInt(frameMap, elasticattr.SpanStacktraceFrameLineNumber, frame.Lineno)
		putPtrInt(frameMap, elasticattr.SpanStacktraceFrameLineColumn, frame.Colno)
		putNonEmptyStr(frameMap, elasticattr.SpanStacktraceFrameFilename, frame.Filename)
		putNonEmptyStr(frameMap, elasticattr.SpanStacktraceFrameClassname, frame.Classname)
		putNonEmptyStr(frameMap, elasticattr.SpanStacktraceFrameLineContext, frame.ContextLine)
		putNonEmptyStr(frameMap, elasticattr.SpanStacktraceFrameModule, frame.Module)
		putNonEmptyStr(frameMap, elasticattr.SpanStacktraceFrameFunction, frame.Function)
		putNonEmptyStr(frameMap, elasticattr.SpanStacktraceFrameAbsPath, frame.AbsPath)

		if len(frame.PreContext) > 0 {
			preSlice := frameMap.PutEmptySlice(elasticattr.SpanStacktraceFrameContextPre)
			preSlice.EnsureCapacity(len(frame.PreContext))
			for _, pre := range frame.PreContext {
				preSlice.AppendEmpty().SetStr(pre)
			}
		}
		if len(frame.PostContext) > 0 {
			postSlice := frameMap.PutEmptySlice(elasticattr.SpanStacktraceFrameContextPost)
			postSlice.EnsureCapacity(len(frame.PostContext))
			for _, post := range frame.PostContext {
				postSlice.AppendEmpty().SetStr(post)
			}
		}

		if frame.LibraryFrame {
			frameMap.PutBool(elasticattr.SpanStacktraceFrameLibraryFrame, frame.LibraryFrame)
		}
		// Note: ExcludeFromGrouping does not have an 'omitempty' json tag in the apm-data model
		// so we always set it to match the existing behavior.
		// The flag is also not available in input data, so it will always be false.
		frameMap.PutBool(elasticattr.SpanStacktraceExcludeFromGrouping, frame.ExcludeFromGrouping)
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
		putPtrInt(attributesMap, elasticattr.TransactionSpanCountStarted, event.Transaction.SpanCount.Started)
		putPtrInt(attributesMap, elasticattr.TransactionSpanCountDropped, event.Transaction.SpanCount.Dropped)
	}

	if event.Transaction.UserExperience != nil {
		attributesMap.PutDouble(elasticattr.TransactionUserExperienceCumulativeLayoutShift, event.Transaction.UserExperience.CumulativeLayoutShift)
		attributesMap.PutDouble(elasticattr.TransactionUserExperienceFirstInputDelay, event.Transaction.UserExperience.FirstInputDelay)
		attributesMap.PutDouble(elasticattr.TransactionUserExperienceTotalBlockingTime, event.Transaction.UserExperience.TotalBlockingTime)

		if event.Transaction.UserExperience.LongTask != nil {
			attributesMap.PutInt(elasticattr.TransactionUserExperienceLongTaskCount, int64(event.Transaction.UserExperience.LongTask.Count))
			attributesMap.PutDouble(elasticattr.TransactionUserExperienceLongTaskMax, event.Transaction.UserExperience.LongTask.Max)
			attributesMap.PutDouble(elasticattr.TransactionUserExperienceLongTaskSum, event.Transaction.UserExperience.LongTask.Sum)
		}
	}

	setKeyValueMap(elasticattr.TransactionCustom, attributesMap, event.Transaction.Custom)
	setProfilerStackTraceIDs(event.Transaction.ProfilerStackTraceIds, attributesMap)

	// transaction.dropped_spans_stats is intentionally not surfaced on the
	// transaction span. It is instead expanded into one synthetic CLIENT
	// span per stat by the receiver, so elasticapmconnector can derive
	// service_destination metrics for the dropped/compressed spans.
	// See appendDroppedSpansStatsSpans in receiver.go.

	setTransactionMarks(event.Transaction.Marks, attributesMap)
	setMessage("transaction", event.Transaction.Message, attributesMap)

	// add session attributes which hold optional transaction session information for RUM
	if event.Session != nil {
		putNonEmptyStr(attributesMap, elasticattr.SessionID, event.Session.Id)
		if event.Session.Sequence != 0 {
			attributesMap.PutInt(elasticattr.SessionSequence, int64(event.Session.Sequence))
		}
	}
}

func setProfilerStackTraceIDs(ids []string, attributesMap pcommon.Map) {
	if len(ids) == 0 {
		return
	}

	slice := attributesMap.PutEmptySlice(elasticattr.TransactionProfilerStackTraceIDs)
	slice.EnsureCapacity(len(ids))
	for _, id := range ids {
		slice.AppendEmpty().SetStr(id)
	}
}

// setTransactionMarks maps transaction marks to attributes map.
// The marks will be a map of objects (maps), each map representing a mark with measurements.
// Empty marks will be ignored.
func setTransactionMarks(marks map[string]*modelpb.TransactionMark, attributesMap pcommon.Map) {
	if len(marks) == 0 {
		return
	}

	allMarksMap := attributesMap.PutEmptyMap(elasticattr.TransactionMarks)
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

// SetElasticSpecificFieldsForLog sets fields that are not defined by OTel.
// Unlike fields from IntakeV2ToDerivedFields.go, these fields are not used by the UI.
func SetElasticSpecificFieldsForLog(event *modelpb.APMEvent, attributesMap pcommon.Map) {
	setHTTP(event.Http, attributesMap)

	// processor.event is not set for logs
	if event.Log != nil {
		putNonEmptyStr(attributesMap, elasticattr.LogLogger, event.Log.Logger)

		if event.Log.Origin != nil {
			putNonEmptyStr(attributesMap, elasticattr.LogOriginFunction, event.Log.Origin.FunctionName)
			if event.Log.Origin.File != nil {
				if event.Log.Origin.File.Line != 0 {
					attributesMap.PutInt(elasticattr.LogOriginFileLine, int64(event.Log.Origin.File.Line))
				}
				putNonEmptyStr(attributesMap, elasticattr.LogOriginFileName, event.Log.Origin.File.Name)
			}
		}
	}

	if event.Event != nil {
		putNonEmptyStr(attributesMap, elasticattr.EventAction, event.Event.Action)
		putNonEmptyStr(attributesMap, elasticattr.EventDataset, event.Event.Dataset)
		putNonEmptyStr(attributesMap, elasticattr.EventCategory, event.Event.Category)
		putNonEmptyStr(attributesMap, elasticattr.EventType, event.Event.Type)
	}

	if event.Error == nil {
		attributesMap.PutStr(elasticattr.EventKind, "event")
	}

	if event.Process != nil {
		if event.Process.Thread != nil {
			// only the process thread name is available at the log level
			putNonEmptyStr(attributesMap, elasticattr.ProcessThreadName, event.Process.Thread.Name)
		}
	}

	if event.Error != nil {
		setKeyValueMap(elasticattr.ErrorCustom, attributesMap, event.Error.Custom)

		putNonEmptyStr(attributesMap, elasticattr.ErrorMessage, event.Error.Message)
		putNonEmptyStr(attributesMap, elasticattr.ErrorType, event.Error.Type)
		putNonEmptyStr(attributesMap, elasticattr.ErrorStackTrace, event.Error.StackTrace)

		if event.Error.Log != nil {
			putNonEmptyStr(attributesMap, elasticattr.ErrorLogMessage, event.Error.Log.Message)
			putNonEmptyStr(attributesMap, elasticattr.ErrorLogLevel, event.Error.Log.Level)
			putNonEmptyStr(attributesMap, elasticattr.ErrorLogParamMessage, event.Error.Log.ParamMessage)
			putNonEmptyStr(attributesMap, elasticattr.ErrorLogLoggerName, event.Error.Log.LoggerName)
			setStackTraceList(elasticattr.ErrorLogStackTrace, attributesMap, event.Error.Log.Stacktrace)
		}
	}
}
