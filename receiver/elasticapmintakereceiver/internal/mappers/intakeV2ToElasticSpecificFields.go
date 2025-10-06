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
	if event.Http != nil {
		if event.Http.Request != nil {
			if event.Http.Request.Body != nil {
				attributesMap.PutStr(attr.HTTPRequestBody, event.Http.Request.Body.GetStringValue())
			}
			if event.Http.Request.Id != "" {
				attributesMap.PutStr(attr.HTTPRequestID, event.Http.Request.Id)
			}
			if event.Http.Request.Referrer != "" {
				attributesMap.PutStr(attr.HTTPRequestReferrer, event.Http.Request.Referrer)
			}
		}

		if event.Http.Response != nil {
			if event.Http.Response.DecodedBodySize != nil {
				attributesMap.PutInt(attr.HTTPResponseDecodedBodySize, int64(*event.Http.Response.DecodedBodySize))
			}
			if event.Http.Response.EncodedBodySize != nil {
				attributesMap.PutInt(attr.HTTPResponseEncodedBodySize, int64(*event.Http.Response.EncodedBodySize))
			}
			if event.Http.Response.TransferSize != nil {
				attributesMap.PutInt(attr.HTTPResponseTransferSize, int64(*event.Http.Response.TransferSize))
			}
		}
	}

	if event.Span == nil {
		return
	}

	if event.Span.Db != nil {
		if event.Span.Db.Link != "" {
			attributesMap.PutStr(attr.SpanDBLink, event.Span.Db.Link)
		}
		if event.Span.Db.RowsAffected != nil {
			// SemConv db.response.returned_rows is similar, but not the same
			attributesMap.PutInt(attr.SpanDBRowsAffected, int64(*event.Span.Db.RowsAffected))
		}
		if event.Span.Db.UserName != "" {
			attributesMap.PutStr(attr.SpanDBUserName, event.Span.Db.UserName)
		}
	}

	setMessage("span", event.Span.Message, attributesMap)

	if event.Span.Composite != nil {
		compressionStrategy, ok := compressionStrategyText[event.Span.Composite.CompressionStrategy]
		if ok {
			attributesMap.PutStr(attr.SpanCompositeCompressionStrategy, compressionStrategy)
		}
		attributesMap.PutInt(attr.SpanCompositeCount, int64(event.Span.Composite.Count))
		attributesMap.PutInt(attr.SpanCompositeSum, int64(event.Span.Composite.Sum))
	}

	attributesMap.PutDouble(attr.SpanRepresentativeCount, event.Span.RepresentativeCount)

	setStackTraceList(attributesMap, event.Span.Stacktrace)
}

// setMessage sets message fields from either a span or transaction message.
func setMessage(prefix string, m *modelpb.Message, attributesMap pcommon.Map) {
	if m == nil {
		return
	}
	if m.Body != "" {
		attributesMap.PutStr(fmt.Sprintf("%s.%s", prefix, attr.MessageBody), m.Body)
	}
	if m.AgeMillis != nil {
		attributesMap.PutInt(fmt.Sprintf("%s.%s", prefix, attr.MessageAgeMs), int64(*m.AgeMillis))
	}
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
func setStackTraceList(attributesMap pcommon.Map, stacktrace []*modelpb.StacktraceFrame) {
	if len(stacktrace) == 0 {
		return
	}

	stacktraceSlice := attributesMap.PutEmptySlice(attr.SpanStacktrace)
	stacktraceSlice.EnsureCapacity(len(stacktrace))
	for _, frame := range stacktrace {
		frameMap := stacktraceSlice.AppendEmpty().SetEmptyMap()

		setKeyValueMap(attr.SpanStacktraceFrameVars, frameMap, frame.Vars)

		if frame.Lineno != nil {
			frameMap.PutInt(attr.SpanStacktraceFrameLineNumber, int64(*frame.Lineno))
		}
		if frame.Colno != nil {
			frameMap.PutInt(attr.SpanStacktraceFrameLineColumn, int64(*frame.Colno))
		}
		if frame.Filename != "" {
			frameMap.PutStr(attr.SpanStacktraceFrameFilename, frame.Filename)
		}
		if frame.Classname != "" {
			frameMap.PutStr(attr.SpanStacktraceFrameClassname, frame.Classname)
		}
		if frame.ContextLine != "" {
			frameMap.PutStr(attr.SpanStacktraceFrameLineContext, frame.ContextLine)
		}
		if frame.Module != "" {
			frameMap.PutStr(attr.SpanStacktraceFrameModule, frame.Module)
		}
		if frame.Function != "" {
			frameMap.PutStr(attr.SpanStacktraceFrameFunction, frame.Function)
		}
		if frame.AbsPath != "" {
			frameMap.PutStr(attr.SpanStacktraceFrameAbsPath, frame.AbsPath)
		}

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

		frameMap.PutBool(attr.SpanStacktraceFrameLibraryFrame, frame.LibraryFrame)
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
	// TODO: map other fields: http.*, source.*, user_agent.*, client.*

	if event.Transaction == nil {
		return
	}

	if event.Transaction.SpanCount != nil {
		if event.Transaction.SpanCount.Started != nil {
			attributesMap.PutInt(attr.TransactionSpanCountStarted, int64(*event.Transaction.SpanCount.Started))
		}
		if event.Transaction.SpanCount.Dropped != nil {
			attributesMap.PutInt(attr.TransactionSpanCountDropped, int64(*event.Transaction.SpanCount.Dropped))
		}
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
		if stat.DestinationServiceResource != "" {
			statMap.PutStr(attr.TransactionDroppedSpansStatsDestinationServiceResource, stat.DestinationServiceResource)
		}
		if stat.Outcome != "" {
			statMap.PutStr(attr.TransactionDroppedSpansStatsOutcome, stat.Outcome)
		}
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
		if event.Cloud.ProjectId != "" {
			attributesMap.PutStr(attr.CloudProjectID, event.Cloud.ProjectId)
		}
		if event.Cloud.ProjectName != "" {
			attributesMap.PutStr(attr.CloudProjectName, event.Cloud.ProjectName)
		}
	}

	if event.Faas != nil {
		if event.Faas.TriggerRequestId != "" {
			attributesMap.PutStr(attr.TriggerRequestId, event.Faas.TriggerRequestId)
		}
		if event.Faas.Execution != "" {
			attributesMap.PutStr(attr.FaaSExecution, event.Faas.Execution)
		}
	}

	if event.Agent != nil {
		if event.Agent.EphemeralId != "" {
			attributesMap.PutStr(attr.AgentEphemeralId, event.Agent.EphemeralId)
		}
		if event.Agent.ActivationMethod != "" {
			attributesMap.PutStr(attr.AgentActivationMethod, event.Agent.ActivationMethod)
		}
	}

	if event.Service != nil {
		if event.Service.Language != nil {
			if event.Service.Language.Name != "" {
				attributesMap.PutStr(attr.ServiceLanguageName, event.Service.Language.Name)
			}
			if event.Service.Language.Version != "" {
				attributesMap.PutStr(attr.ServiceLanguageVersion, event.Service.Language.Version)
			}
		}
		if event.Service.Framework != nil {
			if event.Service.Framework.Name != "" {
				attributesMap.PutStr(attr.ServiceFrameworkName, event.Service.Framework.Name)
			}
			if event.Service.Framework.Version != "" {
				attributesMap.PutStr(attr.ServiceFrameworkVersion, event.Service.Framework.Version)
			}
		}
		if event.Service.Runtime != nil {
			if event.Service.Runtime.Name != "" {
				attributesMap.PutStr(attr.ServiceRuntimeName, event.Service.Runtime.Name)
			}
			if event.Service.Runtime.Version != "" {
				attributesMap.PutStr(attr.ServiceRuntimeVersion, event.Service.Runtime.Version)
			}
		}
		if event.Service.Origin != nil {
			if event.Service.Origin.Id != "" {
				attributesMap.PutStr(attr.ServiceOriginId, event.Service.Origin.Id)
			}
			if event.Service.Origin.Name != "" {
				attributesMap.PutStr(attr.ServiceOriginName, event.Service.Origin.Name)
			}
			if event.Service.Origin.Version != "" {
				attributesMap.PutStr(attr.ServiceOriginVersion, event.Service.Origin.Version)
			}
		}
		if event.Service.Target != nil {
			if event.Service.Target.Name != "" {
				attributesMap.PutStr(attr.ServiceTargetName, event.Service.Target.Name)
			}
			if event.Service.Target.Type != "" {
				attributesMap.PutStr(attr.ServiceTargetType, event.Service.Target.Type)
			}
		}
	}

	if event.Host != nil {
		if event.Host.Os != nil {
			if event.Host.Os.Platform != "" {
				attributesMap.PutStr(attr.HostOSPlatform, event.Host.Os.Platform)
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
// Note: modelpb.Events supports single and slice label values,
// but the apm data model only support single value labels, so the slice values are ignored.
// See schema:
// - https://github.com/elastic/apm-data/blob/main/input/elasticapm/internal/modeldecoder/v2/model.go#L75
// - https://github.com/elastic/apm-data/blob/main/input/elasticapm/internal/modeldecoder/v2/model.go#L433
// - https://github.com/elastic/apm-data/blob/main/input/elasticapm/internal/modeldecoder/v2/model.go#L969
//
// The apm data library logic will take care of overwriting metadata labels with event labels when decoding
// the input to modelpb.APMEvent, so we simply copy all labels from the event here.
func setLabels(event *modelpb.APMEvent, attributesMap pcommon.Map) {
	for key, labelValue := range event.Labels {
		if key != "" && labelValue != nil && labelValue.Value != "" {
			attrKey := "labels." + key
			attributesMap.PutStr(attrKey, labelValue.Value)
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
	// processor.event is not set for logs
	if event.Log != nil {
		if event.Log.Logger != "" {
			attributesMap.PutStr(attr.LogLogger, event.Log.Logger)
		}

		if event.Log.Origin != nil {
			if event.Log.Origin.FunctionName != "" {
				attributesMap.PutStr(attr.LogOriginFunction, event.Log.Origin.FunctionName)
			}
			if event.Log.Origin.File != nil {
				if event.Log.Origin.File.Line != 0 {
					attributesMap.PutInt(attr.LogOriginFileLine, int64(event.Log.Origin.File.Line))
				}
				if event.Log.Origin.File.Name != "" {
					attributesMap.PutStr(attr.LogOriginFileName, event.Log.Origin.File.Name)
				}
			}
		}
	}

	if event.Event != nil {
		if event.Event.Action != "" {
			attributesMap.PutStr(attr.EventAction, event.Event.Action)
		}
		if event.Event.Dataset != "" {
			attributesMap.PutStr(attr.EventDataset, event.Event.Dataset)
		}
		if event.Event.Category != "" {
			attributesMap.PutStr(attr.EventCategory, event.Event.Category)
		}
		if event.Event.Type != "" {
			attributesMap.PutStr(attr.EventType, event.Event.Type)
		}
	}

	if event.Error == nil {
		attributesMap.PutStr(attr.EventKind, "event")
	}

	if event.Process != nil {
		if event.Process.Thread != nil {
			if event.Process.Thread.Name != "" {
				attributesMap.PutStr(attr.ProcessThreadName, event.Process.Thread.Name)
			}
		}
		if event.Process.Title != "" {
			attributesMap.PutStr(attr.ProcessTitle, event.Process.Title)
		}
	}

	if event.Session != nil {
		if event.Session.Id != "" {
			attributesMap.PutStr(attr.SessionID, event.Session.Id)
		}
	}

	setKeyValueMap(attr.ErrorCustom, attributesMap, event.Error.Custom)
}
