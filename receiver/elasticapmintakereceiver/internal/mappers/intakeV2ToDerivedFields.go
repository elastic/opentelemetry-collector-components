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
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	attr "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal"
)

// SetDerivedFieldsForTransaction sets fields that are NOT part of OTel for transactions. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForTransaction(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutStr(elasticattr.ProcessorEvent, "transaction")
	attributes.PutInt(elasticattr.TransactionDurationUs, int64(event.Event.Duration/1_000))

	setCommonDerivedRecordAttributes(event, attributes)

	if event.Transaction == nil {
		return
	}

	putNonEmptyStr(attributes, elasticattr.TransactionName, event.Transaction.Name)
	putNonEmptyStr(attributes, elasticattr.TransactionType, event.Transaction.Type)
	putNonEmptyStr(attributes, elasticattr.TransactionResult, event.Transaction.Result)
	attributes.PutBool(elasticattr.TransactionSampled, event.Transaction.Sampled)
}

// setCommonDerivedRecordAttributes sets common attributes which are shared at the record
// level for span, transaction, and error events.
func setCommonDerivedRecordAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Transaction != nil {
		putNonEmptyStr(attributes, elasticattr.TransactionID, event.Transaction.Id)
	}

	if event.Service != nil && event.Service.Target != nil {
		attributes.PutStr(elasticattr.ServiceTargetType, event.Service.Target.Type)
		attributes.PutStr(elasticattr.ServiceTargetName, event.Service.Target.Name)
	}
}

// SetDerivedFieldsForSpan sets fields that are NOT part of OTel for spans. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForSpan(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutStr(elasticattr.ProcessorEvent, "span")
	attributes.PutInt(elasticattr.SpanDurationUs, int64(event.Event.Duration/1_000))

	setCommonDerivedRecordAttributes(event, attributes)

	if event.Span == nil {
		return
	}

	attributes.PutStr("span.id", event.Span.Id)

	putNonEmptyStr(attributes, elasticattr.SpanName, event.Span.Name)
	putNonEmptyStr(attributes, elasticattr.SpanType, event.Span.Type)
	putNonEmptyStr(attributes, elasticattr.SpanSubtype, event.Span.Subtype)
	putNonEmptyStr(attributes, "span.action", event.Span.Action)

	putPtrBool(attributes, "span.sync", event.Span.Sync)

	if event.Span.DestinationService != nil {
		putNonEmptyStr(attributes, elasticattr.SpanDestinationServiceResource, event.Span.DestinationService.Resource)
	}
}

// SetDerivedResourceAttributes sets resource fields that are NOT part of OTel. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedResourceAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Agent != nil {
		attributes.PutStr(elasticattr.AgentName, event.Agent.Name)
		attributes.PutStr(elasticattr.AgentVersion, event.Agent.Version)
	}

	if event.Service != nil {
		if event.Service.Language != nil {
			putNonEmptyStr(attributes, attr.ServiceLanguageName, event.Service.Language.Name)
			putNonEmptyStr(attributes, attr.ServiceLanguageVersion, event.Service.Language.Version)
		}
	}
}

// SetDerivedFieldsForMetrics sets fields that are NOT part of OTel for metrics. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForMetrics(attributes pcommon.Map) {
	attributes.PutStr(elasticattr.ProcessorEvent, "metric")
}

// SetDerivedFieldsCommon sets shared fields that are NOT part of OTel for multipe signals. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsCommon(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutInt(elasticattr.TimestampUs, int64(event.Timestamp/1_000))

	outcome := event.GetEvent().GetOutcome()
	if strings.EqualFold(outcome, "success") {
		attributes.PutStr(elasticattr.EventOutcome, "success")
	} else if strings.EqualFold(outcome, "failure") {
		attributes.PutStr(elasticattr.EventOutcome, "failure")
	} else {
		attributes.PutStr(elasticattr.EventOutcome, "unknown")
	}
}

// SetDerivedFieldsForError sets fields that are NOT part of OTel for errors. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForError(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutStr(elasticattr.ProcessorEvent, "error")

	setCommonDerivedRecordAttributes(event, attributes)

	if event.Error == nil {
		return
	}

	putNonEmptyStr(attributes, elasticattr.ErrorID, event.Error.Id)
	putNonEmptyStr(attributes, elasticattr.ParentID, event.ParentId)

	attributes.PutStr(elasticattr.ErrorGroupingKey, event.Error.GroupingKey)
	attributes.PutInt(elasticattr.TimestampUs, int64(event.Timestamp/1_000))

	if event.Error.Culprit != "" {
		attributes.PutStr(attr.ErrorCulprit, event.Error.Culprit)
	}

	if event.Error.Exception != nil {
		// Create error.exception as an array of exception objects, following the apm-data JSON schema
		// in https://github.com/elastic/apm-data/blob/main/model/modeljson/internal/error.go
		setExceptionObjectArray(event.Error.Exception, attributes)
	}
}

// exceptionWithParent holds an exception, its index, and its parent index in the flattened array.
// A parentIdx of -1 indicates no parent (root exception).
type exceptionWithParent struct {
	exception *modelpb.Exception
	index     int
	parentIdx int
}

// setExceptionObjectArray creates `error.exception` as an array of exception objects,
// following the apm-data JSON schema. Each exception is a structured object.
// The exception tree is flattened depth-first with root exception first, followed by nested causes.
func setExceptionObjectArray(exc *modelpb.Exception, attributes pcommon.Map) {
	// Collect all exceptions (root + causes) into a flat list with parent indices
	// Root exception has parentIdx=-1 (no parent) and startIdx=0
	exceptions := collectExceptions(exc, -1, 0)

	if len(exceptions) == 0 {
		return
	}

	exceptionSlice := attributes.PutEmptySlice(attr.ErrorException)
	exceptionSlice.EnsureCapacity(len(exceptions))

	for _, e := range exceptions {
		exceptionMap := exceptionSlice.AppendEmpty().SetEmptyMap()
		setExceptionObject(e.exception, exceptionMap, e.index, e.parentIdx)
	}
}

// collectExceptions recursively collects all exceptions from the tree into a flat list.
// The root exception is collected first, followed by nested causes (depth-first).
// Each exception is paired with its own index and parent's index in the resulting array.
// startIdx is the index that the current exception will have in the final flattened array.
func collectExceptions(e *modelpb.Exception, parentIdx int, startIdx int) []exceptionWithParent {
	result := []exceptionWithParent{{exception: e, index: startIdx, parentIdx: parentIdx}}
	myIdx := startIdx       // This exception's index in the flattened array
	nextIdx := startIdx + 1 // Next available index for children

	for _, cause := range e.Cause {
		children := collectExceptions(cause, myIdx, nextIdx)
		nextIdx += len(children) // Update next available index
		result = append(result, children...)
	}
	return result
}

// setExceptionObject populates a single exception object map with all its fields.
// index is the exception's position in the flattened array.
// parentIdx is the index of the parent exception in the flattened array, or -1 for root.
func setExceptionObject(e *modelpb.Exception, exceptionMap pcommon.Map, index int, parentIdx int) {
	// Set parent index for chained exceptions only when necessary.
	// Following apm-data logic: the parent field is only set when the exception
	// is NOT immediately after its parent in the array (index > parentIdx + 1).
	// If the exception directly follows its parent, the implicit rule is that
	// the preceding exception is the parent.
	if index > parentIdx+1 {
		exceptionMap.PutInt(attr.ErrorExceptionParent, int64(parentIdx))
	}

	if e.Code != "" {
		exceptionMap.PutStr(attr.ErrorExceptionCode, e.Code)
	}
	if e.Message != "" {
		exceptionMap.PutStr(attr.ErrorExceptionMessage, e.Message)
	}
	if e.Type != "" {
		exceptionMap.PutStr(attr.ErrorExceptionType, e.Type)
	}
	if e.Module != "" {
		exceptionMap.PutStr(attr.ErrorExceptionModule, e.Module)
	}
	if e.Handled != nil {
		exceptionMap.PutBool(attr.ErrorExceptionHandled, *e.Handled)
	}

	// Set attributes if present
	if len(e.Attributes) > 0 {
		attrMap := exceptionMap.PutEmptyMap(attr.ErrorExceptionAttributes)
		attrMap.EnsureCapacity(len(e.Attributes))
		for _, kv := range e.Attributes {
			if kv.Key != "" && kv.Value != nil {
				insertValue(attrMap.PutEmpty(kv.Key), kv.Value)
			}
		}
	}

	// Set stacktrace as array of frame objects
	if len(e.Stacktrace) > 0 {
		setExceptionStacktrace(exceptionMap, e.Stacktrace)
	}
}

// setExceptionStacktrace creates the stacktrace array of frame objects for an exception
func setExceptionStacktrace(exceptionMap pcommon.Map, frames []*modelpb.StacktraceFrame) {
	stacktraceSlice := exceptionMap.PutEmptySlice(attr.ErrorExceptionStacktrace)
	stacktraceSlice.EnsureCapacity(len(frames))

	for _, frame := range frames {
		frameMap := stacktraceSlice.AppendEmpty().SetEmptyMap()

		// Note: ExcludeFromGrouping does not have an 'omitempty' json tag in the apm-data model
		// so we always set it to match the existing behavior.
		frameMap.PutBool(attr.ErrorExceptionStacktraceExcludeFromGrouping, frame.ExcludeFromGrouping)

		if frame.AbsPath != "" {
			frameMap.PutStr(attr.ErrorExceptionStacktraceAbsPath, frame.AbsPath)
		}
		if frame.Filename != "" {
			frameMap.PutStr(attr.ErrorExceptionStacktraceFilename, frame.Filename)
		}
		if frame.Classname != "" {
			frameMap.PutStr(attr.ErrorExceptionStacktraceClassname, frame.Classname)
		}
		if frame.Function != "" {
			frameMap.PutStr(attr.ErrorExceptionStacktraceFunction, frame.Function)
		}
		if frame.Module != "" {
			frameMap.PutStr(attr.ErrorExceptionStacktraceModule, frame.Module)
		}
		if frame.LibraryFrame {
			frameMap.PutBool(attr.ErrorExceptionStacktraceLibraryFrame, frame.LibraryFrame)
		}

		// Set line info as nested object
		if frame.Lineno != nil || frame.Colno != nil || frame.ContextLine != "" {
			if frame.Lineno != nil {
				frameMap.PutInt(attr.ErrorExceptionStacktraceLineNumber, int64(*frame.Lineno))
			}
			if frame.Colno != nil {
				frameMap.PutInt(attr.ErrorExceptionStacktraceLineColumn, int64(*frame.Colno))
			}
			if frame.ContextLine != "" {
				frameMap.PutStr(attr.ErrorExceptionStacktraceLineContext, frame.ContextLine)
			}
		}

		// Set context pre/post
		if len(frame.PreContext) > 0 {
			preSlice := frameMap.PutEmptySlice(attr.ErrorExceptionStacktraceContextPre)
			preSlice.EnsureCapacity(len(frame.PreContext))
			for _, pre := range frame.PreContext {
				preSlice.AppendEmpty().SetStr(pre)
			}
		}
		if len(frame.PostContext) > 0 {
			postSlice := frameMap.PutEmptySlice(attr.ErrorExceptionStacktraceContextPost)
			postSlice.EnsureCapacity(len(frame.PostContext))
			for _, post := range frame.PostContext {
				postSlice.AppendEmpty().SetStr(post)
			}
		}

		// Set vars if present
		if len(frame.Vars) > 0 {
			varsMap := frameMap.PutEmptyMap(attr.ErrorExceptionStacktraceVars)
			varsMap.EnsureCapacity(len(frame.Vars))
			for _, kv := range frame.Vars {
				if kv.Key != "" && kv.Value != nil {
					insertValue(varsMap.PutEmpty(kv.Key), kv.Value)
				}
			}
		}
	}
}

// SetDerivedFieldsForLog sets fields that are NOT part of OTel for logs. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForLog(event *modelpb.APMEvent, attributes pcommon.Map) {
	setCommonDerivedRecordAttributes(event, attributes)
}
