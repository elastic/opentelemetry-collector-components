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

package ndjsondecoder // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/ndjsondecoder"

import (
	"encoding/hex"
	"fmt"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
)

// DecodeError reads the next NDJSON line and decodes it as an APM error event.
func DecodeError(dec *NDJSONStreamDecoder) (*errorEvent, error) {
	var root errorRoot
	if err := dec.Decode(&root); err != nil {
		return nil, err
	}
	return &root.Error, nil
}

// DecodeLog reads the next NDJSON line and decodes it as an ECS/APM log event.
func DecodeLog(dec *NDJSONStreamDecoder) (*log, error) {
	var root logRoot
	if err := dec.Decode(&root); err != nil {
		return nil, err
	}
	if err := root.processNestedSource(); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
	}
	return &root.Log, nil
}

// ErrorContextService returns the per-event service override for e, if any.
func ErrorContextService(e *errorEvent) *contextService {
	if e.Context.Service == (contextService{}) {
		return nil
	}
	return &e.Context.Service
}

// LogContextService builds a contextService from the ECS log service fields in l.
// Returns nil when no service fields are set.
func LogContextService(l *log) *contextService {
	if !l.ServiceName.IsSet() && !l.ServiceVersion.IsSet() &&
		!l.ServiceEnvironment.IsSet() && !l.ServiceNodeName.IsSet() {
		return nil
	}
	return &contextService{
		Name:        l.ServiceName,
		Version:     l.ServiceVersion,
		Environment: l.ServiceEnvironment,
		Node:        contextServiceNode{Name: l.ServiceNodeName},
	}
}

// AppendError appends a plog.LogRecord for the APM error event e to sl.
// Resource-level fields (service, host, agent, user_agent, client, labels) are NOT
// written here; the stream handler owns them.
func AppendError(sl plog.ScopeLogs, e *errorEvent, _ *zap.Logger) {
	lr := sl.LogRecords().AppendEmpty()
	if e.Timestamp.IsSet() {
		lr.SetTimestamp(pcommon.Timestamp(e.Timestamp.Val.UnixNano()))
	}
	if e.TraceID.IsSet() {
		if id, err := traceIDFromHex(e.TraceID.Val); err == nil {
			lr.SetTraceID(id)
		}
	}
	if e.TransactionID.IsSet() {
		if id, err := spanIDFromHex(e.TransactionID.Val); err == nil {
			lr.SetSpanID(id)
		}
	}
	lr.Body().SetStr(errorMessage(e))

	attrs := lr.Attributes()
	attrs.PutStr(elasticattr.ProcessorEvent, "error")
	if e.Context.Service.Target.Type.IsSet() {
		attrs.PutStr(elasticattr.ServiceTargetType, e.Context.Service.Target.Type.Val)
	}
	if e.Context.Service.Target.Name.IsSet() {
		attrs.PutStr(elasticattr.ServiceTargetName, e.Context.Service.Target.Name.Val)
	}
	if e.TransactionID.IsSet() {
		attrs.PutStr(elasticattr.TransactionID, e.TransactionID.Val)
	}
	attrs.PutStr(elasticattr.ErrorID, e.ID.Val)
	if e.ParentID.IsSet() {
		attrs.PutStr(elasticattr.ParentID, e.ParentID.Val)
	}
	attrs.PutStr(elasticattr.ErrorGroupingKey, computeErrorGroupingKey(e))
	if e.Timestamp.IsSet() {
		attrs.PutInt(elasticattr.TimestampUs, e.Timestamp.Val.UnixMicro())
	}
	if e.Culprit.IsSet() {
		attrs.PutStr(elasticattr.ErrorCulprit, e.Culprit.Val)
	}
	if exceptionPresent(e.Exception) {
		setExceptionObjectArray(attrs, e.Exception)
	}
	setContextHTTPSemConv(attrs, &e.Context)
	setTransactionURLAttrs(attrs, &e.Context.Request.URL)
	setContextHTTPElastic(attrs, &e.Context)
	if len(e.Context.Custom) > 0 {
		m := attrs.PutEmptyMap(elasticattr.ErrorCustom)
		for k, v := range e.Context.Custom {
			insertAnyValue(m.PutEmpty(k), v)
		}
	}
	setErrorLogAttrs(attrs, &e.Log)
	if e.Transaction.Name.IsSet() {
		attrs.PutStr(elasticattr.TransactionName, e.Transaction.Name.Val)
	}
	if e.Transaction.Type.IsSet() {
		attrs.PutStr(elasticattr.TransactionType, e.Transaction.Type.Val)
	}
	if e.Transaction.Sampled.IsSet() && e.Transaction.Sampled.Val {
		attrs.PutBool(elasticattr.TransactionSampled, true)
	}
	// No severityText: event.Log is nil for APM errors (only error.Log is set).
}

// AppendLog appends a plog.LogRecord for the ECS/APM log event l to sl.
// Resource-level fields (service, host, agent, faas, labels) are NOT written here.
func AppendLog(sl plog.ScopeLogs, l *log, _ *zap.Logger) {
	lr := sl.LogRecords().AppendEmpty()
	if l.Timestamp.IsSet() {
		lr.SetTimestamp(pcommon.Timestamp(l.Timestamp.Val.UnixNano()))
	}
	if l.TraceID.IsSet() {
		if id, err := traceIDFromHex(l.TraceID.Val); err == nil {
			lr.SetTraceID(id)
		}
	}
	// SpanID: prefer span.id, fall back to transaction.id
	if l.SpanID.IsSet() {
		if id, err := spanIDFromHex(l.SpanID.Val); err == nil {
			lr.SetSpanID(id)
		}
	} else if l.TransactionID.IsSet() {
		if id, err := spanIDFromHex(l.TransactionID.Val); err == nil {
			lr.SetSpanID(id)
		}
	}
	lr.Body().SetStr(l.Message.Val)
	if l.Level.IsSet() {
		lr.SetSeverityText(l.Level.Val)
	}

	isECSError := l.ErrorType.IsSet() || l.ErrorMessage.IsSet() || l.ErrorStacktrace.IsSet()

	attrs := lr.Attributes()
	if isECSError {
		attrs.PutStr(elasticattr.ProcessorEvent, "error")
	}
	if l.TransactionID.IsSet() {
		attrs.PutStr(elasticattr.TransactionID, l.TransactionID.Val)
	}
	if isECSError {
		attrs.PutStr(elasticattr.ErrorGroupingKey, ecsLogErrorGroupingKey)
		if l.Timestamp.IsSet() {
			attrs.PutInt(elasticattr.TimestampUs, l.Timestamp.Val.UnixMicro())
		}
	}
	if l.Logger.IsSet() {
		attrs.PutStr(elasticattr.LogLogger, l.Logger.Val)
	}
	if l.OriginFunction.IsSet() {
		attrs.PutStr(elasticattr.LogOriginFunction, l.OriginFunction.Val)
	}
	if l.OriginFileLine.IsSet() {
		attrs.PutInt(elasticattr.LogOriginFileLine, int64(l.OriginFileLine.Val))
	}
	if l.OriginFileName.IsSet() {
		attrs.PutStr(elasticattr.LogOriginFileName, l.OriginFileName.Val)
	}
	if l.EventDataset.IsSet() {
		attrs.PutStr(elasticattr.EventDataset, l.EventDataset.Val)
	}
	if !isECSError {
		attrs.PutStr(elasticattr.EventKind, "event")
	}
	if l.ProcessThreadName.IsSet() {
		attrs.PutStr(elasticattr.ProcessThreadName, l.ProcessThreadName.Val)
	}
	if isECSError {
		if l.ErrorMessage.IsSet() {
			attrs.PutStr(elasticattr.ErrorMessage, l.ErrorMessage.Val)
		}
		if l.ErrorType.IsSet() {
			attrs.PutStr(elasticattr.ErrorType, l.ErrorType.Val)
		}
		if l.ErrorStacktrace.IsSet() {
			attrs.PutStr(elasticattr.ErrorStackTrace, l.ErrorStacktrace.Val)
		}
	}
}

// ecsLogErrorGroupingKey is the xxhash64 of empty input, used as the fixed grouping key for
// ECS log error events (error.type/message/stack_trace fields are not fed into the algorithm).
const ecsLogErrorGroupingKey = "ef46db3751d8e999"

// errorMessage returns the best available error message for an APM error event.
func errorMessage(e *errorEvent) string {
	if e.Log.Message.IsSet() && e.Log.Message.Val != "" {
		return e.Log.Message.Val
	}
	if e.Exception.Message.IsSet() {
		return e.Exception.Message.Val
	}
	return ""
}

// exceptionPresent reports whether exc has any meaningful content.
func exceptionPresent(exc errorException) bool {
	return exc.Type.IsSet() || exc.Message.IsSet() || len(exc.Cause) > 0 || len(exc.Stacktrace) > 0
}

// computeErrorGroupingKey computes error.grouping_key using the same algorithm as
// apm-data's modelprocessor.SetGroupingKey (see groupingkey.go processError).
func computeErrorGroupingKey(e *errorEvent) string {
	h := xxhash.New()
	updated := false
	excPresent := exceptionPresent(e.Exception)
	// 1. Exception types (DFS across cause tree)
	if excPresent {
		if hashExcTreeTypes(e.Exception, h) {
			updated = true
		}
	}
	// 2. Log param_message
	if e.Log.ParamMessage.IsSet() && e.Log.ParamMessage.Val != "" {
		_, _ = h.WriteString(e.Log.ParamMessage.Val)
		updated = true
	}
	// 3. Exception stacktraces (DFS)
	haveExcStk := false
	if excPresent {
		haveExcStk = hashExcTreeStacktrace(e.Exception, h)
		if haveExcStk {
			updated = true
		}
	}
	// 4. Log stacktrace (only when no exception stacktrace was found)
	if !haveExcStk && len(e.Log.Stacktrace) > 0 {
		if hashStacktraceFramesForKey(e.Log.Stacktrace, h) {
			updated = true
		}
	}
	// 5. Fallback: exception messages
	if !updated && excPresent {
		hashExcTreeMessages(e.Exception, h)
	}
	// 6. Fallback: log message
	if !updated && e.Log.Message.IsSet() {
		_, _ = h.WriteString(e.Log.Message.Val)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func hashExcTreeTypes(exc errorException, h *xxhash.Digest) bool {
	updated := false
	if exc.Type.IsSet() && exc.Type.Val != "" {
		_, _ = h.WriteString(exc.Type.Val)
		updated = true
	}
	for _, cause := range exc.Cause {
		if hashExcTreeTypes(cause, h) {
			updated = true
		}
	}
	return updated
}

func hashExcTreeStacktrace(exc errorException, h *xxhash.Digest) bool {
	updated := hashStacktraceFramesForKey(exc.Stacktrace, h)
	for _, cause := range exc.Cause {
		if hashExcTreeStacktrace(cause, h) {
			updated = true
		}
	}
	return updated
}

func hashExcTreeMessages(exc errorException, h *xxhash.Digest) {
	if exc.Message.IsSet() && exc.Message.Val != "" {
		_, _ = h.WriteString(exc.Message.Val)
	}
	for _, cause := range exc.Cause {
		hashExcTreeMessages(cause, h)
	}
}

// hashStacktraceFramesForKey hashes frame identifiers into h, matching apm-data's
// hashStacktrace (ExcludeFromGrouping is always false in our model).
func hashStacktraceFramesForKey(frames []stacktraceFrame, h *xxhash.Digest) bool {
	for _, f := range frames {
		switch {
		case f.Module.IsSet() && f.Module.Val != "":
			_, _ = h.WriteString(f.Module.Val)
		case f.Filename.IsSet() && f.Filename.Val != "":
			_, _ = h.WriteString(f.Filename.Val)
		default:
			_, _ = h.WriteString(f.Classname.Val) // may be empty; matches original behaviour
		}
		_, _ = h.WriteString(f.Function.Val) // may be empty
	}
	return len(frames) > 0
}

// setExceptionObjectArray writes the depth-first flattened exception tree into attrs
// under error.exception. The parent field is only written when an exception is not the
// immediate DFS successor of its parent.
func setExceptionObjectArray(attrs pcommon.Map, root errorException) {
	type entry struct {
		exc       errorException
		parentIdx int
	}
	var flat []entry
	var collect func(exc errorException, parentIdx int)
	collect = func(exc errorException, parentIdx int) {
		idx := len(flat)
		flat = append(flat, entry{exc, parentIdx})
		for _, cause := range exc.Cause {
			collect(cause, idx)
		}
	}
	collect(root, -1)

	sl := attrs.PutEmptySlice(elasticattr.ErrorException)
	sl.EnsureCapacity(len(flat))
	for i, e := range flat {
		fm := sl.AppendEmpty().SetEmptyMap()
		if i > e.parentIdx+1 {
			fm.PutInt(elasticattr.ErrorExceptionParent, int64(e.parentIdx))
		}
		writeExceptionFields(fm, e.exc)
	}
}

func writeExceptionFields(fm pcommon.Map, exc errorException) {
	if exc.Code.IsSet() {
		fm.PutStr(elasticattr.ErrorExceptionCode, fmt.Sprint(exc.Code.Val))
	}
	if exc.Message.IsSet() {
		fm.PutStr(elasticattr.ErrorExceptionMessage, exc.Message.Val)
	}
	if exc.Type.IsSet() {
		fm.PutStr(elasticattr.ErrorExceptionType, exc.Type.Val)
	}
	if exc.Module.IsSet() {
		fm.PutStr(elasticattr.ErrorExceptionModule, exc.Module.Val)
	}
	if exc.Handled.IsSet() {
		fm.PutBool(elasticattr.ErrorExceptionHandledField, exc.Handled.Val)
	}
	if len(exc.Attributes) > 0 {
		m := fm.PutEmptyMap(elasticattr.ErrorExceptionAttributes)
		for k, v := range exc.Attributes {
			insertAnyValue(m.PutEmpty(k), v)
		}
	}
	if len(exc.Stacktrace) > 0 {
		sl := fm.PutEmptySlice(elasticattr.ErrorExceptionStacktrace)
		sl.EnsureCapacity(len(exc.Stacktrace))
		for _, f := range exc.Stacktrace {
			appendStacktraceFrame(sl.AppendEmpty().SetEmptyMap(), f)
		}
	}
}

// setErrorLogAttrs writes error.log.* attributes when the error log sub-object has content.
func setErrorLogAttrs(attrs pcommon.Map, l *errorLog) {
	if !l.Message.IsSet() && !l.LoggerName.IsSet() && !l.Level.IsSet() &&
		!l.ParamMessage.IsSet() && len(l.Stacktrace) == 0 {
		return
	}
	if l.Message.IsSet() {
		attrs.PutStr(elasticattr.ErrorLogMessage, l.Message.Val)
	}
	// Default log level to "error" when the log sub-object is present but level is unset
	// (mirrors apm-data decoder which initialises ErrorLog{Level: "error"}).
	level := l.Level.Val
	if level == "" {
		level = "error"
	}
	attrs.PutStr(elasticattr.ErrorLogLevel, level)
	if l.ParamMessage.IsSet() {
		attrs.PutStr(elasticattr.ErrorLogParamMessage, l.ParamMessage.Val)
	}
	if l.LoggerName.IsSet() {
		attrs.PutStr(elasticattr.ErrorLogLoggerName, l.LoggerName.Val)
	}
	if len(l.Stacktrace) > 0 {
		sl := attrs.PutEmptySlice(elasticattr.ErrorLogStackTrace)
		sl.EnsureCapacity(len(l.Stacktrace))
		for _, f := range l.Stacktrace {
			appendStacktraceFrame(sl.AppendEmpty().SetEmptyMap(), f)
		}
	}
}
