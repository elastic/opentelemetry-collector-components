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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	xxhash "github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
)

// reDestResource matches the destination service resource format "type" or "type/name".
var reDestResource = regexp.MustCompile(`^([a-z0-9]+)(?:/(\w+))?$`)

// DecodeSpan reads the next NDJSON line from dec and decodes it as a span event.
func DecodeSpan(dec *NDJSONStreamDecoder) (*span, error) {
	var root spanRoot
	if err := dec.Decode(&root); err != nil {
		return nil, err
	}
	if err := root.validate(); err != nil {
		return nil, ValidationError{err: fmt.Errorf("validation error: %w", err)}
	}
	return &root.Span, nil
}

// DecodeTransaction reads the next NDJSON line from dec and decodes it as a transaction event.
func DecodeTransaction(dec *NDJSONStreamDecoder) (*transaction, error) {
	var root transactionRoot
	if err := dec.Decode(&root); err != nil {
		return nil, err
	}
	if err := root.validate(); err != nil {
		return nil, ValidationError{err: fmt.Errorf("validation error: %w", err)}
	}
	return &root.Transaction, nil
}

// SpanContextService returns the per-event service context for a span, or nil if unset.
// Used by the stream handler for per-event resource fingerprinting.
func SpanContextService(sp *span) *contextService {
	if sp.Context.Service == (contextService{}) {
		return nil
	}
	svc := sp.Context.Service
	return &svc
}

// TransactionContextService returns the per-event service context for a transaction, or nil if unset.
func TransactionContextService(tx *transaction) *contextService {
	if tx.Context.Service == (contextService{}) {
		return nil
	}
	svc := tx.Context.Service
	return &svc
}

// AppendSpan appends a span to ss and populates all attributes.
// txStart is the parent transaction's start timestamp (used only when sp.Timestamp is unset).
func AppendSpan(ss ptrace.ScopeSpans, sp *span, txStart pcommon.Timestamp, logger *zap.Logger) {
	s := ss.Spans().AppendEmpty()

	// Timestamps
	var startNano uint64
	if sp.Timestamp.IsSet() {
		startNano = uint64(sp.Timestamp.Val.UnixNano())
	} else {
		// sp.Start is relative to txStart in milliseconds (RUM only).
		// Non-RUM agents always set an explicit timestamp, so txStart=0 has no
		// practical effect today. If RUM support is added, the caller must pass
		// the request-receive time as txStart (matching apm-data's approach of
		// baseEvent.Timestamp + start*ms) rather than the parent transaction
		// timestamp, since that is not tracked in the stream.
		startNano = uint64(txStart) + uint64(sp.Start.Val*1e6)
	}
	durationNano := uint64(sp.Duration.Val * 1e6)
	s.SetStartTimestamp(pcommon.Timestamp(startNano))
	s.SetEndTimestamp(pcommon.Timestamp(startNano + durationNano))

	// IDs
	if traceID, err := traceIDFromHex(sp.TraceID.Val); err == nil {
		s.SetTraceID(traceID)
	} else {
		logger.Error("ndjsondecoder: failed to parse span trace_id", zap.String("trace_id", sp.TraceID.Val))
	}
	if spanID, err := spanIDFromHex(sp.ID.Val); err == nil {
		s.SetSpanID(spanID)
	} else {
		logger.Error("ndjsondecoder: failed to parse span id", zap.String("span_id", sp.ID.Val))
	}
	if sp.ParentID.IsSet() {
		if parentID, err := spanIDFromHex(sp.ParentID.Val); err == nil {
			s.SetParentSpanID(parentID)
		} else {
			logger.Error("ndjsondecoder: failed to parse span parent_id", zap.String("parent_id", sp.ParentID.Val))
		}
	}

	// Kind from OTel span_kind; when OTel is set but no span_kind, derive from span type
	kind := mapSpanKind(sp.OTel.SpanKind.Val)
	if sp.OTel.IsSet() && kind == ptrace.SpanKindUnspecified {
		switch sp.Type.Val {
		case "db", "external", "storage":
			kind = ptrace.SpanKindClient
		default:
			kind = ptrace.SpanKindInternal
		}
	}
	s.SetKind(kind)

	// Name
	s.SetName(sp.Name.Val)

	// Compute outcome: derive from HTTP status when not explicitly set (mirrors apm-data v2 decoder)
	outcome := sp.Outcome.Val
	if !sp.Outcome.IsSet() {
		sc := 0
		if sp.Context.HTTP.Response.StatusCode.IsSet() {
			sc = sp.Context.HTTP.Response.StatusCode.Val
		} else if sp.Context.HTTP.StatusCode.IsSet() {
			sc = sp.Context.HTTP.StatusCode.Val
		}
		if sc != 0 {
			if sc >= http.StatusBadRequest {
				outcome = "failure"
			} else {
				outcome = "success"
			}
		} else {
			outcome = "unknown"
		}
		// OTel spans without explicit failure default to success (mirrors transaction OTel handling)
		if sp.OTel.IsSet() && outcome == "unknown" {
			outcome = "success"
		}
	}

	// Status
	setSpanStatus(s, outcome)

	// TraceState from representative count
	repCount := spanRepresentativeCount(sp.SampleRate)
	if repCount > 0 && repCount != 1 {
		if tv := probabilityToTValue(1.0 / repCount); tv != "" {
			s.TraceState().FromRaw(fmt.Sprintf("ot=th:%s", tv))
		}
	}

	// Span links
	for _, link := range sp.Links {
		sl := s.Links().AppendEmpty()
		if tid, err := traceIDFromHex(link.TraceID.Val); err == nil {
			sl.SetTraceID(tid)
		}
		if sid, err := spanIDFromHex(link.SpanID.Val); err == nil {
			sl.SetSpanID(sid)
		}
	}

	attrs := s.Attributes()

	// Derived fields (mirroring SetDerivedFieldsForSpan)
	attrs.PutInt(elasticattr.TimestampUs, int64(startNano/1000))
	setOutcomeAttr(attrs, outcome)
	attrs.PutStr(elasticattr.ProcessorEvent, "span")
	attrs.PutInt(elasticattr.SpanDurationUs, int64(sp.Duration.Val*1000))
	if sp.TransactionID.IsSet() {
		attrs.PutStr(elasticattr.TransactionID, sp.TransactionID.Val)
	}
	// service.target: use explicit value when set; derive from destination.service.resource otherwise
	if sp.Context.Service.Target.Type.IsSet() || sp.Context.Service.Target.Name.IsSet() {
		attrs.PutStr(elasticattr.ServiceTargetType, sp.Context.Service.Target.Type.Val)
		attrs.PutStr(elasticattr.ServiceTargetName, sp.Context.Service.Target.Name.Val)
	} else if sp.Context.Destination.Service.Resource.IsSet() {
		typ, name := targetFromDestResource(sp.Context.Destination.Service.Resource.Val)
		attrs.PutStr(elasticattr.ServiceTargetType, typ)
		attrs.PutStr(elasticattr.ServiceTargetName, name)
	}
	attrs.PutStr(elasticattr.SpanID, sp.ID.Val)
	if sp.Name.IsSet() {
		attrs.PutStr(elasticattr.SpanName, sp.Name.Val)
	}

	// Type / subtype / action (may need dot-splitting); default to "unknown" when absent
	typ, sub, action := parseSpanType(sp)
	if typ == "" {
		typ = "unknown"
	}
	attrs.PutStr(elasticattr.SpanType, typ)
	if sub != "" {
		attrs.PutStr(elasticattr.SpanSubtype, sub)
	}
	if action != "" {
		attrs.PutStr(elasticattr.SpanAction, action)
	}

	if sp.Sync.IsSet() {
		attrs.PutBool("span.sync", sp.Sync.Val)
	}
	if sp.Context.Destination.Service.Resource.IsSet() {
		attrs.PutStr(elasticattr.SpanDestinationServiceResource, sp.Context.Destination.Service.Resource.Val)
	}

	// SemConv: HTTP
	setSpanHTTPSemConv(attrs, sp)

	// SemConv: DB
	if sp.Context.Database.Type.IsSet() {
		attrs.PutStr(string(semconv.DBSystemKey), sp.Context.Database.Type.Val)
	}
	if sp.Context.Database.Instance.IsSet() {
		attrs.PutStr(string(semconv.DBNamespaceKey), sp.Context.Database.Instance.Val)
	}
	if sp.Context.Database.Statement.IsSet() {
		attrs.PutStr(string(semconv.DBQueryTextKey), sp.Context.Database.Statement.Val)
	}

	// SemConv: Messaging
	if sp.Context.Message.Queue.Name.IsSet() || sp.Context.Message.Body.IsSet() || sp.Context.Message.RoutingKey.IsSet() {
		if sub != "" {
			attrs.PutStr(string(semconv.MessagingSystemKey), sub)
		}
		if action != "" {
			attrs.PutStr(string(semconv.MessagingOperationNameKey), action)
		}
		if sp.Context.Message.Queue.Name.IsSet() {
			attrs.PutStr(string(semconv.MessagingDestinationNameKey), sp.Context.Message.Queue.Name.Val)
		}
	}

	// SemConv: Destination
	if sp.Context.Destination.Address.IsSet() {
		attrs.PutStr(string(semconv.DestinationAddressKey), sp.Context.Destination.Address.Val)
	}
	if sp.Context.Destination.Port.IsSet() {
		attrs.PutInt(string(semconv.DestinationPortKey), int64(sp.Context.Destination.Port.Val))
	}

	// Elastic-specific: child IDs
	if len(sp.ChildIDs) > 0 {
		sl := attrs.PutEmptySlice(elasticattr.SpanChildID)
		sl.EnsureCapacity(len(sp.ChildIDs))
		for _, id := range sp.ChildIDs {
			sl.AppendEmpty().SetStr(id)
		}
	}

	// Elastic-specific: HTTP response headers + request body/ID
	if sp.Context.HTTP.Response.Headers.IsSet() {
		setHTTPHeadersMap(elasticattr.HTTPResponseHeaders, attrs, sp.Context.HTTP.Response.Headers.Val)
	}
	if sp.Context.HTTP.Response.DecodedBodySize.IsSet() {
		attrs.PutInt(elasticattr.HTTPResponseDecodedBodySize, int64(sp.Context.HTTP.Response.DecodedBodySize.Val))
	}
	if sp.Context.HTTP.Response.TransferSize.IsSet() {
		attrs.PutInt(elasticattr.HTTPResponseTransferSize, int64(sp.Context.HTTP.Response.TransferSize.Val))
	}
	if sp.Context.HTTP.Request.Body.IsSet() {
		insertAnyValue(attrs.PutEmpty(elasticattr.HTTPRequestBodyOriginal), sp.Context.HTTP.Request.Body.Val)
	}
	if sp.Context.HTTP.Request.ID.IsSet() {
		attrs.PutStr(elasticattr.HTTPRequestID, sp.Context.HTTP.Request.ID.Val)
	}

	// Elastic-specific: DB extras
	if sp.Context.Database.Link.IsSet() {
		attrs.PutStr(elasticattr.SpanDBLink, sp.Context.Database.Link.Val)
	}
	if sp.Context.Database.RowsAffected.IsSet() {
		attrs.PutInt(elasticattr.SpanDBRowsAffected, int64(sp.Context.Database.RowsAffected.Val))
	}
	if sp.Context.Database.User.IsSet() {
		attrs.PutStr(elasticattr.SpanDBUserName, sp.Context.Database.User.Val)
	}

	// Elastic-specific: message extras
	setSpanMessageAttrs("span", attrs, sp.Context.Message)

	// Elastic-specific: composite
	if sp.Composite.CompressionStrategy.IsSet() {
		attrs.PutStr(elasticattr.SpanCompositeCompressionStrategy, sp.Composite.CompressionStrategy.Val)
		attrs.PutInt(elasticattr.SpanCompositeCount, int64(sp.Composite.Count.Val))
		attrs.PutInt(elasticattr.SpanCompositeSumUs, int64(sp.Composite.Sum.Val*1000))
	}

	// Elastic-specific: destination service name/type.
	// Old pipeline wrote name and type as empty strings whenever destination.service was present.
	if sp.Context.Destination.Service.Resource.IsSet() ||
		sp.Context.Destination.Service.Name.IsSet() ||
		sp.Context.Destination.Service.Type.IsSet() {
		attrs.PutStr(elasticattr.SpanDestinationServiceName, sp.Context.Destination.Service.Name.Val)
		attrs.PutStr(elasticattr.SpanDestinationServiceType, sp.Context.Destination.Service.Type.Val)
	}

	// span.representative_count is always written
	attrs.PutDouble(elasticattr.SpanRepresentativeCount, repCount)

	// Stacktrace
	if len(sp.Stacktrace) > 0 {
		setStackTraceFrames(elasticattr.SpanStacktrace, attrs, sp.Stacktrace)
	}
}

// AppendTransaction appends a transaction to ss and returns the created span (for DSS use).
func AppendTransaction(ss ptrace.ScopeSpans, tx *transaction, logger *zap.Logger) ptrace.Span {
	s := ss.Spans().AppendEmpty()

	// Timestamps: use 0 (Unix epoch) when timestamp is absent, matching old pipeline behaviour
	var startNano uint64
	if !tx.Timestamp.Val.IsZero() {
		startNano = uint64(tx.Timestamp.Val.UnixNano())
	}
	durationNano := uint64(tx.Duration.Val * 1e6)
	s.SetStartTimestamp(pcommon.Timestamp(startNano))
	s.SetEndTimestamp(pcommon.Timestamp(startNano + durationNano))

	// IDs: transaction spans use tx.ID as the span ID
	if traceID, err := traceIDFromHex(tx.TraceID.Val); err == nil {
		s.SetTraceID(traceID)
	} else {
		logger.Error("ndjsondecoder: failed to parse transaction trace_id", zap.String("trace_id", tx.TraceID.Val))
	}
	if spanID, err := spanIDFromHex(tx.ID.Val); err == nil {
		s.SetSpanID(spanID)
	} else {
		logger.Error("ndjsondecoder: failed to parse transaction id", zap.String("id", tx.ID.Val))
	}
	if tx.ParentID.IsSet() {
		if parentID, err := spanIDFromHex(tx.ParentID.Val); err == nil {
			s.SetParentSpanID(parentID)
		} else {
			logger.Error("ndjsondecoder: failed to parse transaction parent_id", zap.String("parent_id", tx.ParentID.Val))
		}
	}

	// Compute outcome: derive from HTTP response status when not explicitly set
	outcome := tx.Outcome.Val
	if !tx.Outcome.IsSet() {
		if tx.Context.Response.StatusCode.IsSet() {
			sc := tx.Context.Response.StatusCode.Val
			if sc >= http.StatusInternalServerError {
				outcome = "failure"
			} else {
				outcome = "success"
			}
		} else {
			outcome = "unknown"
		}
	}
	// result may be overridden by OTel processing below
	result := tx.Result.Val
	// OTel: span kind from transaction type, outcome/result defaults when outcome is unknown
	kind := mapSpanKind(tx.OTel.SpanKind.Val)
	if tx.OTel.IsSet() {
		// Only derive kind from transaction type when span_kind was absent; an unrecognised
		// value must not be overridden (mirrors apm-data mapOTelAttributesTransaction).
		if !tx.OTel.SpanKind.IsSet() {
			switch tx.Type.Val {
			case "request":
				kind = ptrace.SpanKindServer
			case "messaging":
				kind = ptrace.SpanKindConsumer
			default:
				kind = ptrace.SpanKindInternal
			}
		}
		if outcome == "unknown" {
			outcome = "success"
			if result == "" {
				result = "Success"
			}
		}
	}

	s.SetKind(kind)
	s.SetName(tx.Name.Val)
	setSpanStatus(s, outcome)

	repCount := spanRepresentativeCount(tx.SampleRate)
	if repCount > 0 && repCount != 1 {
		if tv := probabilityToTValue(1.0 / repCount); tv != "" {
			s.TraceState().FromRaw(fmt.Sprintf("ot=th:%s", tv))
		}
	}

	for _, link := range tx.Links {
		sl := s.Links().AppendEmpty()
		if tid, err := traceIDFromHex(link.TraceID.Val); err == nil {
			sl.SetTraceID(tid)
		}
		if sid, err := spanIDFromHex(link.SpanID.Val); err == nil {
			sl.SetSpanID(sid)
		}
	}

	attrs := s.Attributes()

	// Derived fields
	attrs.PutInt(elasticattr.TimestampUs, int64(startNano/1000))
	setOutcomeAttr(attrs, outcome)
	attrs.PutStr(elasticattr.ProcessorEvent, "transaction")
	attrs.PutInt(elasticattr.TransactionDurationUs, int64(tx.Duration.Val*1000))
	attrs.PutStr(elasticattr.TransactionID, tx.ID.Val)
	if tx.Context.Service.Target.Type.IsSet() || tx.Context.Service.Target.Name.IsSet() {
		attrs.PutStr(elasticattr.ServiceTargetType, tx.Context.Service.Target.Type.Val)
		attrs.PutStr(elasticattr.ServiceTargetName, tx.Context.Service.Target.Name.Val)
	}
	if tx.Name.IsSet() {
		attrs.PutStr(elasticattr.TransactionName, tx.Name.Val)
	}
	// Default type to "unknown" when absent/empty, matching apm-data decoder behaviour
	txType := tx.Type.Val
	if txType == "" {
		txType = "unknown"
	}
	attrs.PutStr(elasticattr.TransactionType, txType)
	if !tx.Sampled.IsSet() || tx.Sampled.Val {
		attrs.PutBool(elasticattr.TransactionSampled, true)
	}
	if result != "" {
		attrs.PutStr(elasticattr.TransactionResult, result)
	}

	// SemConv: HTTP for transaction
	setContextHTTPSemConv(attrs, &tx.Context)
	setTransactionURLAttrs(attrs, &tx.Context.Request.URL)

	// SemConv: Messaging destination
	if tx.Context.Message.Queue.Name.IsSet() {
		attrs.PutStr(string(semconv.MessagingDestinationNameKey), tx.Context.Message.Queue.Name.Val)
	}

	// Elastic-specific: HTTP
	setContextHTTPElastic(attrs, &tx.Context)

	// Elastic-specific: span count
	if tx.SpanCount.Started.IsSet() {
		attrs.PutInt(elasticattr.TransactionSpanCountStarted, int64(tx.SpanCount.Started.Val))
	}
	if tx.SpanCount.Dropped.IsSet() {
		attrs.PutInt(elasticattr.TransactionSpanCountDropped, int64(tx.SpanCount.Dropped.Val))
	}

	// Elastic-specific: user experience
	if tx.UserExperience.CumulativeLayoutShift.IsSet() {
		attrs.PutDouble(elasticattr.TransactionUserExperienceCumulativeLayoutShift, tx.UserExperience.CumulativeLayoutShift.Val)
	}
	if tx.UserExperience.FirstInputDelay.IsSet() {
		attrs.PutDouble(elasticattr.TransactionUserExperienceFirstInputDelay, tx.UserExperience.FirstInputDelay.Val)
	}
	if tx.UserExperience.TotalBlockingTime.IsSet() {
		attrs.PutDouble(elasticattr.TransactionUserExperienceTotalBlockingTime, tx.UserExperience.TotalBlockingTime.Val)
	}
	if tx.UserExperience.Longtask.Count.IsSet() {
		attrs.PutInt(elasticattr.TransactionUserExperienceLongTaskCount, tx.UserExperience.Longtask.Count.Val)
		attrs.PutDouble(elasticattr.TransactionUserExperienceLongTaskMax, tx.UserExperience.Longtask.Max.Val)
		attrs.PutDouble(elasticattr.TransactionUserExperienceLongTaskSum, tx.UserExperience.Longtask.Sum.Val)
	}

	// Elastic-specific: custom
	if len(tx.Context.Custom) > 0 {
		m := attrs.PutEmptyMap(elasticattr.TransactionCustom)
		for k, v := range tx.Context.Custom {
			insertAnyValue(m.PutEmpty(k), v)
		}
	}

	// Elastic-specific: transaction marks
	if len(tx.Marks.Events) > 0 {
		allMarks := attrs.PutEmptyMap(elasticattr.TransactionMarks)
		for markName, events := range tx.Marks.Events {
			if len(events.Measurements) == 0 {
				continue
			}
			markMap := allMarks.PutEmptyMap(markName)
			for name, val := range events.Measurements {
				markMap.PutDouble(name, val)
			}
		}
	}

	// Elastic-specific: transaction message
	setSpanMessageAttrs("transaction", attrs, tx.Context.Message)

	// Elastic-specific: session
	if tx.Session.ID.IsSet() {
		attrs.PutStr(elasticattr.SessionID, tx.Session.ID.Val)
		if tx.Session.Sequence.IsSet() && tx.Session.Sequence.Val > 0 {
			attrs.PutInt(elasticattr.SessionSequence, int64(tx.Session.Sequence.Val))
		}
	}

	// OTel: elastic.profiler_stack_trace_ids becomes transaction.profiler_stack_trace_ids
	if tx.OTel.IsSet() {
		if ids, ok := tx.OTel.Attributes["elastic.profiler_stack_trace_ids"]; ok {
			if arr, ok := ids.([]interface{}); ok && len(arr) > 0 {
				sl := attrs.PutEmptySlice(elasticattr.TransactionProfilerStackTraceIDs)
				for _, item := range arr {
					if str, ok := item.(string); ok {
						sl.AppendEmpty().SetStr(str)
					}
				}
			}
		}
	}

	return s
}

// AppendDroppedSpanStats expands transaction.dropped_spans_stats into synthetic CLIENT spans.
// Each DSS entry becomes one _noindex span with composite sum/count for the metrics pipeline.
func AppendDroppedSpanStats(ss ptrace.ScopeSpans, parent ptrace.Span, stats []transactionDroppedSpanStats) {
	if len(stats) == 0 {
		return
	}
	parentSpanID := parent.SpanID()
	for i, stat := range stats {
		// Deterministic synthetic span ID: xxhash(parentSpanID || stat_index)
		d := xxhash.New()
		_, _ = d.Write(parentSpanID[:])
		var idxBuf [4]byte
		binary.BigEndian.PutUint32(idxBuf[:], uint32(i))
		_, _ = d.Write(idxBuf[:])
		var spanID pcommon.SpanID
		binary.BigEndian.PutUint64(spanID[:], d.Sum64())

		s := ss.Spans().AppendEmpty()
		s.SetTraceID(parent.TraceID())
		s.SetSpanID(spanID)
		s.SetStartTimestamp(parent.StartTimestamp())
		s.SetEndTimestamp(parent.StartTimestamp())
		s.TraceState().FromRaw(parent.TraceState().AsRaw())

		attrs := s.Attributes()
		attrs.PutEmptySlice("elasticsearch.mapping.hints").AppendEmpty().SetStr("_noindex")
		attrs.PutStr(elasticattr.SpanName, "")
		if stat.DestinationServiceResource.IsSet() {
			attrs.PutStr(elasticattr.SpanDestinationServiceResource, stat.DestinationServiceResource.Val)
		}
		if stat.ServiceTargetType.IsSet() {
			attrs.PutStr(elasticattr.ServiceTargetType, stat.ServiceTargetType.Val)
		}
		if stat.ServiceTargetName.IsSet() {
			attrs.PutStr(elasticattr.ServiceTargetName, stat.ServiceTargetName.Val)
		}
		if stat.Outcome.IsSet() {
			attrs.PutStr(elasticattr.EventOutcome, stat.Outcome.Val)
		}
		attrs.PutInt(elasticattr.SpanCompositeSumUs, int64(stat.Duration.Sum.Us.Val))
		attrs.PutInt(elasticattr.SpanCompositeCount, int64(stat.Duration.Count.Val))
	}
}

// --- helpers ---

func setSpanStatus(s ptrace.Span, outcome string) {
	switch strings.ToLower(outcome) {
	case "success":
		s.Status().SetCode(ptrace.StatusCodeOk)
	case "failure":
		s.Status().SetCode(ptrace.StatusCodeError)
	}
}

func setOutcomeAttr(attrs pcommon.Map, outcome string) {
	switch strings.ToLower(outcome) {
	case "success":
		attrs.PutStr(elasticattr.EventOutcome, "success")
	case "failure":
		attrs.PutStr(elasticattr.EventOutcome, "failure")
	default:
		attrs.PutStr(elasticattr.EventOutcome, "unknown")
	}
}

// parseSpanType returns (type, subtype, action) splitting on "." when subtype/action are unset.
func parseSpanType(sp *span) (string, string, string) {
	typ := sp.Type.Val
	sub := sp.Subtype.Val
	act := sp.Action.Val
	if sub == "" && act == "" && strings.Contains(typ, ".") {
		parts := strings.SplitN(typ, ".", 3)
		typ = parts[0]
		if len(parts) > 1 {
			sub = parts[1]
		}
		if len(parts) > 2 {
			act = parts[2]
		}
	}
	return typ, sub, act
}

func setSpanHTTPSemConv(attrs pcommon.Map, sp *span) {
	if sp.Context.HTTP.Method.IsSet() {
		attrs.PutStr(string(semconv.HTTPRequestMethodKey), sp.Context.HTTP.Method.Val)
	}
	// Status code: prefer response.status_code, fall back to deprecated top-level status_code
	if sp.Context.HTTP.Response.StatusCode.IsSet() {
		attrs.PutInt(string(semconv.HTTPResponseStatusCodeKey), int64(sp.Context.HTTP.Response.StatusCode.Val))
	} else if sp.Context.HTTP.StatusCode.IsSet() {
		attrs.PutInt(string(semconv.HTTPResponseStatusCodeKey), int64(sp.Context.HTTP.StatusCode.Val))
	}
	if sp.Context.HTTP.Response.EncodedBodySize.IsSet() {
		attrs.PutInt(string(semconv.HTTPResponseBodySizeKey), int64(sp.Context.HTTP.Response.EncodedBodySize.Val))
	}
	if sp.Context.HTTP.URL.IsSet() {
		attrs.PutStr(string(semconv.URLOriginalKey), sp.Context.HTTP.URL.Val)
	}
}

func setContextHTTPSemConv(attrs pcommon.Map, ctx *eventContext) {
	if ctx.Request.Method.IsSet() {
		attrs.PutStr(string(semconv.HTTPRequestMethodKey), ctx.Request.Method.Val)
	}
	if ctx.Response.StatusCode.IsSet() {
		attrs.PutInt(string(semconv.HTTPResponseStatusCodeKey), int64(ctx.Response.StatusCode.Val))
	}
	if ctx.Response.EncodedBodySize.IsSet() {
		attrs.PutInt(string(semconv.HTTPResponseBodySizeKey), int64(ctx.Response.EncodedBodySize.Val))
	}
}

func setTransactionURLAttrs(attrs pcommon.Map, u *contextRequestURL) {
	if u.Raw.IsSet() {
		attrs.PutStr(string(semconv.URLOriginalKey), u.Raw.Val)
	}
	protocol := strings.TrimSuffix(u.Protocol.Val, ":")
	if protocol != "" {
		attrs.PutStr(string(semconv.URLSchemeKey), protocol)
	}
	if u.Full.IsSet() {
		attrs.PutStr(string(semconv.URLFullKey), u.Full.Val)
	}
	if u.Hostname.IsSet() {
		attrs.PutStr(string(semconv.URLDomainKey), u.Hostname.Val)
	}
	if u.Path.IsSet() {
		attrs.PutStr(string(semconv.URLPathKey), u.Path.Val)
	}
	if u.Search.IsSet() {
		attrs.PutStr(string(semconv.URLQueryKey), u.Search.Val)
	}
	if u.Hash.IsSet() {
		attrs.PutStr(string(semconv.URLFragmentKey), u.Hash.Val)
	}
	if u.Port.IsSet() {
		switch p := u.Port.Val.(type) {
		case string:
			if n, err := strconv.Atoi(p); err == nil && n != 0 {
				attrs.PutInt(string(semconv.URLPortKey), int64(n))
			}
		case json.Number:
			if n, err := p.Int64(); err == nil && n != 0 {
				attrs.PutInt(string(semconv.URLPortKey), n)
			}
		}
	}
}

func setContextHTTPElastic(attrs pcommon.Map, ctx *eventContext) {
	if ctx.Request.HTTPVersion.IsSet() {
		attrs.PutStr(elasticattr.HTTPVersion, ctx.Request.HTTPVersion.Val)
	}
	if ctx.Request.Headers.IsSet() {
		setHTTPHeadersMap(elasticattr.HTTPRequestHeaders, attrs, ctx.Request.Headers.Val)
	}
	if len(ctx.Request.Cookies) > 0 {
		m := attrs.PutEmptyMap(elasticattr.HTTPRequestCookies)
		for k, v := range ctx.Request.Cookies {
			insertAnyValue(m.PutEmpty(k), v)
		}
	}
	if len(ctx.Request.Env) > 0 {
		m := attrs.PutEmptyMap(elasticattr.HTTPRequestEnv)
		for k, v := range ctx.Request.Env {
			insertAnyValue(m.PutEmpty(k), v)
		}
	}
	if ctx.Request.Body.IsSet() {
		insertAnyValue(attrs.PutEmpty(elasticattr.HTTPRequestBodyOriginal), ctx.Request.Body.Val)
	}
	if ctx.Page.Referer.IsSet() {
		attrs.PutStr(elasticattr.HTTPRequestReferrer, ctx.Page.Referer.Val)
	}
	if ctx.Response.Headers.IsSet() {
		setHTTPHeadersMap(elasticattr.HTTPResponseHeaders, attrs, ctx.Response.Headers.Val)
	}
	if ctx.Response.Finished.IsSet() {
		attrs.PutBool(elasticattr.HTTPResponseFinished, ctx.Response.Finished.Val)
	}
	if ctx.Response.HeadersSent.IsSet() {
		attrs.PutBool(elasticattr.HTTPResponseHeadersSent, ctx.Response.HeadersSent.Val)
	}
	if ctx.Response.DecodedBodySize.IsSet() {
		attrs.PutInt(elasticattr.HTTPResponseDecodedBodySize, int64(ctx.Response.DecodedBodySize.Val))
	}
	if ctx.Response.TransferSize.IsSet() {
		attrs.PutInt(elasticattr.HTTPResponseTransferSize, int64(ctx.Response.TransferSize.Val))
	}
}

func setSpanMessageAttrs(prefix string, attrs pcommon.Map, m contextMessage) {
	if !m.RoutingKey.IsSet() && !m.Body.IsSet() && !m.Age.Milliseconds.IsSet() && !m.Headers.IsSet() {
		return
	}
	if m.RoutingKey.IsSet() {
		attrs.PutStr(fmt.Sprintf("%s.%s", prefix, elasticattr.MessageRoutingKey), m.RoutingKey.Val)
	}
	if m.Body.IsSet() {
		attrs.PutStr(fmt.Sprintf("%s.%s", prefix, elasticattr.MessageBody), m.Body.Val)
	}
	if m.Age.Milliseconds.IsSet() {
		attrs.PutInt(fmt.Sprintf("%s.%s", prefix, elasticattr.MessageAgeMs), int64(m.Age.Milliseconds.Val))
	}
	if m.Headers.IsSet() {
		for name, values := range m.Headers.Val {
			key := fmt.Sprintf("%s.%s.%s", prefix, elasticattr.MessageHeadersPrefix, name)
			sl := attrs.PutEmptySlice(key)
			sl.EnsureCapacity(len(values))
			for _, v := range values {
				sl.AppendEmpty().SetStr(v)
			}
		}
	}
}

// setHTTPHeadersMap writes net/http.Header as a pcommon.Map of string→[]string.
func setHTTPHeadersMap(key string, attrs pcommon.Map, headers http.Header) {
	if len(headers) == 0 {
		return
	}
	m := attrs.PutEmptyMap(key)
	m.EnsureCapacity(len(headers))
	for name, values := range headers {
		if len(values) == 0 {
			continue
		}
		sl := m.PutEmptySlice(name)
		sl.EnsureCapacity(len(values))
		for _, v := range values {
			sl.AppendEmpty().SetStr(v)
		}
	}
}

func setStackTraceFrames(key string, attrs pcommon.Map, frames []stacktraceFrame) {
	sl := attrs.PutEmptySlice(key)
	sl.EnsureCapacity(len(frames))
	for _, f := range frames {
		appendStacktraceFrame(sl.AppendEmpty().SetEmptyMap(), f)
	}
}

// appendStacktraceFrame writes one stacktrace frame into fm.
// The frame field keys are identical for span stacktraces and error exception/log stacktraces.
func appendStacktraceFrame(fm pcommon.Map, f stacktraceFrame) {
	if len(f.Vars) > 0 {
		vm := fm.PutEmptyMap(elasticattr.SpanStacktraceFrameVars)
		for k, v := range f.Vars {
			insertAnyValue(vm.PutEmpty(k), v)
		}
	}
	if f.LineNumber.IsSet() {
		fm.PutInt(elasticattr.SpanStacktraceFrameLineNumber, int64(f.LineNumber.Val))
	}
	if f.ColumnNumber.IsSet() {
		fm.PutInt(elasticattr.SpanStacktraceFrameLineColumn, int64(f.ColumnNumber.Val))
	}
	if f.Filename.IsSet() && f.Filename.Val != "" {
		fm.PutStr(elasticattr.SpanStacktraceFrameFilename, f.Filename.Val)
	}
	if f.Classname.IsSet() && f.Classname.Val != "" {
		fm.PutStr(elasticattr.SpanStacktraceFrameClassname, f.Classname.Val)
	}
	if f.ContextLine.IsSet() && f.ContextLine.Val != "" {
		fm.PutStr(elasticattr.SpanStacktraceFrameLineContext, f.ContextLine.Val)
	}
	if f.Module.IsSet() && f.Module.Val != "" {
		fm.PutStr(elasticattr.SpanStacktraceFrameModule, f.Module.Val)
	}
	if f.Function.IsSet() && f.Function.Val != "" {
		fm.PutStr(elasticattr.SpanStacktraceFrameFunction, f.Function.Val)
	}
	if f.AbsPath.IsSet() && f.AbsPath.Val != "" {
		fm.PutStr(elasticattr.SpanStacktraceFrameAbsPath, f.AbsPath.Val)
	}
	if len(f.PreContext) > 0 {
		pre := fm.PutEmptySlice(elasticattr.SpanStacktraceFrameContextPre)
		pre.EnsureCapacity(len(f.PreContext))
		for _, c := range f.PreContext {
			pre.AppendEmpty().SetStr(c)
		}
	}
	if len(f.PostContext) > 0 {
		post := fm.PutEmptySlice(elasticattr.SpanStacktraceFrameContextPost)
		post.EnsureCapacity(len(f.PostContext))
		for _, c := range f.PostContext {
			post.AppendEmpty().SetStr(c)
		}
	}
	if f.LibraryFrame.IsSet() && f.LibraryFrame.Val {
		fm.PutBool(elasticattr.SpanStacktraceFrameLibraryFrame, true)
	}
	// ExcludeFromGrouping is always written (no omitempty in apm-data), will be false
	fm.PutBool(elasticattr.SpanStacktraceExcludeFromGrouping, false)
}

// insertAnyValue converts a JSON-decoded value (string, json.Number, bool, map, slice)
// into the destination pcommon.Value.
func insertAnyValue(dest pcommon.Value, src any) {
	if src == nil {
		return
	}
	switch v := src.(type) {
	case string:
		dest.SetStr(v)
	case json.Number:
		if f, err := v.Float64(); err == nil {
			dest.SetDouble(f)
		} else {
			dest.SetStr(v.String())
		}
	case bool:
		dest.SetBool(v)
	case map[string]interface{}:
		if len(v) == 0 {
			return
		}
		m := dest.SetEmptyMap()
		for k, val := range v {
			if val == nil {
				continue
			}
			if mv, ok := val.(map[string]interface{}); ok && len(mv) == 0 {
				continue
			}
			insertAnyValue(m.PutEmpty(k), val)
		}
	case []interface{}:
		sl := dest.SetEmptySlice()
		for _, item := range v {
			insertAnyValue(sl.AppendEmpty(), item)
		}
	}
}

func spanRepresentativeCount(sr Float64) float64 {
	if !sr.IsSet() {
		return 1.0
	}
	if sr.Val > 0 {
		return 1.0 / sr.Val
	}
	return 0
}

// traceIDFromHex decodes a hex string into a pcommon.TraceID, padding with zeros if short.
func traceIDFromHex(s string) (pcommon.TraceID, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return pcommon.TraceID{}, err
	}
	var id pcommon.TraceID
	copy(id[:], b)
	return id, nil
}

// spanIDFromHex decodes a hex string into a pcommon.SpanID, padding with zeros if short.
func spanIDFromHex(s string) (pcommon.SpanID, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return pcommon.SpanID{}, err
	}
	var id pcommon.SpanID
	copy(id[:], b)
	return id, nil
}

// probabilityToTValue converts a sampling probability (0, 1] to a W3C tracestate T-value.
// Mirrors the implementation in mappers/intakeV2ToOtlpTopLevelFields.go.
func probabilityToTValue(probability float64) string {
	if probability <= 0 || probability > 1 {
		return ""
	}
	if probability == 1 {
		return "0"
	}
	const maxThreshold = (1 << 56) - 1
	raw := math.Round((1.0 - probability) * (1 << 56))
	if raw < 0 {
		raw = 0
	}
	if raw > maxThreshold {
		raw = maxThreshold
	}
	s := fmt.Sprintf("%014x", uint64(raw))
	s = strings.TrimRight(s, "0")
	if s == "" {
		return "0"
	}
	return s
}

// targetFromDestResource parses destination.service.resource into (type, name).
// Mirrors apm-data's targetFromDestinationResource in modeldecoder/v2/decoder.go.
func targetFromDestResource(res string) (typ, name string) {
	m := reDestResource.FindStringSubmatch(res)
	if m == nil {
		return "", res
	}
	return m[1], m[2]
}

// mapSpanKind maps the OTel string span kind to ptrace.SpanKind.
// Mirrors the implementation in receiver.go.
func mapSpanKind(kind string) ptrace.SpanKind {
	switch strings.ToUpper(kind) {
	case "INTERNAL":
		return ptrace.SpanKindInternal
	case "CLIENT":
		return ptrace.SpanKindClient
	case "PRODUCER":
		return ptrace.SpanKindProducer
	case "CONSUMER":
		return ptrace.SpanKindConsumer
	case "SERVER":
		return ptrace.SpanKindServer
	default:
		return ptrace.SpanKindUnspecified
	}
}
