package mappers

import (
	"encoding/hex"
	"strings"
	"time"

	"github.com/elastic/apm-data/model/modelpb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type TopLevelFieldSetter interface {
	SetTraceID(v pcommon.TraceID)
	SetSpanID(v pcommon.SpanID)
}

// Shared across LogRecord and Span
func SetTopLevelFieldsCommon(event *modelpb.APMEvent, timestamp time.Time, t TopLevelFieldSetter, logger *zap.Logger) {

	if event.Trace != nil && event.Trace.Id != "" {
		traceId, err := traceIDFromHex(event.Trace.Id)
		if err == nil {
			t.SetTraceID(traceId)
		} else {
			logger.Error("failed to parse trace ID", zap.String("trace_id", event.Trace.Id))
		}
	}

	if event.Span != nil && event.Span.Id != "" {
		spanId, err := spanIdFromHex(event.Span.Id)
		if err == nil {
			t.SetSpanID(spanId)
		} else {
			logger.Error("failed to parse span ID", zap.String("span_id", event.Span.Id))
		}
	}

	if event.Transaction != nil && event.Transaction.Id != "" {
		transactionId, err := spanIdFromHex(event.Transaction.Id)
		if err != nil {
			t.SetSpanID(transactionId)
		} else {
			logger.Error("failed to parse transaction ID", zap.String("transaction_id", (event.Transaction.Id)))
		}
	}
}

// Sets top level fields on ptrace.Span based on the APMEvent
func SetTopLevelFieldsSpan(event *modelpb.APMEvent, timestamp time.Time, s ptrace.Span, logger *zap.Logger) {
	SetTopLevelFieldsCommon(event, timestamp, s, logger)

	if event.ParentId != "" {
		parentId, err := spanIdFromHex(event.ParentId)
		if err == nil {
			s.SetParentSpanID(parentId)
		} else {
			logger.Error("failed to parse parent span ID", zap.String("parent_id", event.ParentId))
		}
	}

	if strings.EqualFold(event.Event.Outcome, "success") {
		s.Status().SetCode(ptrace.StatusCodeOk)
	} else if strings.EqualFold(event.Event.Outcome, "failure") {
		s.Status().SetCode(ptrace.StatusCodeError)
	}

	duration := time.Duration(event.GetEvent().GetDuration())
	s.SetStartTimestamp(pcommon.NewTimestampFromTime(timestamp))
	s.SetEndTimestamp(pcommon.NewTimestampFromTime(timestamp.Add(duration)))
}

// Sets top level fields on plog.LogRecord based on the APMEvent
func SetTopLevelFieldsLogRecord(event *modelpb.APMEvent, timestamp time.Time, l plog.LogRecord, logger *zap.Logger) {
	SetTopLevelFieldsCommon(event, timestamp, l, logger)
	l.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
}

func traceIDFromHex(hexStr string) (pcommon.TraceID, error) {
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return pcommon.TraceID{}, err
	}
	var id pcommon.TraceID
	copy(id[:], bytes)
	return id, nil
}

func spanIdFromHex(hexStr string) (pcommon.SpanID, error) {
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return pcommon.SpanID{}, err
	}
	var id pcommon.SpanID
	copy(id[:], bytes)
	return id, nil
}
