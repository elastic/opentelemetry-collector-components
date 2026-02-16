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

package enrichments // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments"

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling"
	"github.com/ua-parser/uap-go/uaparser"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv25 "go.opentelemetry.io/otel/semconv/v1.25.0"
	semconv27 "go.opentelemetry.io/otel/semconv/v1.27.0"
	semconv37 "go.opentelemetry.io/otel/semconv/v1.37.0"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/codes"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/attribute"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
)

// defaultRepresentativeCount is the representative count to use for adjusting
// sampled spans when a value could not be found from tracestate. Our default is
// to assume sampling all spans.
const defaultRepresentativeCount = 1.0

// EnrichSpan adds Elastic specific attributes to the OTel span.
// These attributes are derived from the base attributes and appended to
// the span attributes. The enrichment logic is performed by categorizing
// the OTel spans into 2 different categories:
//   - Elastic transactions, defined as spans which measure the highest
//     level of work being performed with a service.
//   - Elastic spans, defined as all spans (including transactions).
//     However, for the enrichment logic spans are treated as a separate
//     entity i.e. all transactions are not enriched as spans and vice versa.
func EnrichSpan(
	span ptrace.Span,
	cfg config.Config,
	userAgentParser *uaparser.Parser,
) {
	var c spanEnrichmentContext
	c.Enrich(span, cfg, userAgentParser)
}

type spanEnrichmentContext struct {
	urlFull *url.URL

	peerService              string
	serverAddress            string
	urlScheme                string
	urlDomain                string
	urlPath                  string
	urlQuery                 string
	rpcSystem                string
	rpcService               string
	grpcStatus               string
	dbName                   string
	dbSystem                 string
	messagingOperation       string
	messagingSystem          string
	messagingDestinationName string
	genAiSystem              string
	typeValue                string
	transactionType          string

	// The inferred* attributes are derived from a base attribute
	userAgentOriginal        string
	userAgentName            string
	userAgentVersion         string
	inferredUserAgentName    string
	inferredUserAgentVersion string

	serverPort     int64
	urlPort        int64
	httpStatusCode int64

	spanStatusCode ptrace.StatusCode

	// TODO (lahsivjar): Refactor span enrichment to better utilize isTransaction
	isTransaction            bool
	isMessaging              bool
	isRPC                    bool
	isHTTP                   bool
	isDB                     bool
	messagingDestinationTemp bool
	isGenAi                  bool
}

func (s *spanEnrichmentContext) Enrich(
	span ptrace.Span,
	cfg config.Config,
	userAgentParser *uaparser.Parser,
) {
	// Extract top level span information.
	s.spanStatusCode = span.Status().Code()

	// Extract information from span attributes.
	span.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch k {
		case string(semconv25.PeerServiceKey):
			s.peerService = v.Str()
		case string(semconv25.ServerAddressKey):
			s.serverAddress = v.Str()
		case string(semconv25.ServerPortKey):
			s.serverPort = v.Int()
		case string(semconv25.NetPeerNameKey):
			if s.serverAddress == "" {
				// net.peer.name is deprecated, so has lower priority
				// only set when not already set with server.address
				// and allowed to be overridden by server.address.
				s.serverAddress = v.Str()
			}
		case string(semconv25.NetPeerPortKey):
			if s.serverPort == 0 {
				// net.peer.port is deprecated, so has lower priority
				// only set when not already set with server.port and
				// allowed to be overridden by server.port.
				s.serverPort = v.Int()
			}
		case string(semconv25.MessagingDestinationNameKey):
			s.isMessaging = true
			s.messagingDestinationName = v.Str()
		case string(semconv25.MessagingOperationKey),
			string(semconv37.MessagingOperationNameKey):
			s.isMessaging = true
			s.messagingOperation = v.Str()
		case string(semconv25.MessagingSystemKey):
			s.isMessaging = true
			s.messagingSystem = v.Str()
		case string(semconv25.MessagingDestinationTemporaryKey):
			s.isMessaging = true
			s.messagingDestinationTemp = true
		case string(semconv25.HTTPStatusCodeKey),
			string(semconv25.HTTPResponseStatusCodeKey):
			s.isHTTP = true
			s.httpStatusCode = v.Int()
		case string(semconv25.HTTPMethodKey),
			string(semconv25.HTTPRequestMethodKey),
			string(semconv25.HTTPTargetKey),
			string(semconv25.HTTPSchemeKey),
			string(semconv25.HTTPFlavorKey),
			string(semconv25.NetHostNameKey):
			s.isHTTP = true
		case string(semconv25.URLFullKey),
			string(semconv25.HTTPURLKey):
			s.isHTTP = true
			// ignoring error as if parse fails then we don't want the url anyway
			s.urlFull, _ = url.Parse(v.Str())
		case string(semconv25.URLSchemeKey):
			s.isHTTP = true
			s.urlScheme = v.Str()
		case string(semconv25.URLDomainKey):
			s.isHTTP = true
			s.urlDomain = v.Str()
		case string(semconv25.URLPortKey):
			s.isHTTP = true
			s.urlPort = v.Int()
		case string(semconv25.URLPathKey):
			s.isHTTP = true
			s.urlPath = v.Str()
		case string(semconv25.URLQueryKey):
			s.isHTTP = true
			s.urlQuery = v.Str()
		case string(semconv25.RPCGRPCStatusCodeKey):
			s.isRPC = true
			s.grpcStatus = codes.Code(v.Int()).String()
		case string(semconv25.RPCSystemKey):
			s.isRPC = true
			s.rpcSystem = v.Str()
		case string(semconv25.RPCServiceKey):
			s.isRPC = true
			s.rpcService = v.Str()
		case string(semconv25.DBStatementKey),
			string(semconv25.DBUserKey), string(semconv37.DBQueryTextKey):
			s.isDB = true
		case string(semconv25.DBNameKey), string(semconv37.DBNamespaceKey):
			s.isDB = true
			s.dbName = v.Str()
		case string(semconv25.DBSystemKey), string(semconv37.DBSystemNameKey):
			s.isDB = true
			s.dbSystem = v.Str()
		case string(semconv27.GenAISystemKey), string(semconv37.GenAIProviderNameKey):
			s.isGenAi = true
			s.genAiSystem = v.Str()
		case string(semconv27.UserAgentOriginalKey):
			s.userAgentOriginal = v.Str()
		case string(semconv27.UserAgentNameKey):
			s.userAgentName = v.Str()
		case string(semconv27.UserAgentVersionKey):
			s.userAgentVersion = v.Str()
		case "type":
			s.typeValue = v.Str()
		case elasticattr.TransactionType:
			s.transactionType = v.Str()
		}
		return true
	})

	s.normalizeAttributes(userAgentParser)
	s.isTransaction = isElasticTransaction(span)
	s.enrich(span, cfg)

	spanEvents := span.Events()
	for i := 0; i < spanEvents.Len(); i++ {
		var c spanEventEnrichmentContext
		c.enrich(s, spanEvents.At(i), cfg.SpanEvent)
	}
}

func (s *spanEnrichmentContext) enrich(span ptrace.Span, cfg config.Config) {

	// In OTel, a local root span can represent an outgoing call or a producer span.
	// In such cases, the span is still mapped into a transaction, but enriched
	// with additional attributes that are specific to the outgoing call or producer span.
	isExitRootSpan := s.isTransaction && (span.Kind() == ptrace.SpanKindClient || span.Kind() == ptrace.SpanKindProducer)

	if s.isTransaction {
		s.enrichTransaction(span, cfg.Transaction)
	}
	if !s.isTransaction || isExitRootSpan {
		s.enrichSpan(span, cfg.Span, cfg.Transaction.Type.Enabled, isExitRootSpan)
	}
}

func (s *spanEnrichmentContext) enrichTransaction(
	span ptrace.Span,
	cfg config.ElasticTransactionConfig,
) {
	if cfg.TimestampUs.Enabled {
		attribute.PutInt(span.Attributes(), elasticattr.TimestampUs, getTimestampUs(span.StartTimestamp()))
	}
	if cfg.Sampled.Enabled {
		attribute.PutBool(span.Attributes(), elasticattr.TransactionSampled, s.getSampled())
	}
	if cfg.ID.Enabled {
		transactionID := span.SpanID().String()
		attribute.PutStr(span.Attributes(), elasticattr.TransactionID, transactionID)

		if cfg.ClearSpanID.Enabled {
			span.SetSpanID(pcommon.SpanID{})
		}
	}
	if cfg.Root.Enabled {
		attribute.PutBool(span.Attributes(), elasticattr.TransactionRoot, isTraceRoot(span))
	}
	if cfg.Name.Enabled {
		attribute.PutStr(span.Attributes(), elasticattr.TransactionName, span.Name())
		if cfg.ClearSpanName.Enabled {
			span.SetName("")
		}
	}
	if cfg.ProcessorEvent.Enabled {
		attribute.PutStr(span.Attributes(), elasticattr.ProcessorEvent, "transaction")
	}
	if cfg.RepresentativeCount.Enabled {
		repCount := getRepresentativeCount(span.TraceState().AsRaw())
		attribute.PutDouble(span.Attributes(), elasticattr.TransactionRepresentativeCount, repCount)
	}
	if cfg.DurationUs.Enabled {
		attribute.PutInt(span.Attributes(), elasticattr.TransactionDurationUs, getDurationUs(span))
	}
	if cfg.Type.Enabled {
		attribute.PutStr(span.Attributes(), elasticattr.TransactionType, s.getTxnType())
	}
	if cfg.Result.Enabled {
		s.setTxnResult(span)
	}
	if cfg.EventOutcome.Enabled {
		s.setEventOutcome(span)
	}
	if cfg.InferredSpans.Enabled {
		s.setInferredSpans(span)
	}
	if cfg.UserAgent.Enabled {
		s.setUserAgentIfRequired(span)
	}
	if cfg.MessageQueueName.Enabled {
		s.setMessageQueue(span)
	}
	if cfg.RemoveMessaging.Enabled {
		s.removeMessagingAttrs(span)
	}
}

func (s *spanEnrichmentContext) enrichSpan(
	span ptrace.Span,
	cfg config.ElasticSpanConfig,
	transactionTypeEnabled bool,
	isExitRootSpan bool,
) {
	var spanType, spanSubtype string

	if cfg.TimestampUs.Enabled {
		attribute.PutInt(span.Attributes(), elasticattr.TimestampUs, getTimestampUs(span.StartTimestamp()))
	}
	if cfg.RepresentativeCount.Enabled {
		repCount := getRepresentativeCount(span.TraceState().AsRaw())
		attribute.PutDouble(span.Attributes(), elasticattr.SpanRepresentativeCount, repCount)
	}
	if cfg.Action.Enabled {
		s.setSpanAction(span)
	}
	if cfg.TypeSubtype.Enabled {
		spanType, spanSubtype = s.setSpanTypeSubtype(span)
	}
	if cfg.EventOutcome.Enabled {
		s.setEventOutcome(span)
	}
	if cfg.DurationUs.Enabled {
		attribute.PutInt(span.Attributes(), elasticattr.SpanDurationUs, getDurationUs(span))
	}
	if cfg.ServiceTarget.Enabled {
		s.setServiceTarget(span)
	}
	if cfg.DestinationService.Enabled {
		s.setDestinationService(span)
	}
	if cfg.InferredSpans.Enabled {
		s.setInferredSpans(span)
	}
	if cfg.ProcessorEvent.Enabled && !isExitRootSpan {
		attribute.PutStr(span.Attributes(), elasticattr.ProcessorEvent, "span")
	}
	if cfg.UserAgent.Enabled {
		s.setUserAgentIfRequired(span)
	}
	if cfg.MessageQueueName.Enabled {
		s.setMessageQueue(span)
	}
	if cfg.RemoveMessaging.Enabled {
		s.removeMessagingAttrs(span)
	}

	// The transaction type should not be updated if it was originally provided (s.transactionType is not empty)
	// Prior enrichment logic may have set this value by using `s.getTxnType()`, in this
	// case it is okay to update the transaction type with a more specific value.
	if isExitRootSpan && transactionTypeEnabled && s.typeValue == "" && s.transactionType == "" {
		if spanType != "" {
			transactionType := spanType
			if spanSubtype != "" {
				transactionType += "." + spanSubtype
			}
			span.Attributes().PutStr(elasticattr.TransactionType, transactionType)
		}
	}
}

// normalizeAttributes sets any dependent attributes that
// might not have been explicitly set as an attribute.
func (s *spanEnrichmentContext) normalizeAttributes(userAgentPraser *uaparser.Parser) {
	if s.rpcSystem == "" && s.grpcStatus != "" {
		s.rpcSystem = "grpc"
	}
	if s.userAgentOriginal != "" && userAgentPraser != nil {
		ua := userAgentPraser.ParseUserAgent(s.userAgentOriginal)
		s.inferredUserAgentName = ua.Family
		s.inferredUserAgentVersion = ua.ToVersionString()
	}
}

func (s *spanEnrichmentContext) getSampled() bool {
	// Assumes that the method is called only for transaction
	return true
}

func (s *spanEnrichmentContext) getTxnType() string {
	txnType := "unknown"
	switch {
	case s.typeValue != "":
		txnType = s.typeValue
	case s.isMessaging:
		txnType = "messaging"
	case s.isRPC, s.isHTTP:
		txnType = "request"
	}
	return txnType
}

func (s *spanEnrichmentContext) setTxnResult(span ptrace.Span) {
	var result string

	if s.isHTTP && s.httpStatusCode > 0 {
		switch i := s.httpStatusCode / 100; i {
		case 1, 2, 3, 4, 5:
			result = standardStatusCodeResults[i-1]
		default:
			result = fmt.Sprintf("HTTP %d", s.httpStatusCode)
		}
	}
	if s.isRPC {
		result = s.grpcStatus
	}
	if result == "" {
		switch s.spanStatusCode {
		case ptrace.StatusCodeError:
			result = "Error"
		default:
			// default to success if all else fails
			result = "Success"
		}
	}

	attribute.PutStr(span.Attributes(), elasticattr.TransactionResult, result)
}

func (s *spanEnrichmentContext) setEventOutcome(span ptrace.Span) {
	// default to success outcome
	outcome := "success"
	successCount := getRepresentativeCount(span.TraceState().AsRaw())
	switch {
	case s.spanStatusCode == ptrace.StatusCodeError:
		outcome = "failure"
		successCount = 0
	case s.spanStatusCode == ptrace.StatusCodeOk:
		// keep the default success outcome
	case s.httpStatusCode >= http.StatusInternalServerError:
		// TODO (lahsivjar): Handle GRPC status code? - not handled in apm-data
		// TODO (lahsivjar): Move to HTTPResponseStatusCode? Backward compatibility?
		outcome = "failure"
		successCount = 0
	}

	attribute.PutStr(span.Attributes(), elasticattr.EventOutcome, outcome)
	attribute.PutInt(span.Attributes(), elasticattr.SuccessCount, int64(successCount))
}

func (s *spanEnrichmentContext) setSpanAction(span ptrace.Span) {
	if s.messagingOperation != "" {
		attribute.PutStr(span.Attributes(), elasticattr.SpanAction, s.messagingOperation)
	}
}

func (s *spanEnrichmentContext) setMessageQueue(span ptrace.Span) {
	messageQueueNameKey := elasticattr.SpanMessageQueueName
	if s.isTransaction {
		messageQueueNameKey = elasticattr.TransactionMessageQueueName
	}

	if s.messagingDestinationName != "" {
		attribute.PutStr(span.Attributes(), messageQueueNameKey, s.messagingDestinationName)
	}
}

// removeMessagingAttrs removes messaging semconv attributes from the span here during processing
// to avoid having to either map or remove during export time.
//
// Note: The elasticapmintake receiver maps extra attributes to semconv for messaging spans here:
// https://github.com/elastic/opentelemetry-collector-components/blob/1da3fff6d82232de8982a9fe46da72354e6ba51c/receiver/elasticapmintakereceiver/internal/mappers/intakeV2ToSemConv.go#L127-L129.
// This creates results in elastic and semconv attribute mapping that is specific to messaging spans
// that is not consistent for all span types (http, grpc, db, etc.).
// This is another reason the attributes are deleted here to avoid special handling at export time.
func (s *spanEnrichmentContext) removeMessagingAttrs(span ptrace.Span) {
	if s.isMessaging {
		span.Attributes().Remove(string(semconv25.MessagingOperationKey))
		span.Attributes().Remove(string(semconv37.MessagingOperationNameKey))
		span.Attributes().Remove(string(semconv25.MessagingSystemKey))
		span.Attributes().Remove(string(semconv25.MessagingDestinationNameKey))
	}
}

func (s *spanEnrichmentContext) setSpanTypeSubtype(span ptrace.Span) (spanType string, spanSubtype string) {
	switch {
	case s.isDB:
		spanType = "db"
		spanSubtype = s.dbSystem
	case s.isMessaging:
		spanType = "messaging"
		spanSubtype = s.messagingSystem
	case s.isRPC:
		spanType = "external"
		spanSubtype = s.rpcSystem
	case s.isHTTP:
		spanType = "external"
		spanSubtype = "http"
	case s.isGenAi:
		spanType = "genai"
		spanSubtype = s.genAiSystem
	default:
		switch span.Kind() {
		case ptrace.SpanKindInternal:
			spanType = "app"
			spanSubtype = "internal"
		default:
			spanType = "unknown"
		}
	}

	attribute.PutStr(span.Attributes(), elasticattr.SpanType, spanType)
	if spanSubtype != "" {
		attribute.PutStr(span.Attributes(), elasticattr.SpanSubtype, spanSubtype)
	}

	return spanType, spanSubtype
}

func (s *spanEnrichmentContext) setServiceTarget(span ptrace.Span) {
	var targetType, targetName string

	if s.peerService != "" {
		targetName = s.peerService
	}

	switch {
	case s.isDB:
		targetType = "db"
		if s.dbSystem != "" {
			targetType = s.dbSystem
		}
		if s.dbName != "" {
			targetName = s.dbName
		}
	case s.isMessaging:
		targetType = "messaging"
		if s.messagingSystem != "" {
			targetType = s.messagingSystem
		}
		if !s.messagingDestinationTemp && s.messagingDestinationName != "" {
			targetName = s.messagingDestinationName
		}
	case s.isRPC:
		targetType = "external"
		if s.rpcSystem != "" {
			targetType = s.rpcSystem
		}
		if s.rpcService != "" {
			targetName = s.rpcService
		}
	case s.isHTTP:
		targetType = "http"
		if resource := getHostPort(
			s.urlFull, s.urlDomain, s.urlPort,
			s.serverAddress, s.serverPort, // fallback
		); resource != "" {
			targetName = resource
		}
	}

	// set either target.type or target.name if at least one is available
	if targetType != "" || targetName != "" {
		attribute.PutStr(span.Attributes(), elasticattr.ServiceTargetType, targetType)
		attribute.PutStr(span.Attributes(), elasticattr.ServiceTargetName, targetName)
	}
}

func (s *spanEnrichmentContext) setDestinationService(span ptrace.Span) {
	var destnResource string
	if s.peerService != "" {
		destnResource = s.peerService
	}

	switch {
	case s.isDB:
		if destnResource == "" && s.dbSystem != "" {
			destnResource = s.dbSystem
		}
	case s.isMessaging:
		if destnResource == "" && s.messagingSystem != "" {
			destnResource = s.messagingSystem
		}
		// For parity with apm-data, destn resource does not handle
		// temporary destination flag. However, it is handled by
		// service.target fields and we might want to do the same here.
		if destnResource != "" && s.messagingDestinationName != "" {
			destnResource += "/" + s.messagingDestinationName
		}
	case s.isRPC, s.isHTTP:
		if destnResource == "" {
			if res := getHostPort(
				s.urlFull, s.urlDomain, s.urlPort,
				s.serverAddress, s.serverPort, // fallback
			); res != "" {
				destnResource = res
			} else {
				// fallback to RPC service
				destnResource = s.rpcService
			}
		}
	}

	if destnResource != "" {
		attribute.PutStr(span.Attributes(), elasticattr.SpanDestinationServiceResource, destnResource)
	}
}

func (s *spanEnrichmentContext) setInferredSpans(span ptrace.Span) {
	if _, exists := span.Attributes().Get(elasticattr.ChildIDs); exists {
		return
	}

	spanLinks := span.Links()
	childIDs := pcommon.NewSlice()
	spanLinks.RemoveIf(func(spanLink ptrace.SpanLink) (remove bool) {
		spanID := spanLink.SpanID()
		spanLink.Attributes().Range(func(k string, v pcommon.Value) bool {
			switch k {
			case "is_child", "elastic.is_child":
				if v.Bool() && !spanID.IsEmpty() {
					remove = true // remove the span link if it has the child attrs
					childIDs.AppendEmpty().SetStr(hex.EncodeToString(spanID[:]))
				}
				return false // stop the loop
			}
			return true
		})
		return remove
	})

	if childIDs.Len() > 0 {
		childIDs.MoveAndAppendTo(span.Attributes().PutEmptySlice(elasticattr.ChildIDs))
	}
}

func (s *spanEnrichmentContext) setUserAgentIfRequired(span ptrace.Span) {
	if s.userAgentName == "" && s.inferredUserAgentName != "" {
		attribute.PutStr(
			span.Attributes(),
			string(semconv27.UserAgentNameKey),
			s.inferredUserAgentName,
		)
	}
	if s.userAgentVersion == "" && s.inferredUserAgentVersion != "" {
		attribute.PutStr(
			span.Attributes(),
			string(semconv27.UserAgentVersionKey),
			s.inferredUserAgentVersion,
		)
	}
}

type spanEventEnrichmentContext struct {
	exceptionType    string
	exceptionMessage string

	exception        bool
	exceptionEscaped bool
}

func (s *spanEventEnrichmentContext) enrich(
	parentCtx *spanEnrichmentContext,
	se ptrace.SpanEvent,
	cfg config.SpanEventConfig,
) {
	// Extract top level span event information.
	s.exception = se.Name() == "exception"
	if s.exception {
		se.Attributes().Range(func(k string, v pcommon.Value) bool {
			switch k {
			case string(semconv25.ExceptionEscapedKey):
				s.exceptionEscaped = v.Bool()
			case string(semconv25.ExceptionTypeKey):
				s.exceptionType = v.Str()
			case string(semconv25.ExceptionMessageKey):
				s.exceptionMessage = v.Str()
			}
			return true
		})
	}

	// Enrich span event attributes.
	if cfg.TimestampUs.Enabled {
		attribute.PutInt(se.Attributes(), elasticattr.TimestampUs, getTimestampUs(se.Timestamp()))
	}
	if cfg.ProcessorEvent.Enabled && s.exception {
		attribute.PutStr(se.Attributes(), elasticattr.ProcessorEvent, "error")
	}
	if s.exceptionType == "" && s.exceptionMessage == "" {
		// Span event does not represent an exception
		return
	}

	// Span event represents exception
	if cfg.ErrorID.Enabled {
		if id, err := newUniqueID(); err == nil {
			attribute.PutStr(se.Attributes(), elasticattr.ErrorID, id)
		}
	}
	if cfg.ErrorExceptionHandled.Enabled {
		attribute.PutBool(se.Attributes(), elasticattr.ErrorExceptionHandled, !s.exceptionEscaped)
	}
	if cfg.ErrorGroupingKey.Enabled {
		// See https://github.com/elastic/apm-data/issues/299
		hash := md5.New()
		// ignoring errors in hashing
		if s.exceptionType != "" {
			_, _ = io.WriteString(hash, s.exceptionType)
		} else if s.exceptionMessage != "" {
			_, _ = io.WriteString(hash, s.exceptionMessage)
		}
		attribute.PutStr(se.Attributes(), elasticattr.ErrorGroupingKey, hex.EncodeToString(hash.Sum(nil)))
	}
	if cfg.ErrorGroupingName.Enabled && s.exceptionMessage != "" {
		attribute.PutStr(se.Attributes(), elasticattr.ErrorGroupingName, s.exceptionMessage)
	}

	// Transaction type and sampled are added as span event enrichment only for errors
	if parentCtx.isTransaction && s.exception {
		if cfg.TransactionSampled.Enabled {
			attribute.PutBool(se.Attributes(), elasticattr.TransactionSampled, parentCtx.getSampled())
		}
		if cfg.TransactionType.Enabled {
			attribute.PutStr(se.Attributes(), elasticattr.TransactionType, parentCtx.getTxnType())
		}
	}
}

// getRepresentativeCount returns the number of spans represented by an
// individually sampled span as per the passed tracestate header.
//
// Representative count is similar to the OTel adjusted count definition
// with a difference that representative count can also include
// dynamically calculated representivity for non-probabilistic sampling.
// In addition, the representative count defaults to 1 if the adjusted
// count is UNKNOWN or the t-value is invalid.
//
// Def: https://opentelemetry.io/docs/specs/otel/trace/tracestate-probability-sampling/#converting-threshold-to-an-adjusted-count-sampling-rate
//
// The count is calculated by using t-value:
// https://opentelemetry.io/docs/specs/otel/trace/tracestate-probability-sampling/#rejection-threshold-t
//
// For compatibility, older calculation is also supported.
//
// Def: https://opentelemetry.io/docs/specs/otel/trace/tracestate-probability-sampling/#adjusted-count)
//
// The count is calculated by using p-value:
// https://opentelemetry.io/docs/reference/specification/trace/tracestate-probability-sampling/#p-value
func getRepresentativeCount(tracestate string) float64 {
	w3cts, err := sampling.NewW3CTraceState(tracestate)
	if err != nil || w3cts.OTelValue() == nil {
		return defaultRepresentativeCount
	}

	otts := w3cts.OTelValue()
	// Use t-value if provided.
	if th, ok := otts.TValueThreshold(); ok {
		// Small optimization for always-sampled case since it's commonly used.
		if th.Unsigned() == 0 {
			return defaultRepresentativeCount
		}
		return th.AdjustedCount()
	}

	var p uint64
	for _, kv := range otts.ExtraValues() {
		if kv.Key == "p" && kv.Value != "" {
			p, _ = strconv.ParseUint(kv.Value, 10, 6)
		}
	}

	if p == 63 {
		// p-value == 63 represents zero adjusted count
		return 0.0
	}
	return math.Pow(2, float64(p))
}

func getDurationUs(span ptrace.Span) int64 {
	return int64(span.EndTimestamp()-span.StartTimestamp()) / 1000
}

func isTraceRoot(span ptrace.Span) bool {
	return span.ParentSpanID().IsEmpty()
}

func isElasticTransaction(span ptrace.Span) bool {
	flags := tracepb.SpanFlags(span.Flags())

	// Events may have already been defined as an elastic transaction.
	// check the processor.event value to avoid incorrectly classifying
	// a span.
	processorEvent, _ := span.Attributes().Get(elasticattr.ProcessorEvent)

	switch {
	case processorEvent.Str() == "transaction":
		return true
	case isTraceRoot(span):
		return true
	case (flags & tracepb.SpanFlags_SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK) == 0:
		// span parent is unknown, fall back to span kind
		return span.Kind() == ptrace.SpanKindServer || span.Kind() == ptrace.SpanKindConsumer
	case (flags & tracepb.SpanFlags_SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK) != 0:
		// span parent is remote
		return true
	}
	return false
}

func getHostPort(
	urlFull *url.URL, urlDomain string, urlPort int64,
	fallbackServerAddress string, fallbackServerPort int64,
) string {
	switch {
	case urlFull != nil:
		return urlFull.Host
	case urlDomain != "":
		if urlPort == 0 {
			return urlDomain
		}
		return net.JoinHostPort(urlDomain, strconv.FormatInt(urlPort, 10))
	case fallbackServerAddress != "":
		if fallbackServerPort == 0 {
			return fallbackServerAddress
		}
		return net.JoinHostPort(fallbackServerAddress, strconv.FormatInt(fallbackServerPort, 10))
	}
	return ""
}

var standardStatusCodeResults = [...]string{
	"HTTP 1xx",
	"HTTP 2xx",
	"HTTP 3xx",
	"HTTP 4xx",
	"HTTP 5xx",
}

func newUniqueID() (string, error) {
	var u [16]byte
	if _, err := io.ReadFull(rand.Reader, u[:]); err != nil {
		return "", err
	}

	// convert to string
	buf := make([]byte, 32)
	hex.Encode(buf, u[:])

	return string(buf), nil
}

func getTimestampUs(ts pcommon.Timestamp) int64 {
	return int64(ts) / 1000
}
