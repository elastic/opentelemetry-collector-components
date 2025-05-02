// ELASTICSEARCH CONFIDENTIAL
// __________________
//
//  Copyright Elasticsearch B.V. All rights reserved.
//
// NOTICE:  All information contained herein is, and remains
// the property of Elasticsearch B.V. and its suppliers, if any.
// The intellectual and technical concepts contained herein
// are proprietary to Elasticsearch B.V. and its suppliers and
// may be covered by U.S. and Foreign Patents, patents in
// process, and are protected by trade secret or copyright
// law.  Dissemination of this information or reproduction of
// this material is strictly forbidden unless prior written
// permission is obtained from Elasticsearch B.V.

package telemetry

import (
	"go.opentelemetry.io/otel/attribute"
)

// ErrorReason represents a set of constant error reasons to
// include as attributes for telemetry.
type ErrorReason string

const (
	projectIDKey   = "project_id"
	protocolKey    = "protocol"
	outcomeKey     = "outcome"
	errorReasonKey = "error_reason"
	codecKey       = "compression.codec"
	statusCodeKey  = "status_code"
	signalTypeKey  = "signal_type"

	TooLarge   ErrorReason = "too_large"
	BadRequest ErrorReason = "bad_request"
	Invalid    ErrorReason = "invalid"
	TooMany    ErrorReason = "too_many"

	SignalTrace  = "trace"
	SignalMetric = "metric"
	SignalLog    = "log"

	ServerError ErrorReason = "server_error"
	ClientError ErrorReason = "client_error"
)

// WithProjectID returns a project ID attribute with key.
func WithProjectID(projectID string) attribute.KeyValue {
	return attribute.String(projectIDKey, projectID)
}

// WithProtocol returns a protocol attribute with key.
func WithProtocol(protocol string) attribute.KeyValue {
	return attribute.String(protocolKey, protocol)
}

// WithOutcome returns a project ID attribute with key.
func WithOutcome(outcome string) attribute.KeyValue {
	return attribute.String(outcomeKey, outcome)
}

// WithErrorReason returns a project ID attribute with key.
func WithErrorReason(reason ErrorReason) attribute.KeyValue {
	return attribute.String(errorReasonKey, string(reason))
}

// WithCompressionCodec returns a compression codec attribute with key
func WithCompressionCodec(codec string) attribute.KeyValue {
	return attribute.String(codecKey, codec)
}

// WithStatusCode returns a status code attribute with key.
func WithStatusCode(code int) attribute.KeyValue {
	return attribute.Int(statusCodeKey, code)
}

func WithSignalType(signal string) attribute.KeyValue {
	return attribute.String(signalTypeKey, signal)
}
