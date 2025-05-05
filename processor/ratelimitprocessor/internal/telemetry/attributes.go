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

package telemetry // import "github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/telemetry"

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
	descisionKey   = "ratelimit_decision"

	TooLarge         ErrorReason = "too_large"
	BadRequest       ErrorReason = "bad_request"
	Invalid          ErrorReason = "invalid"
	TooMany          ErrorReason = "too_many"
	StatusUnderLimit ErrorReason = "underl_limit"
	StatusOverLimit  ErrorReason = "throttled"

	SignalTrace  = "trace"
	SignalMetric = "metric"
	SignalLog    = "log"

	ServerError ErrorReason = "server_error"
	ClientError ErrorReason = "client_error"
)

// WithProjectID returns a project ID attribute with key.
func WithProjectID(project string) attribute.KeyValue {
	return attribute.String(projectIDKey, project)
}

// WithDecision returns decision attribute with key.
func WithDecision(decision string) attribute.KeyValue {
	return attribute.String(descisionKey, decision)
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
