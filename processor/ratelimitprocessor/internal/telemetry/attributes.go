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

// Reason represents a set of constant rate limit processing reasons to
// include as attributes for telemetry.
type Reason string

const (
	projectIDKey = "project_id"
	protocolKey  = "protocol"
	outcomeKey   = "outcome"
	reasonKey    = "reason"
	decisionKey  = "ratelimit_decision"

	TooLarge         Reason = "too_large"
	BadRequest       Reason = "bad_request"
	Invalid          Reason = "invalid"
	TooManyRequests  Reason = "too_many_requests"
	StatusUnderLimit Reason = "under_limit"
	StatusOverLimit  Reason = "over_limit"

	SignalTrace  = "trace"
	SignalMetric = "metric"
	SignalLog    = "log"

	ClientError Reason = "client_error"
	LimitError  Reason = "limit_error"
	RequestErr  Reason = "request_error"
)

// WithProjectID returns a project ID attribute with key.
func WithProjectID(project string) attribute.KeyValue {
	return attribute.String(projectIDKey, project)
}

// WithDecision returns decision attribute with key.
func WithDecision(decision string) attribute.KeyValue {
	return attribute.String(decisionKey, decision)
}

// WithProtocol returns a protocol attribute with key.
func WithProtocol(protocol string) attribute.KeyValue {
	return attribute.String(protocolKey, protocol)
}

// WithOutcome returns an outcome attribute with key.
func WithOutcome(outcome string) attribute.KeyValue {
	return attribute.String(outcomeKey, outcome)
}

// WithReason returns a reason attribute with key.
func WithReason(reason Reason) attribute.KeyValue {
	return attribute.String(reasonKey, string(reason))
}
