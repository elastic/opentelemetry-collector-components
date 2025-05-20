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
	processorIDKey      = "processor_id"
	reasonKey           = "reason"
	decisionKey         = "ratelimit_decision"
	limitThresholdKey   = "limit_threshold"
	throttleBehaviorKey = "throttle_behavior"

	TooManyRequests  Reason = "too_many_requests"
	StatusUnderLimit Reason = "under_limit"
	StatusOverLimit  Reason = "over_limit"

	LimitError Reason = "limit_error"
	RequestErr Reason = "request_error"
)

// WithDecision returns decision attribute with key.
func WithDecision(decision string) attribute.KeyValue {
	return attribute.String(decisionKey, decision)
}

// WithReason returns a reason attribute with key.
func WithReason(reason Reason) attribute.KeyValue {
	return attribute.String(reasonKey, string(reason))
}

// WithLimitThreshold returns limit threshold with key.
func WithLimitThreshold(limitThreshold float64) attribute.KeyValue {
	return attribute.Float64(limitThresholdKey, limitThreshold)
}

// WithThrottleBehavior returns throttle behavior attribute with key.
func WithThrottleBehavior(throttleBehavior string) attribute.KeyValue {
	return attribute.String(throttleBehaviorKey, throttleBehavior)
}
