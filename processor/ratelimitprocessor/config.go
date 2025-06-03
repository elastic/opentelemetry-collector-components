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

package ratelimitprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor"

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
)

// Config holds configuration for the ratelimit processor.
type Config struct {
	// Type of the rate limiter. Options are "gubernator" or
	// "local". Default is "local".
	Type RateLimiterType `mapstructure:"type"`

	// MetadataKeys holds a list of client metadata keys for
	// defining the rate limiting key, in addition to the
	// processor ID.
	MetadataKeys []string `mapstructure:"metadata_keys"`

	// Strategy holds the rate limiting strategy.
	//
	// Defaults to "requests".
	Strategy Strategy `mapstructure:"strategy"`

	// Rate holds bucket refill rate, in tokens per second.
	Rate int `mapstructure:"rate"`

	// Burst holds the maximum capacity of rate limit buckets.
	Burst int `mapstructure:"burst"`

	// ThrottleBehavior holds the behavior when rate limit is exceeded.
	//
	// Defaults to "error"
	ThrottleBehavior ThrottleBehavior `mapstructure:"throttle_behavior"`
}

// Strategy identifies the rate-limiting strategy: requests, records, or bytes.
type Strategy string

const (
	// StrategyRateLimitRequests identifies the strategy for
	// rate limiting by request.
	StrategyRateLimitRequests Strategy = "requests"

	// StrategyRateLimitRecords identifies the strategy for
	// rate limiting by record: log record, span, metric
	// data point, or profile sample.
	StrategyRateLimitRecords Strategy = "records"

	// StrategyRateLimitBytes identifies the strategy for
	// rate limiting by number of bytes.
	//
	// NOTE measuring the size of data in bytes is much more
	// expensive compared to counting the number of requests
	// and records. Bear in mind that this strategy may impact
	// CPU and memory usage.
	StrategyRateLimitBytes Strategy = "bytes"
)

// ThrottleBehavior identifies the behavior when rate limit is exceeded.
type ThrottleBehavior string

const (
	// ThrottleBehaviorError is the behavior to return an error immediately on throttle and does not send the event.
	ThrottleBehaviorError ThrottleBehavior = "error"

	// ThrottleBehaviorDelay is the behavior to delay the sending until it is no longer throttled.
	ThrottleBehaviorDelay ThrottleBehavior = "delay"
)

// RateLimiterType identifies the type of rate limiter
type RateLimiterType string

const (
	// LocalRateLimiter to indicate a local rate limiter should be used
	LocalRateLimiter RateLimiterType = "local"

	// GubernatorRateLimiter to indicate gubernator should be used
	GubernatorRateLimiter RateLimiterType = "gubernator"
)

// GubernatorBehavior controls Gubernator's behavior.
type GubernatorBehavior string

func createDefaultConfig() component.Config {
	return &Config{
		Strategy:         StrategyRateLimitRequests,
		ThrottleBehavior: ThrottleBehaviorError,
		Type:             LocalRateLimiter,
	}
}

func (config *Config) Validate() error {
	var errs []error
	if config.Rate <= 0 {
		errs = append(errs, fmt.Errorf("rate must be greater than zero"))
	}
	if config.Burst <= 0 {
		errs = append(errs, fmt.Errorf("burst must be greater than zero"))
	}
	return errors.Join(errs...)
}

// Validate checks if strategy matches the possible options for the rate limiter's strategy
func (s Strategy) Validate() error {
	switch s {
	case StrategyRateLimitRequests, StrategyRateLimitRecords, StrategyRateLimitBytes:
		return nil
	}
	return fmt.Errorf(
		"invalid strategy %q, expected one of %q",
		s, []string{
			string(StrategyRateLimitRequests),
			string(StrategyRateLimitRecords),
			string(StrategyRateLimitBytes),
		},
	)
}

// Validate checks if throttle behavior matches the possible options
func (s ThrottleBehavior) Validate() error {
	switch s {
	case ThrottleBehaviorError, ThrottleBehaviorDelay:
		return nil
	}
	return fmt.Errorf(
		"invalid throttle behavior %q, expected one of %q",
		s, []string{
			string(ThrottleBehaviorError),
			string(ThrottleBehaviorDelay),
		},
	)
}

func (t RateLimiterType) Validate() error {
	switch t {
	case LocalRateLimiter, GubernatorRateLimiter:
		return nil
	}
	return fmt.Errorf(
		"invalid rate limiter type %q, expected one of %q",
		t, []string{
			string(LocalRateLimiter),
			string(GubernatorRateLimiter),
		},
	)
}
