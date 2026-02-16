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
	"slices"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
)

// Config holds configuration for the ratelimit processor.
type Config struct {
	// MetadataKeys holds a list of client metadata keys for
	// defining the rate limiting key, in addition to the
	// processor ID.
	MetadataKeys []string `mapstructure:"metadata_keys"`

	// Embed the rate limit settings
	RateLimitSettings `mapstructure:",squash"`

	// Overrides holds a list of overrides for the rate limiter.
	//
	// Defaults to empty
	// Overrides map[string]RateLimitOverrides `mapstructure:"overrides"`
	Overrides []RateLimitOverrides `mapstructure:"overrides"`
}

// RateLimitSettings holds the core rate limiting configuration.
type RateLimitSettings struct {
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

	// ThrottleInterval holds the time interval for throttling.
	//
	// Defaults to 1s
	ThrottleInterval time.Duration `mapstructure:"throttle_interval"`

	// RetryDelay holds the time delay to return to the client through RPC
	// errdetails.RetryInfo. See more details of this in the documentation.
	// https://opentelemetry.io/docs/specs/otlp/#otlpgrpc-throttling.
	//
	// Defaults to 1s
	RetryDelay time.Duration `mapstructure:"retry_delay"`
}

// RateLimitOverrides defines per-unique-key override settings.
// It replaces the top-level RateLimitSettings fields when the unique key matches.
// Nil pointer fields leave the corresponding top-level field unchanged.
type RateLimitOverrides struct {
	// Matches are a map of key-value pairs that MUST be a subset of the
	// incoming client metadata for the override to be applied.
	Matches map[string][]string `mapstructure:"matches"`

	// Rate holds bucket refill rate, in tokens per second.
	Rate *int `mapstructure:"rate"`

	// Burst holds the maximum capacity of rate limit buckets.
	Burst *int `mapstructure:"burst"`

	// ThrottleInterval holds the time interval for throttling.
	// Defaults to 1s
	ThrottleInterval *time.Duration `mapstructure:"throttle_interval"`
}

// Strategy identifies the rate-limiting strategy: requests, records, or bytes.
type Strategy string

func (s Strategy) String() string {
	switch s {
	case StrategyRateLimitRequests:
		return "requests_per_sec"
	case StrategyRateLimitRecords:
		return "records_per_sec"
	case StrategyRateLimitBytes:
		return "bytes_per_sec"
	default:
		return string(s) // NOTE(marclop) shouldn't happen due to validation.
	}
}

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

	// DefaultThrottleInterval is the default value for the
	// throttle interval.
	DefaultThrottleInterval time.Duration = 1 * time.Second

	// DefaultRetryDelay is the default value for the retry delay.
	DefaultRetryDelay time.Duration = 1 * time.Second
)

// ThrottleBehavior identifies the behavior when rate limit is exceeded.
type ThrottleBehavior string

const (
	// ThrottleBehaviorError is the behavior to return an error immediately on throttle and does not send the event.
	ThrottleBehaviorError ThrottleBehavior = "error"

	// ThrottleBehaviorDelay is the behavior to delay the sending until it is no longer throttled.
	ThrottleBehaviorDelay ThrottleBehavior = "delay"
)

func createDefaultConfig() component.Config {
	return &Config{
		RateLimitSettings: RateLimitSettings{
			Strategy:         StrategyRateLimitRequests,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: DefaultThrottleInterval,
			RetryDelay:       DefaultRetryDelay,
		},
	}
}

// resolveRateLimit computes the effective RateLimitSettings for a given unique key.
func resolveRateLimit(cfg *Config, metadata client.Metadata) RateLimitSettings {
	result := cfg.RateLimitSettings
	for _, override := range cfg.Overrides {
		match := true
		for k, v := range override.Matches {
			if slices.Compare(metadata.Get(k), v) != 0 {
				match = false
				break
			}
		}
		if match {
			if override.Rate != nil {
				result.Rate = *override.Rate
			}
			if override.Burst != nil {
				result.Burst = *override.Burst
			}
			if override.ThrottleInterval != nil {
				result.ThrottleInterval = *override.ThrottleInterval
			}
			return result
		}
	}
	return result
}

// Validate performs semantic validation of RateLimitSettings.
func (r *RateLimitSettings) Validate() error {
	var errs []error
	if r.Rate <= 0 {
		errs = append(errs, errors.New("rate must be greater than zero"))
	}
	if r.Burst <= 0 {
		errs = append(errs, errors.New("burst must be greater than zero"))
	}
	if err := r.Strategy.Validate(); err != nil {
		errs = append(errs, err)
	}
	if err := r.ThrottleBehavior.Validate(); err != nil {
		errs = append(errs, err)
	}
	if r.ThrottleInterval <= 0 {
		errs = append(errs, fmt.Errorf("throttle_interval must be greater than zero"))
	}
	return errors.Join(errs...)
}

// Validate performs semantic validation of a RateLimitOverrides instance.
func (r *RateLimitOverrides) Validate() error {
	var errs []error
	if r.Rate != nil {
		if *r.Rate <= 0 {
			errs = append(errs, errors.New("rate must be greater than zero"))
		}
	}
	if r.Burst != nil {
		if *r.Burst <= 0 {
			errs = append(errs, errors.New("burst must be greater than zero"))
		}
	}
	if r.ThrottleInterval != nil && *r.ThrottleInterval <= 0 {
		errs = append(errs, errors.New("throttle_interval must be greater than zero"))
	}
	return errors.Join(errs...)
}

func (config *Config) Validate() error {
	var errs []error
	if err := config.RateLimitSettings.Validate(); err != nil {
		errs = append(errs, err)
	}
	for key, override := range config.Overrides {
		if err := override.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("override %q: %w", key, err))
		}
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
