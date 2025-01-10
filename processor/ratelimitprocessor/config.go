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
	"sort"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/confmap"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/gubernator"
)

const (
	msgEmptyField = "%s field value is empty"
)

// Config holds configuration for the ratelimit processor.
type Config struct {
	// Gubernator holds configuration for Gubernator,
	// to control distributed rate limiting.
	//
	// If Gubernator is nil, then rate limiting is performed
	// locally to the collector.
	Gubernator *GubernatorConfig `mapstructure:"gubernator"`

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

	// OnThrottle holds the behavior when rate limit is exceeded.
	//
	// Defaults to "error"
	OnThrottle OnThrottleBehavior `mapstructure:"on_throttle"`
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

// OnThrottleBehavior identifies the behavior when rate limit is exceeded.
type OnThrottleBehavior string

const (
	// OnThrottleError is the behavior to return an error immediately on throttle
	OnThrottleError OnThrottleBehavior = "error"

	// OnThrottleBlock is the behavior to block until throttling is done
	OnThrottleBlock OnThrottleBehavior = "block"
)

// GubernatorConfig holds Gubernator-specific configuration for the ratelimit processor.
type GubernatorConfig struct {
	configgrpc.ClientConfig `mapstructure:",squash"`

	// Behavior holds a list of Gubernator behaviors. If this is unspecified,
	// Gubernator's default batching behavior is used.
	Behavior []GubernatorBehavior `mapstructure:"behavior"`
}

// GubernatorBehavior controls Gubernator's behavior.
type GubernatorBehavior string

func createDefaultConfig() component.Config {
	return &Config{
		Strategy:   StrategyRateLimitRequests,
		OnThrottle: OnThrottleError,
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

func (config *GubernatorConfig) Unmarshal(parser *confmap.Conf) error {
	clientConfig := configgrpc.NewDefaultClientConfig()
	config.ClientConfig = *clientConfig
	return parser.Unmarshal(config)
}

func (config *GubernatorConfig) Validate() error {
	var errs []error
	if config.Endpoint == "" {
		errs = append(errs, fmt.Errorf(msgEmptyField, "gubernator.endpoint"))
	}
	return errors.Join(errs...)
}

func (b GubernatorBehavior) Validate() error {
	if _, ok := gubernator.Behavior_value[strings.ToUpper(string(b))]; ok {
		return nil
	}
	validNames := make([]string, 0, len(gubernator.Behavior_name))
	for _, name := range gubernator.Behavior_name {
		validNames = append(validNames, strings.ToLower(name))
	}
	sort.Strings(validNames)
	return fmt.Errorf("invalid Gubernator behavior %q, expected one of %q", b, validNames)
}
