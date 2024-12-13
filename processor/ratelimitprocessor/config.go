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

package ratelimitprocessor

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
		Strategy: StrategyRateLimitRequests,
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
