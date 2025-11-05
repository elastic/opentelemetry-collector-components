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
	"strings"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
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

	// Embed the rate limit settings
	RateLimitSettings `mapstructure:",squash"`

	// DynamicRateLimiting holds the dynamic rate limiting configuration.
	// This is only applicable when the rate limiter type is "gubernator".
	DynamicRateLimiting `mapstructure:"dynamic_limits"`

	// Overrides holds a list of overrides for the rate limiter.
	//
	// Defaults to empty
	// Overrides map[string]RateLimitOverrides `mapstructure:"overrides"`
	Overrides []RateLimitOverrides `mapstructure:"overrides"`

	// Classes holds named rate limit class definitions for class-based dynamic rate limiting.
	// Only applicable when the rate limiter type is "gubernator".
	//
	// Defaults to empty
	Classes map[string]Class `mapstructure:"classes"`

	// DefaultClass specifies the class name to use when no override exists and
	// the class resolver returns unknown/empty class. Must exist in Classes when set.
	// Only applicable when the rate limiter type is "gubernator".
	//
	// Defaults to empty (no default class)
	DefaultClass string `mapstructure:"default_class"`

	// ClassResolverClass is the component ID of the class resolver extension to use.
	// If not set, class resolution is disabled.
	// Only applicable when the rate limiter type is "gubernator".
	ClassResolver component.ID `mapstructure:"class_resolver"`

	// GubernatorBehavior configures the behavior of rate limiter in Gubernator.
	// Only applicable when the rate limiter type is "gubernator".
	//
	// Options are "batching" or "global". Defaults to "batching".
	GubernatorBehavior GubernatorBehavior `mapstructure:"gubernator_behavior"`
}

// Unmarshal implements temporary logic to parse the older format of the overrides.
// This is achieved by identifying if overrides are defined using the old config
// and mapping it to the new config.
func (config *Config) Unmarshal(componentParser *confmap.Conf) error {
	if componentParser == nil {
		return nil
	}

	var oldFmtOverrideKeys []string
	raw := componentParser.Get("overrides")
	if oldFmtOverrides, ok := raw.(map[string]any); ok {
		// Delete the older deprecated overrides config
		componentParser.Delete("overrides")

		// Convert the older overrides config into the new format and create a
		// new `Conf` from the newer config. The old format override keys are
		// cached and converted to the new format later.
		newRawOverrides := make([]any, 0, len(oldFmtOverrides))
		oldFmtOverrideKeys = make([]string, 0, len(oldFmtOverrides))
		for k, v := range oldFmtOverrides {
			newRawOverrides = append(newRawOverrides, v)
			oldFmtOverrideKeys = append(oldFmtOverrideKeys, k)
		}
		// confM will be missing the `matches` config which will be populated
		// after parsing the old format override keys later.
		confM := map[string]any{"overrides": newRawOverrides}
		if err := componentParser.Merge(confmap.NewFromStringMap(confM)); err != nil {
			return fmt.Errorf("failed to parse deprecated overrides format: %w", err)
		}
	}

	if err := componentParser.Unmarshal(config, confmap.WithIgnoreUnused()); err != nil {
		return err
	}

	// Parse the old format override keys and populate `matches`.
	for i, k := range oldFmtOverrideKeys {
		if len(k) == 0 {
			continue
		}
		matches := make(map[string][]string)
		config.Overrides[i].Matches = matches
		matchKVs := strings.Split(k, ";")
		for _, matchKV := range matchKVs {
			if len(matchKV) == 0 {
				continue
			}
			meta := strings.Split(matchKV, ":")
			if len(meta) < 2 {
				return fmt.Errorf("invalid overrides found: %s", matchKV)
			}
			matches[meta[0]] = meta[1:]
		}
	}
	return nil
}

// DynamicRateLimiting defines settings for dynamic rate limiting.
type DynamicRateLimiting struct {
	// Enabled tells the processor to use dynamic rate limiting.
	Enabled bool `mapstructure:"enabled"`

	// WindowDuration defines the time window for which the dynamic rate limit
	// is calculated on. Defaults to 2 minutes.
	WindowDuration time.Duration `mapstructure:"window_duration"`

	// DefaultWindowMultiplier is the factor by which the previous window rate is
	// multiplied to get the dynamic part of the limit. Defaults to 1.3.
	DefaultWindowMultiplier float64 `mapstructure:"default_window_multiplier"`

	// WindowConfigurator is the component ID of the extension to dynamically
	// determine the window multiplier. The extension is expected to implement
	// the `WindowConfigurator` interface. The window configurator is used in
	// the hot path so it should respond fast. If the configurator returns a
	// negative multiplier then the default multiplier will be used.
	WindowConfigurator component.ID `mapstructure:"window_configurator"`
}

// Class defines a named rate limit class for class-based dynamic rate limiting.
type Class struct {
	// Rate holds bucket refill rate, in tokens per second.
	Rate int `mapstructure:"rate"`

	// Burst holds the maximum capacity of rate limit buckets.
	Burst int `mapstructure:"burst"`

	// DisableDynamic disables dynamic rate escalation for this class.
	// When true, effective rate will always be the static Rate.
	DisableDynamic bool `mapstructure:"disable_dynamic"`
}

// Validate checks the DynamicRateLimiting configuration.
func (d *DynamicRateLimiting) Validate() error {
	if !d.Enabled {
		return nil
	}
	var errs []error
	if d.DefaultWindowMultiplier < 1 {
		errs = append(errs, errors.New("default_window_multiplier must be greater than or equal to 1"))
	}
	if d.WindowDuration <= 0 {
		errs = append(errs, errors.New("window_duration must be greater than zero"))
	}
	return errors.Join(errs...)
}

// Validate checks the Class configuration.
func (c *Class) Validate() error {
	var errs []error
	if c.Rate <= 0 {
		errs = append(errs, errors.New("rate must be greater than zero"))
	}
	if c.Burst < 0 {
		errs = append(errs, errors.New("burst must be non-negative"))
	}
	return errors.Join(errs...)
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

	disableDynamic bool `mapstructure:"-"`
}

// RateLimitOverrides defines per-unique-key override settings.
// It replaces the top-level RateLimitSettings fields when the unique key matches.
// Nil pointer fields leave the corresponding top-level field unchanged.
// DisableDynamic disables dynamic escalation for that specific key when true.
type RateLimitOverrides struct {
	// Matches are a map of key-value pairs that MUST be a subset of the
	// incoming client metadata for the override to be applied.
	Matches map[string][]string `mapstructure:"matches"`

	// Rate holds the override rate limit.
	DisableDynamic bool `mapstructure:"disable_dynamic"`

	// Rate holds bucket refill rate, in tokens per second.
	Rate *int `mapstructure:"rate"`

	// Burst holds the maximum capacity of rate limit buckets.
	Burst *int `mapstructure:"burst"`

	// ThrottleInterval holds the time interval for throttling.
	// It only has an effect when the rate limiter type
	// is "gubernator".
	//
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

const (
	GubernatorBehaviorBatching GubernatorBehavior = "batching"
	GubernatorBehaviorGlobal   GubernatorBehavior = "global"
)

func createDefaultConfig() component.Config {
	return &Config{
		Type: LocalRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Strategy:         StrategyRateLimitRequests,
			ThrottleBehavior: ThrottleBehaviorError,
			ThrottleInterval: DefaultThrottleInterval,
			RetryDelay:       DefaultRetryDelay,
		},
		DynamicRateLimiting: DynamicRateLimiting{
			DefaultWindowMultiplier: 1.3,
			WindowDuration:          2 * time.Minute,
		},
		Classes:      nil,
		DefaultClass: "",
	}
}

// resolveRateLimit computes the effective RateLimitSettings for a given unique key.
// It unifies the legacy per-key override resolution and the class-based precedence logic.
// Precedence order:
//  1. Explicit per-key override (SourceKindOverride)
//  2. Resolved class (SourceKindClass)
//  3. DefaultClass (SourceKindClass)
//  4. Top-level fallback config (SourceKindFallback)
//
// When sourceKind is override or fallback, className will be empty.
func resolveRateLimit(
	cfg *Config,
	className string,
	metadata client.Metadata,
) (result RateLimitSettings, kind SourceKind, name string) {
	result = cfg.RateLimitSettings
	// 1. Per-key override takes absolute precedence regardless of classes.
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
			if override.DisableDynamic {
				result.disableDynamic = true
			}
			return result, SourceKindOverride, ""
		}
	}
	// 2. Resolved class (only if provided and exists)
	if className != "" {
		if class, exists := cfg.Classes[className]; exists {
			result.Rate = class.Rate
			if class.Burst > 0 {
				result.Burst = class.Burst
			}
			if class.DisableDynamic {
				result.disableDynamic = true
			}
			return result, SourceKindClass, className
		}
	}
	// 3. DefaultClass (if configured & exists)
	if cfg.DefaultClass != "" {
		if class, exists := cfg.Classes[cfg.DefaultClass]; exists {
			result.Rate = class.Rate
			if class.Burst > 0 {
				result.Burst = class.Burst
			}
			if class.DisableDynamic {
				result.disableDynamic = true
			}
			return result, SourceKindClass, cfg.DefaultClass
		}
	}
	// 4. Fallback to top-level settings.
	return cfg.RateLimitSettings, SourceKindFallback, ""
}

// SourceKind indicates the source of rate limit settings for telemetry.
type SourceKind string

const (
	// SourceKindOverride indicates the settings originated from an explicit per-key override.
	SourceKindOverride SourceKind = "override"
	// SourceKindClass indicates the settings originated from a class (either resolved or default_class).
	SourceKindClass SourceKind = "class"
	// SourceKindFallback indicates the settings originated from the top-level fallback configuration.
	SourceKindFallback SourceKind = "fallback"
)

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
	if config.Type == GubernatorRateLimiter {
		if err := config.DynamicRateLimiting.Validate(); err != nil {
			errs = append(errs, err)
		}
		// Validate class-based configuration
		if config.DefaultClass != "" {
			if len(config.Classes) == 0 {
				errs = append(errs, errors.New("default_class specified but no classes defined"))
			} else if _, exists := config.Classes[config.DefaultClass]; !exists {
				errs = append(errs, fmt.Errorf("default_class %q does not exist in classes", config.DefaultClass))
			}
		}
		for className, class := range config.Classes {
			if err := class.Validate(); err != nil {
				errs = append(errs, fmt.Errorf("class %q: %w", className, err))
			}
		}
		if config.ClassResolver.String() == "" && len(config.Classes) > 0 {
			errs = append(errs, errors.New(
				"classes defined but class_resolver not specified",
			))
		}
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

// Validate ensures the RateLimiterType is one of the supported values.
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

func (b GubernatorBehavior) Validate() error {
	switch b {
	case GubernatorBehaviorBatching, GubernatorBehaviorGlobal:
		return nil
	case "":
		return nil
	}
	return fmt.Errorf(
		"invalid gubernator behavior %q, expected one of %q",
		b, []string{
			string(GubernatorBehaviorBatching),
			string(GubernatorBehaviorGlobal),
		},
	)
}
