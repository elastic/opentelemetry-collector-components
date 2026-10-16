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

package config // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"

import (
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
)

var _ component.Config = (*Config)(nil)

const (
	// Based on the defaults suggested in https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/sdk.md#base2-exponential-bucket-histogram-aggregation
	defaultMaxExponentialHistogramBuckets = 160
)

type Config struct {
	// Directory is the data directory used by the database to store files.
	// If the directory is empty in-memory storage is used.
	Directory string `mapstructure:"directory"`

	// PassThrough is a configuration that determines whether summary
	// metrics should be passed through as they are or aggregated. This
	// is because they lead to lossy aggregations.
	PassThrough PassThrough `mapstructure:"pass_through"`

	// Intervals is a list of interval configuration that the processor
	// will aggregate over. The interval duration must be in increasing
	// order and must be a factor of the smallest interval duration.
	// TODO (lahsivjar): Make specifying interval easier. We can just
	// optimize the timer to run on differnt times and remove any
	// restriction on different interval configuration.
	Intervals []IntervalConfig `mapstructure:"intervals"`

	// MetadataKeys is a list of client.Metadata keys that will be
	// propagated through the metrics aggregated by the processor.
	//
	// Only the listed metadata keys will be propagated to the
	// resulting metrics.
	//
	// Entries are case-insensitive. Duplicated entries will
	// trigger a validation error.
	MetadataKeys []string `mapstructure:"metadata_keys"`

	ResourceLimit  LimitConfig `mapstructure:"resource_limit"`
	ScopeLimit     LimitConfig `mapstructure:"scope_limit"`
	MetricLimit    LimitConfig `mapstructure:"metric_limit"`
	DatapointLimit LimitConfig `mapstructure:"datapoint_limit"`

	// ExponentialHistogramMaxBuckets sets the maximum number of buckets
	// to use for resulting exponential histograms from merge operations.
	// This allows to bound the maximum number of buckets used by the
	// exponential histograms which could be huge if histograms with
	// precisions ranging in, say, minutes and milliseconds are merged
	// together. Defaults to 160.
	ExponentialHistogramMaxBuckets int `mapstructure:"exponential_histogram_max_buckets"`

	// PreAggregation configures an optional in-memory pre-aggregation
	// buffer in front of the database.
	PreAggregation PreAggregationConfig `mapstructure:"pre_aggregation"`
}

// DefaultPreAggregationMaxSize is the default value of
// PreAggregationConfig.MaxSize.
const DefaultPreAggregationMaxSize int64 = 32 << 20 // 32MiB

// PreAggregationConfig configures the in-memory pre-aggregation buffer.
//
// Without the buffer, every ConsumeMetrics call writes one merge operand per
// interval to the database, and the database merges those operands again on
// every flush and compaction. With the buffer enabled, incoming metrics are
// merged in memory into one value per aggregation key, and the buffer is
// written to the database as a single merge operand per key when it is
// flushed: when MaxSize is reached, when the processing window advances, and
// on shutdown. Aggregated results are identical in both modes.
type PreAggregationConfig struct {
	// Enabled turns on the pre-aggregation buffer. Defaults to false.
	Enabled bool `mapstructure:"enabled"`

	// MaxSize bounds the buffer. It is measured as the total protobuf size
	// of the metrics merged into the buffer since the last flush, which is
	// an upper bound on the memory held by the buffer. When it is reached,
	// the buffer is flushed to the database. Defaults to 32MiB.
	MaxSize int64 `mapstructure:"max_size"`
}

// MaxSizeOrDefault returns the configured MaxSize or the default.
func (c PreAggregationConfig) MaxSizeOrDefault() int64 {
	if c.MaxSize == 0 {
		return DefaultPreAggregationMaxSize
	}
	return c.MaxSize
}

// PassThrough determines whether metrics should be passed through as they
// are or aggregated.
type PassThrough struct {
	// Summary is a flag that determines whether summary metrics should
	// be passed through as they are or aggregated. Since summaries don't
	// have an associated temporality, we assume that summaries are
	// always cumulative.
	Summary bool `mapstructure:"summary"`
}

// IntervalConfig defines the configuration for the intervals that the
// component will aggregate over. OTTL statements are also defined to
// be applied to the metric harvested for each interval after they are
// mature for the interval duration.
type IntervalConfig struct {
	Duration time.Duration `mapstructure:"duration"`
	// Statements are a list of OTTL statements to be executed on the
	// metrics produced for a given interval. The list of available
	// OTTL paths for datapoints can be checked at:
	// https://pkg.go.dev/github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint#section-readme
	// The list of available OTTL editors can be checked at:
	// https://pkg.go.dev/github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs#section-readme
	Statements []string `mapstructure:"statements"`
}

// LimitConfig defines the limits applied over the aggregated metrics.
// After the max cardinality is breached the overflow behaviour kicks in.
type LimitConfig struct {
	MaxCardinality int64          `mapstructure:"max_cardinality"`
	Overflow       OverflowConfig `mapstructure:"overflow"`
}

// OverflowConfig defines the configuration for tweaking the events
// produced after overflow kicks in.
type OverflowConfig struct {
	// Attributes are added to the overflow bucket for the respective
	// limit. For example, attributes defined for an overflow config
	// representing a scope will be added to the overflow scope attributes.
	Attributes []Attribute `mapstructure:"attributes"`
}

// Attribute represent an OTel attribute.
type Attribute struct {
	Key   string `mapstructure:"key"`
	Value any    `mapstructure:"value"`
}

func (cfg *Config) Validate() error {
	// TODO (lahsivjar): Add validation for interval duration
	uniq := map[string]bool{}
	for _, k := range cfg.MetadataKeys {
		l := strings.ToLower(k)
		if _, has := uniq[l]; has {
			return fmt.Errorf("duplicate entry in metadata_keys: %q (case-insensitive)", l)
		}
		uniq[l] = true
	}

	if cfg.ExponentialHistogramMaxBuckets <= 0 {
		return fmt.Errorf(
			"invalid value for exponential_histogram_max_buckets, must be greater than 0, current: %d",
			cfg.ExponentialHistogramMaxBuckets,
		)
	}
	if cfg.PreAggregation.MaxSize < 0 {
		return fmt.Errorf(
			"invalid value for pre_aggregation::max_size, must not be negative, current: %d",
			cfg.PreAggregation.MaxSize,
		)
	}
	return nil
}

func CreateDefaultConfig() component.Config {
	return &Config{
		Intervals: []IntervalConfig{
			{Duration: 60 * time.Second},
		},
		ExponentialHistogramMaxBuckets: defaultMaxExponentialHistogramBuckets,
	}
}
