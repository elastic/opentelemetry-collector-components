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

	// Storage holds optional tuning options for the embedded Pebble
	// database. Zero values select the built-in defaults.
	Storage StorageConfig `mapstructure:"storage"`
}

const (
	// DefaultMemTableSize is the default size of a Pebble memtable.
	DefaultMemTableSize int64 = 32 << 20 // 32MiB
	// DefaultMemTableStopWritesThreshold is the default number of
	// memtables at which writes are stopped until a flush completes.
	DefaultMemTableStopWritesThreshold = 2

	// minMemTableSize is twice the processor's batch commit threshold
	// (8MiB) so that regular batch commits are never classified as
	// large batches by Pebble.
	minMemTableSize int64 = 16 << 20
	// maxMemTableSize is the upper bound enforced by Pebble.
	maxMemTableSize int64 = 4<<30 - 1
)

// StorageConfig tunes the embedded Pebble database. All fields are
// optional; zero values select the defaults.
type StorageConfig struct {
	// MemTableSize is the size in bytes of each Pebble memtable. Larger
	// memtables combine more merge operands for the same key in a single
	// flush, which reduces flush CPU and write stalls under sustained
	// load at the cost of memory. Defaults to 32MiB. When set, it must be
	// at least 16MiB and less than 4GiB.
	MemTableSize int64 `mapstructure:"memtable_size"`

	// MemTableStopWritesThreshold is the number of memtables (the active
	// memtable plus those queued for flushing) at which writes stop until
	// a flush completes. Raising it absorbs flush latency instead of
	// stalling ingestion. Peak memtable memory is approximately
	// MemTableSize * MemTableStopWritesThreshold. Defaults to 2. When
	// set, it must be at least 2.
	MemTableStopWritesThreshold int `mapstructure:"memtable_stop_writes_threshold"`

	// SyncWrites controls whether each batch commit waits for the
	// write-ahead log to be fsynced. Defaults to true. Disabling it
	// removes fsync from the ingestion path, but writes since the last
	// sync can be lost if the node crashes, and on disks with a
	// throughput cap it can increase stalls unless WALBytesPerSync is
	// also set. Ignored when directory is empty (in-memory mode).
	SyncWrites *bool `mapstructure:"sync_writes"`

	// WALBytesPerSync makes Pebble sync the write-ahead log in the
	// background every WALBytesPerSync bytes. This spreads WAL writeback
	// evenly instead of leaving it for a single large sync when the WAL
	// rotates. It is mainly useful with SyncWrites disabled. Defaults
	// to 0 (disabled). Ignored when directory is empty (in-memory mode).
	WALBytesPerSync int64 `mapstructure:"wal_bytes_per_sync"`
}

// SyncWritesOrDefault returns whether batch commits should sync the WAL.
func (c StorageConfig) SyncWritesOrDefault() bool {
	if c.SyncWrites == nil {
		return true
	}
	return *c.SyncWrites
}

// MemTableSizeOrDefault returns the configured memtable size or the default.
func (c StorageConfig) MemTableSizeOrDefault() int64 {
	if c.MemTableSize == 0 {
		return DefaultMemTableSize
	}
	return c.MemTableSize
}

// MemTableStopWritesThresholdOrDefault returns the configured threshold or
// the default.
func (c StorageConfig) MemTableStopWritesThresholdOrDefault() int {
	if c.MemTableStopWritesThreshold == 0 {
		return DefaultMemTableStopWritesThreshold
	}
	return c.MemTableStopWritesThreshold
}

// Validate validates the storage configuration.
func (c StorageConfig) Validate() error {
	if c.MemTableSize != 0 && (c.MemTableSize < minMemTableSize || c.MemTableSize > maxMemTableSize) {
		return fmt.Errorf(
			"invalid value for storage::memtable_size, must be between %d and %d bytes, current: %d",
			minMemTableSize, maxMemTableSize, c.MemTableSize,
		)
	}
	if c.WALBytesPerSync < 0 {
		return fmt.Errorf(
			"invalid value for storage::wal_bytes_per_sync, must not be negative, current: %d",
			c.WALBytesPerSync,
		)
	}
	if c.MemTableStopWritesThreshold != 0 && c.MemTableStopWritesThreshold < 2 {
		return fmt.Errorf(
			"invalid value for storage::memtable_stop_writes_threshold, must be at least 2, current: %d",
			c.MemTableStopWritesThreshold,
		)
	}
	return nil
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
	return cfg.Storage.Validate()
}

func CreateDefaultConfig() component.Config {
	return &Config{
		Intervals: []IntervalConfig{
			{Duration: 60 * time.Second},
		},
		ExponentialHistogramMaxBuckets: defaultMaxExponentialHistogramBuckets,
	}
}
