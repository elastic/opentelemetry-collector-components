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

package config

import (
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

func TestConfig(t *testing.T) {
	for _, tc := range []struct {
		name           string
		input          map[string]any
		expected       *Config
		expectedErrMsg string
	}{
		{
			name:  "empty",
			input: nil,
			expected: func() *Config {
				return CreateDefaultConfig().(*Config)
			}(),
		},
		{
			name: "duplicate_metadata",
			input: map[string]any{
				"metadata_keys": []string{"test.1", "test.2", "test.1"},
			},
			expectedErrMsg: "duplicate entry in metadata_keys",
		},
		{
			name: "invalid_max_buckets",
			input: map[string]any{
				"exponential_histogram_max_buckets": -8,
			},
			expectedErrMsg: "invalid value for exponential_histogram_max_buckets",
		},
		{
			name: "memtable_size_too_small",
			input: map[string]any{
				"storage": map[string]any{"memtable_size": 1 << 20},
			},
			expectedErrMsg: "invalid value for storage::memtable_size",
		},
		{
			name: "memtable_size_too_large",
			input: map[string]any{
				"storage": map[string]any{"memtable_size": int64(4 << 30)},
			},
			expectedErrMsg: "invalid value for storage::memtable_size",
		},
		{
			name: "memtable_stop_writes_threshold_too_small",
			input: map[string]any{
				"storage": map[string]any{"memtable_stop_writes_threshold": 1},
			},
			expectedErrMsg: "invalid value for storage::memtable_stop_writes_threshold",
		},
		{
			name: "max_concurrent_compactions_too_small",
			input: map[string]any{
				"storage": map[string]any{"max_concurrent_compactions": 0},
			},
			expectedErrMsg: "invalid value for storage::max_concurrent_compactions",
		},
		{
			name: "valid_storage",
			input: map[string]any{
				"storage": map[string]any{
					"memtable_size":                  64 << 20,
					"memtable_stop_writes_threshold": 4,
					"max_concurrent_compactions":     2,
				},
			},
			expected: func() *Config {
				cfg := CreateDefaultConfig().(*Config)
				cfg.Storage = StorageConfig{
					MemTableSize:                64 << 20,
					MemTableStopWritesThreshold: 4,
					MaxConcurrentCompactions:    2,
				}
				return cfg
			}(),
		},
		{
			name: "negative_pre_aggregation_max_size",
			input: map[string]any{
				"pre_aggregation": map[string]any{"enabled": true, "max_size": -1},
			},
			expectedErrMsg: "invalid value for pre_aggregation::max_size",
		},
		{
			name: "valid_pre_aggregation",
			input: map[string]any{
				"pre_aggregation": map[string]any{"enabled": true, "max_size": 1 << 20},
			},
			expected: func() *Config {
				cfg := CreateDefaultConfig().(*Config)
				cfg.PreAggregation = PreAggregationConfig{Enabled: true, MaxSize: 1 << 20}
				return cfg
			}(),
		},
		{
			name: "valid_full",
			input: map[string]any{
				"metadata_keys":                     []string{"test.1", "test.2"},
				"exponential_histogram_max_buckets": 256,
			},
			expected: func() *Config {
				cfg := CreateDefaultConfig().(*Config)
				cfg.MetadataKeys = []string{"test.1", "test.2"}
				cfg.ExponentialHistogramMaxBuckets = 256
				return cfg
			}(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Add `lsminterval` key as a top level key to emulate actual configs
			conf := confmap.NewFromStringMap(map[string]any{"lsminterval": tc.input})
			subConf, err := conf.Sub(component.NewIDWithName(metadata.Type, "").String())
			require.NoError(t, err)

			actual := CreateDefaultConfig()
			require.NoError(t, subConf.Unmarshal(&actual))

			err = confmap.Validate(actual)
			if tc.expectedErrMsg != "" {
				assert.ErrorContains(t, err, tc.expectedErrMsg)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, actual)
			}
		})
	}
}

func TestStorageConfigDefaults(t *testing.T) {
	expected := StorageConfig{
		MemTableSize:                DefaultMemTableSize,
		MemTableStopWritesThreshold: DefaultMemTableStopWritesThreshold,
		MaxConcurrentCompactions:    DefaultMaxConcurrentCompactions,
	}
	assert.Equal(t, expected, DefaultStorageConfig())
	assert.Equal(t, expected, CreateDefaultConfig().(*Config).Storage)
	assert.NoError(t, DefaultStorageConfig().Validate())
	// Defaults come from CreateDefaultConfig, not from zero values.
	assert.Error(t, StorageConfig{}.Validate())
}

func TestPreAggregationMaxSizeOrDefault(t *testing.T) {
	assert.Equal(t, DefaultPreAggregationMaxSize, PreAggregationConfig{}.MaxSizeOrDefault())
	assert.Equal(t, int64(1<<20), PreAggregationConfig{MaxSize: 1 << 20}.MaxSizeOrDefault())
}
