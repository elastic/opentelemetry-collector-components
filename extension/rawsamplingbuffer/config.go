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

package rawsamplingbuffer

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config represents the configuration for the rawsamplingbuffer extension.
type Config struct {
	// BufferSize is the maximum number of entries in the buffer.
	BufferSize int `mapstructure:"buffer_size"`

	// MaxEntrySize is the maximum size in bytes per log entry.
	MaxEntrySize int `mapstructure:"max_entry_size"`

	// TTL specifies how long entries live in the buffer.
	TTL time.Duration `mapstructure:"ttl"`

	// EvictionPolicy determines how entries are evicted when buffer is full.
	// Supported values: "lru" (least recently used)
	EvictionPolicy string `mapstructure:"eviction_policy"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the extension configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.BufferSize <= 0 {
		return errors.New("buffer_size must be greater than 0")
	}
	if cfg.MaxEntrySize <= 0 {
		return errors.New("max_entry_size must be greater than 0")
	}
	if cfg.TTL <= 0 {
		return errors.New("ttl must be greater than 0")
	}
	if cfg.EvictionPolicy != "lru" {
		return errors.New("eviction_policy must be 'lru'")
	}
	return nil
}
