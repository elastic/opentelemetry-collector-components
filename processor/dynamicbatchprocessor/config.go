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

package dynamicbatchprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/dynamicbatchprocessor"

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config configures the dynamic batch processor.
type Config struct {
	// QueueBatch config applied to each per-metadata shard.
	exporterhelper.QueueBatchConfig `mapstructure:",squash"`

	// MetadataKeys are the client metadata header keys to shard on (must be lowercase).
	// Each unique combination of values gets its own queue+batcher instance.
	MetadataKeys []string `mapstructure:"metadata_keys"`

	// MetadataCardinalityLimit caps the number of concurrent live shards.
	// Requests for a new metadata combination beyond this limit return an error.
	// 0 means unlimited.
	MetadataCardinalityLimit int `mapstructure:"metadata_cardinality_limit"`

	// IdleTimeout is how long a shard can receive no traffic before being flushed and shut down.
	// The GC goroutine runs every IdleTimeout/2.
	IdleTimeout time.Duration `mapstructure:"idle_timeout"`
}

func (cfg *Config) Validate() error {
	if len(cfg.MetadataKeys) == 0 {
		return errors.New("metadata_keys must not be empty")
	}
	for i, k := range cfg.MetadataKeys {
		if strings.ToLower(k) != k {
			return fmt.Errorf("metadata_keys[%d] %q must be lowercase", i, k)
		}
	}
	if cfg.MetadataCardinalityLimit < 0 {
		return errors.New("metadata_cardinality_limit must be non-negative")
	}
	if cfg.IdleTimeout <= 0 {
		return errors.New("idle_timeout must be positive")
	}
	return cfg.QueueBatchConfig.Validate()
}
