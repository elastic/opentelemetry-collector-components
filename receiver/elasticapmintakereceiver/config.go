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

package elasticapmintakereceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver"

import (
	"fmt"
	"time"

	"github.com/elastic/opentelemetry-lib/config/configelasticsearch"
	"go.opentelemetry.io/collector/config/confighttp"

	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/ndjsondecoder"
)

// Config defines configuration for the Elastic APM receiver.
type Config struct {
	// When using APM agent configuration, information fetched from Elasticsearch will be cached in memory for some time.
	AgentConfig AgentConfig `mapstructure:"agent_config"`

	// BatchBytes is the accumulated size, in raw NDJSON bytes, of intake v2
	// events decoded and processed together. A batch may exceed it by up to
	// one max_event_size before flushing, and the decoded in-memory
	// representation is typically ~5-7x the raw size.
	BatchBytes int `mapstructure:"batch_bytes"`

	// BatchFlushInterval is the maximum time decoded events are buffered
	// waiting for their batch to fill before being processed anyway. The
	// bound is checked as events arrive; an idle stream's buffered tail is
	// processed on the next event or at end of stream. Zero disables the
	// bound.
	BatchFlushInterval time.Duration `mapstructure:"batch_flush_interval"`

	// MaxConcurrentDecoders is the maximum number of intake requests whose bodies
	// can be decoded concurrently. Zero disables the limit entirely (no
	// concurrency gating).
	MaxConcurrentDecoders int `mapstructure:"max_concurrent_decoders"`

	// MaxEventSize is the maximum allowed event size, in bytes.
	MaxEventSize int `mapstructure:"max_event_size"`

	confighttp.ServerConfig `mapstructure:",squash"`
}

type AgentConfig struct {
	// AgentConfig fetcher is disabled by default.
	Enabled bool `mapstructure:"enabled"`

	// Elasticsearch contains the configuration options used to connect to
	// an Elasticsearch instance to retrieve the APM Central Configurations.
	// Configuration options can be found in https://github.com/elastic/opentelemetry-lib/blob/main/config/configelasticsearch/configclient.go#L69
	Elasticsearch configelasticsearch.ClientConfig `mapstructure:"elasticsearch"`

	// CacheDuration duration defines the timeout to fetch and update agent
	// configurations. Default is 30 seconds.
	CacheDuration time.Duration `mapstructure:"cache_duration"`
}

// streamConfig maps the receiver configuration to the NDJSON stream
// decoder's batching bounds.
func (cfg *Config) streamConfig() ndjsondecoder.Config {
	return ndjsondecoder.Config{
		BatchBytes:    cfg.BatchBytes,
		FlushInterval: cfg.BatchFlushInterval,
		MaxLineLength: cfg.MaxEventSize,
	}
}

// Validate checks the receiver configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.BatchBytes <= 0 {
		return fmt.Errorf("batch_bytes must be positive")
	}
	if cfg.BatchFlushInterval < 0 {
		return fmt.Errorf("batch_flush_interval must not be negative (0 disables the bound)")
	}
	if cfg.MaxConcurrentDecoders < 0 {
		return fmt.Errorf("max_concurrent_decoders must not be negative (0 disables the limit)")
	}
	if cfg.MaxEventSize <= 0 {
		return fmt.Errorf("max_event_size must be positive")
	}
	return cfg.AgentConfig.Elasticsearch.Validate()
}
