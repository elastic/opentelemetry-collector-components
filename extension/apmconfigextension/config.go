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

package apmconfigextension // import "github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension"

import (
	"errors"
	"time"

	"github.com/elastic/opentelemetry-lib/config/configelasticsearch"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/xconfmap"
)

const (
	// Protocol values.
	protoHTTP = "opamp::protocols::http"
)

type Config struct {
	// Source defines the remote configuration source settings.
	Source SourceConfig `mapstructure:"source"`

	// OpAMP defines the configuration for the embedded OpAMP server.
	OpAMP OpAMPConfig `mapstructure:"opamp"`
}

type SourceConfig struct {
	// Elasticsearch configures a fetcher that retrieves remote configuration
	// data from an Elasticsearch cluster.
	Elasticsearch *ElasticsearchFetcher `mapstructure:"elasticsearch"`
}

type ElasticsearchFetcher struct {
	// Elasticsearch client configuration.
	configelasticsearch.ClientConfig `mapstructure:",squash"`

	// CacheDuration specifies how long the fetched remote configuration for an agent
	// should be cached before fetching it again from Elasticsearch.
	CacheDuration time.Duration `mapstructure:"cache_duration"`
}

type OpAMPConfig struct {
	// Protocols is the configuration for the supported protocols, currently
	// HTTP (TBD: websocket).
	Protocols `mapstructure:"protocols"`
	// Cache holds configuration related to agents caching
	Cache CacheConfig `mapstructure:"cache"`
}

type CacheConfig struct {
	// Capacity defines the maximum number of agents to cache.
	// Once this is reached, the least recently
	// used entries will be evicted.
	Capacity uint32 `mapstructure:"capacity"`

	// TTL defines the duration before the cache key gets evicted
	TTL time.Duration `mapstructure:"ttl"`
}

// Protocols is the configuration for the supported protocols.
type Protocols struct {
	ServerConfig *confighttp.ServerConfig `mapstructure:"http"`
	// prevent unkeyed literal initialization
	_ struct{}
}

var (
	_ xconfmap.Validator  = (*Config)(nil)
	_ xconfmap.Validator  = (*ElasticsearchFetcher)(nil)
	_ confmap.Unmarshaler = (*Config)(nil)
	_ component.Config    = (*Config)(nil)
)

// Validate checks the receiver configuration is valid
func (cfg *ElasticsearchFetcher) Validate() error {
	if cfg.CacheDuration <= 0 {
		return errors.New("cache_duration requires positive value")
	}

	return nil
}

// Validate checks the receiver configuration is valid
func (cfg *Config) Validate() error {
	if cfg.OpAMP.Protocols.ServerConfig == nil {
		return errors.New("must specify at least one protocol when using the apmconfig extension")
	}
	return nil
}

// Unmarshal a confmap.Conf into the config struct.
func (cfg *Config) Unmarshal(conf *confmap.Conf) error {
	// first load the config normally
	err := conf.Unmarshal(cfg)
	if err != nil {
		return err
	}

	if !conf.IsSet(protoHTTP) {
		cfg.OpAMP.Protocols.ServerConfig = nil
	}

	return nil
}
