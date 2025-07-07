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
	"time"

	"github.com/elastic/opentelemetry-lib/config/configelasticsearch"
	"go.opentelemetry.io/collector/config/confighttp"
)

// Config defines configuration for the Elastic APM receiver.
type Config struct {
	// When using APM agent configuration, information fetched from Elasticsearch will be cached in memory for some time.
	AgentConfig AgentConfig `mapstructure:"agent_config"`

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

// Validate checks the receiver configuration is valid.
func (cfg *Config) Validate() error {
	return cfg.AgentConfig.Elasticsearch.Validate()
}
