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

	"go.opentelemetry.io/collector/config/confighttp"

	"github.com/elastic/opentelemetry-lib/config/configelasticsearch"
)

// Config defines configuration for the Elastic APM receiver.
type Config struct {
	// When using APM agent configuration, information fetched from Elasticsearch will be cached in memory for some time.
	AgentConfig AgentConfig `mapstructure:"agent_config"`

	// PreserveAllHistogramBounds keeps the last explicit bound (upper boundary)
	// in histogram data points, producing len(bucket_counts) == len(explicit_bounds).
	// This deviates from the OTel spec but allows the ES exporter to detect
	// APM intake histograms by heuristic (requires opentelemetry-collector-contrib#46831).
	// Default: false (standard OTel format: len(bucket_counts) == len(explicit_bounds) + 1).
	//
	// This can be removed once the ES exporter heuristic detection has been released for a few versions.
	PreserveAllHistogramBounds bool `mapstructure:"preserve_all_histogram_bounds"`

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
