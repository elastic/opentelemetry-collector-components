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
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/extension"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/elastic/centralconfig"
	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/internal/metadata"
	"github.com/elastic/opentelemetry-lib/agentcfg"
	"github.com/elastic/opentelemetry-lib/config/configelasticsearch"
)

func NewFactory() extension.Factory {
	return extension.NewFactory(
		metadata.Type,
		createDefaultConfig,
		createExtension,
		metadata.ExtensionStability,
	)
}

var defaultCacheConfig = CacheConfig{
	// Cache capacity for active agents
	Capacity: 1024,
	// TTL for each cached agent entry (30s heartbeat interval)
	// Allows ~4 missed heartbeats before cache eviction
	TTL: 30 * 4 * time.Second,
}

func createDefaultConfig() component.Config {
	defaultElasticSearchClient := configelasticsearch.NewDefaultClientConfig()
	httpCfg := confighttp.NewDefaultServerConfig()
	httpCfg.Endpoint = "localhost:4320"

	return &Config{
		Source: SourceConfig{
			Elasticsearch: &ElasticsearchFetcher{
				ClientConfig: defaultElasticSearchClient,
				// using apm-server default
				CacheDuration: 30 * time.Second,
			},
		},
		OpAMP: OpAMPConfig{
			Protocols: Protocols{
				ServerConfig: &httpCfg,
			},
			Cache: defaultCacheConfig,
		},
	}
}

func createExtension(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	extCfg := cfg.(*Config)
	elasticsearchRemoteConfig := func(ctx context.Context, host component.Host, telemetry component.TelemetrySettings) (apmconfig.RemoteConfigClient, error) {
		esClient, err := extCfg.Source.Elasticsearch.ClientConfig.ToClient(ctx, host, telemetry)
		if err != nil {
			return nil, err
		}
		fetcher := agentcfg.NewElasticsearchFetcher(esClient, extCfg.Source.Elasticsearch.CacheDuration, telemetry.Logger)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			// ensure Go routine is scheduled
			wg.Done()
			if err := fetcher.Run(ctx); err != nil {
				set.Logger.Error(err.Error())
			}
		}()

		wg.Wait()
		return centralconfig.NewFetcherAPMWatcher(fetcher, telemetry.Logger), nil
	}
	return newApmConfigExtension(cfg.(*Config), set, elasticsearchRemoteConfig), nil
}
