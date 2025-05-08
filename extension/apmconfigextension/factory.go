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

func createDefaultConfig() component.Config {
	defaultElasticSearchClient := configelasticsearch.NewDefaultClientConfig()
	return &Config{
		AgentConfig: AgentConfig{
			Elasticsearch: defaultElasticSearchClient,
			// using apm-server default
			CacheDuration: 30 * time.Second,
		},
		OpAMP: OpAMPConfig{
			Server: OpAMPServerConfig{
				Endpoint: "127.0.0.1:4320",
			},
		},
	}
}

func createExtension(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	extCfg := cfg.(*Config)
	elasticsearchRemoteConfig := func(ctx context.Context, host component.Host, telemetry component.TelemetrySettings) (apmconfig.RemoteConfigClient, error) {
		esClient, err := extCfg.AgentConfig.Elasticsearch.ToClient(ctx, host, telemetry)
		if err != nil {
			return nil, err
		}
		fetcher := agentcfg.NewElasticsearchFetcher(esClient, extCfg.AgentConfig.CacheDuration, telemetry.Logger)

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
