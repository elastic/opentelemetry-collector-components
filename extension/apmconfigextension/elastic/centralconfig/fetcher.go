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

package centralconfig // import "github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/elastic/centralconfig"

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/elastic/opentelemetry-lib/agentcfg"
	"github.com/open-telemetry/opamp-go/protobufs"
	semconv "go.opentelemetry.io/otel/semconv/v1.28.0"
	"go.uber.org/zap"
)

const configContentType = "text/json"

var _ apmconfig.RemoteConfigClient = (*fetcherAPMWatcher)(nil)

type fetcherAPMWatcher struct {
	configFetcher agentcfg.Fetcher
	logger        *zap.Logger
}

func NewFetcherAPMWatcher(fetcher agentcfg.Fetcher, logger *zap.Logger) *fetcherAPMWatcher {
	return &fetcherAPMWatcher{
		configFetcher: fetcher,
		logger:        logger,
	}
}

func (fw *fetcherAPMWatcher) RemoteConfig(ctx context.Context, agentUid apmconfig.InstanceUid, agentAttrs apmconfig.IdentifyingAttributes) (*protobufs.AgentRemoteConfig, error) {
	var serviceParams agentcfg.Service
	for _, attr := range agentAttrs {
		switch attr.GetKey() {
		case string(semconv.ServiceNameKey):
			serviceParams.Name = attr.GetValue().GetStringValue()
		case string(semconv.DeploymentEnvironmentNameKey):
			serviceParams.Environment = attr.GetValue().GetStringValue()
		}
	}

	if serviceParams.Name == "" {
		return nil, fmt.Errorf("%w: service.name attribute must be provided", apmconfig.UnidentifiedAgent)
	}
	result, err := fw.configFetcher.Fetch(ctx, agentcfg.Query{
		Service: serviceParams,
	})
	if err != nil {
		return nil, err
	} else if len(result.Source.Settings) == 0 {
		return nil, nil
	}

	marshallConfig, err := json.Marshal(result.Source.Settings)
	if err != nil {
		return nil, err
	}

	return &protobufs.AgentRemoteConfig{ConfigHash: []byte(result.Source.Etag), Config: &protobufs.AgentConfigMap{
		ConfigMap: map[string]*protobufs.AgentConfigFile{
			"": {
				Body:        marshallConfig,
				ContentType: configContentType,
			},
		},
	}}, nil
}
