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
	"time"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/elastic/opentelemetry-lib/agentcfg"
	"github.com/open-telemetry/opamp-go/protobufs"
	semconv "go.opentelemetry.io/collector/semconv/v1.26.0"
	"go.uber.org/zap"
)

const configContentType = "text/json"

var _ apmconfig.RemoteConfigClient = (*fetcherAPMWatcher)(nil)

type fetcherAPMWatcher struct {
	configFetcher agentcfg.Fetcher
	cacheDuration time.Duration

	// OpAMP instanceID to service mapping
	uidToService map[string]agentcfg.Service

	logger *zap.Logger
}

func NewFetcherAPMWatcher(fetcher agentcfg.Fetcher, cacheDuration time.Duration, logger *zap.Logger) *fetcherAPMWatcher {
	return &fetcherAPMWatcher{
		configFetcher: fetcher,
		cacheDuration: cacheDuration,
		uidToService:  make(map[string]agentcfg.Service),
		logger:        logger,
	}
}

func (fw *fetcherAPMWatcher) RemoteConfig(ctx context.Context, agentMsg *protobufs.AgentToServer) (*protobufs.AgentRemoteConfig, error) {
	if agentDescription := agentMsg.GetAgentDescription(); agentDescription != nil {
		var serviceParams agentcfg.Service
		for _, attr := range agentDescription.GetIdentifyingAttributes() {
			switch attr.GetKey() {
			case semconv.AttributeServiceName:
				serviceParams.Name = attr.GetValue().GetStringValue()
			case semconv.AttributeDeploymentEnvironment:
				serviceParams.Environment = attr.GetValue().GetStringValue()
			}
		}
		// only update the internal cache if service name is set
		if serviceParams.Name != "" {
			fw.uidToService[string(agentMsg.GetInstanceUid())] = serviceParams
		}
	}

	serviceParams, ok := fw.uidToService[string(agentMsg.GetInstanceUid())]
	if !ok || serviceParams.Name == "" {
		return nil, fmt.Errorf("%w: service.name attribute must be provided", apmconfig.UnidentifiedAgent)
	}
	result, err := fw.configFetcher.Fetch(ctx, agentcfg.Query{
		Service: serviceParams,
	})
	if err != nil {
		return nil, err
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
