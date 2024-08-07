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
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"

	"go.elastic.co/apm/v2/apmconfig"
	"go.elastic.co/apm/v2/transport"
	semconv "go.opentelemetry.io/collector/semconv/v1.5.0"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	otelapmconfig "github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/open-telemetry/opamp-go/protobufs"
)

type APMClient struct {
	client apmconfig.Watcher

	agentsCtx       context.Context
	agentsCancelCtx context.CancelFunc

	logger *zap.Logger
}

func initialTransport(opts transport.HTTPTransportOptions) (transport.Transport, error) {
	// User-Agent should be "apm-agent-go/<agent-version> (service-name service-version)".
	httpTransport, err := transport.NewHTTPTransport(opts)
	if err != nil {
		return nil, err
	}
	return httpTransport, nil
}

// TODO: move config to type instead of map[string]string
func configHash(encodedConfig []byte) ([]byte, error) {
	hasher := sha256.New()
	_, err := hasher.Write(encodedConfig)
	if err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

func changeToConfig(change apmconfig.Change) (otelapmconfig.RemoteConfig, error) {
	if change.Err != nil {
		return otelapmconfig.RemoteConfig{}, change.Err
	} else if len(change.Attrs) == 0 {
		return otelapmconfig.RemoteConfig{}, nil
	}

	encodedConfig, err := yaml.Marshal(change.Attrs)
	if err != nil {
		return otelapmconfig.RemoteConfig{}, err
	}

	configHash, err := configHash(encodedConfig)
	if err != nil {
		return otelapmconfig.RemoteConfig{}, err
	}

	return otelapmconfig.RemoteConfig{Hash: configHash, Attrs: change.Attrs}, nil
}

func (c *APMClient) Close() error {
	c.agentsCancelCtx()
	return nil
}

type AgentAPMClient struct {
	cancelFunc context.CancelFunc
	changes    <-chan apmconfig.Change
}

func NewAgentAPMClient(ctx context.Context, apmClient apmconfig.Watcher, params apmconfig.WatchParams) *AgentAPMClient {
	ctx, cancelFn := context.WithCancel(ctx)
	changes := apmClient.WatchConfig(ctx, params)

	return &AgentAPMClient{
		cancelFn,
		changes,
	}
}

func (a *AgentAPMClient) RemoteConfig(ctx context.Context) (otelapmconfig.RemoteConfig, error) {
	select {
	case change := <-a.changes:
		return changeToConfig(change)
	case <-ctx.Done():
	default:
	}
	return otelapmconfig.RemoteConfig{}, nil
}

func (a *AgentAPMClient) Disconnect(ctx context.Context) error {
	a.cancelFunc()
	return nil
}

func (c *APMClient) RemoteConfigClient(ctx context.Context, agentMsg *protobufs.AgentToServer) (otelapmconfig.RemoteConfigClient, error) {
	var params apmconfig.WatchParams
	if agentMsg.AgentDescription != nil {
		for _, attr := range agentMsg.GetAgentDescription().GetIdentifyingAttributes() {
			switch attr.GetKey() {
			case semconv.AttributeServiceName:
				params.Service.Name = attr.GetValue().GetStringValue()
			case semconv.AttributeDeploymentEnvironment:
				params.Service.Environment = attr.GetValue().GetStringValue()
			}
		}
	}
	if params.Service.Name == "" {
		return nil, errors.New("unidentified agent: service.name attribute must be provided")
	}
	return NewAgentAPMClient(c.agentsCtx, c.client, params), nil
}

func NewCentralConfigClient(urls []*url.URL, token string, logger *zap.Logger) (*APMClient, error) {
	userAgent := fmt.Sprintf("%s (%s)", transport.DefaultUserAgent(), "apmconfigextension/0.0.1")
	transportOpts := transport.HTTPTransportOptions{
		UserAgent:   userAgent,
		SecretToken: token,
		ServerURLs:  urls,
	}
	initialTransport, err := initialTransport(transportOpts)
	if err != nil {
		return nil, err
	}

	if cw, ok := initialTransport.(apmconfig.Watcher); ok {
		agentsCtx, agentsCancelCtx := context.WithCancel(context.Background())
		return &APMClient{
			cw,
			agentsCtx,
			agentsCancelCtx,
			logger,
		}, nil
	}

	return nil, errors.New("transport does not implement the Watcher")
}
