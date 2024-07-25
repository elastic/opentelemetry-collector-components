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

package centralconfig

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"

	"go.elastic.co/apm/v2/apmconfig"
	"go.elastic.co/apm/v2/transport"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	otelapmconfig "github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
)

type APMClient struct {
	client apmconfig.Watcher

	myCtx context.Context

	logger *zap.Logger

	agents            map[string]<-chan apmconfig.Change
	agentscancelFuncs map[string]context.CancelFunc
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

func (c *APMClient) agentChange(ctx context.Context, agentUid string) apmconfig.Change {
	var change apmconfig.Change
	if changeChan, ok := c.agents[agentUid]; ok {
		select {
		case change = <-changeChan:
		case <-ctx.Done():
		default:
		}
	}
	return change
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

func (c *APMClient) RemoteConfig(ctx context.Context, agentParams otelapmconfig.Params) (otelapmconfig.RemoteConfig, error) {
	params := apmconfig.WatchParams{}
	params.Service.Name = fmt.Sprintf(agentParams.Service.Name)
	params.Service.Environment = agentParams.Service.Environment

	var change apmconfig.Change
	if _, ok := c.agents[agentParams.AgentUiD]; !ok {
		if params.Service.Name == "" {
			return otelapmconfig.RemoteConfig{}, errors.New("unidentified agent: service.name attribute must be provided")
		}
		ctx, cancelFn := context.WithCancel(c.myCtx)
		c.agentscancelFuncs[agentParams.AgentUiD] = cancelFn
		c.agents[agentParams.AgentUiD] = c.client.WatchConfig(ctx, params)

		change = <-c.agents[agentParams.AgentUiD]

	} else {
		// non blocking call if already received first config
		change = c.agentChange(ctx, agentParams.AgentUiD)
	}

	return changeToConfig(change)
}

func (c *APMClient) Close() error {
	for i := range c.agentscancelFuncs {
		c.agentscancelFuncs[i]()
	}

	return nil
}

// func (c *APMClient) LastConfig(context.Context, otelapmconfig.Params, []byte) error {
// 	// TODO: update transport to notify latest applied remote config
// 	return nil
// }

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
		return &APMClient{
			cw,
			context.Background(),
			logger,
			make(map[string]<-chan apmconfig.Change),
			make(map[string]context.CancelFunc),
		}, nil
	}

	return nil, errors.New("transport does not implement the Watcher")
}
