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

package apmconfigextension

import (
	"context"
	"errors"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/server/types"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type remoteConfigMock struct {
	remoteConfigFn func(context.Context, *protobufs.AgentToServer) (apmconfig.RemoteConfig, error)
}

func (f *remoteConfigMock) RemoteConfig(ctx context.Context, msg *protobufs.AgentToServer) (apmconfig.RemoteConfig, error) {
	return f.remoteConfigFn(ctx, msg)
}

func TestOnMessage(t *testing.T) {
	type inOutOpamp struct {
		agentToServer         *protobufs.AgentToServer
		expectedServerToAgent *protobufs.ServerToAgent
	}

	testcases := map[string]struct {
		opampMessages []inOutOpamp
		callbacks     *types.Callbacks
	}{
		"empty AgentToServer message, no instance_uid": {
			opampMessages: []inOutOpamp{
				{
					agentToServer: &protobufs.AgentToServer{},
					expectedServerToAgent: &protobufs.ServerToAgent{
						Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
						Flags:        uint64(protobufs.ServerToAgentFlags_ServerToAgentFlags_ReportFullState),
						ErrorResponse: &protobufs.ServerErrorResponse{
							ErrorMessage: "instance_uid must be provided",
							Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_Unknown,
						},
					},
				},
			},
			callbacks: newRemoteConfigCallbacks(&remoteConfigMock{
				remoteConfigFn: func(context.Context, *protobufs.AgentToServer) (apmconfig.RemoteConfig, error) {
					return apmconfig.RemoteConfig{}, nil
				},
			}, zap.NewNop()),
		},
		"remote config provider error": {
			opampMessages: []inOutOpamp{
				{
					agentToServer: &protobufs.AgentToServer{
						InstanceUid: []byte("test"),
					},
					expectedServerToAgent: &protobufs.ServerToAgent{
						InstanceUid:  []byte("test"),
						Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
						ErrorResponse: &protobufs.ServerErrorResponse{
							ErrorMessage: "error retrieving remote configuration: testing error",
							Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_Unknown,
						},
					},
				},
			},
			callbacks: newRemoteConfigCallbacks(&remoteConfigMock{
				remoteConfigFn: func(context.Context, *protobufs.AgentToServer) (apmconfig.RemoteConfig, error) {
					return apmconfig.RemoteConfig{}, errors.New("testing error")
				},
			}, zap.NewNop()),
		},
		"unidentified agent": {
			opampMessages: []inOutOpamp{
				{
					agentToServer: &protobufs.AgentToServer{
						InstanceUid: []byte("test"),
					},
					expectedServerToAgent: &protobufs.ServerToAgent{
						InstanceUid:  []byte("test"),
						Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
						Flags:        uint64(protobufs.ServerToAgentFlags_ServerToAgentFlags_ReportFullState),
						ErrorResponse: &protobufs.ServerErrorResponse{
							ErrorMessage: "error retrieving remote configuration: agent could not be identified",
							Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_Unknown,
						},
					},
				},
			},
			callbacks: newRemoteConfigCallbacks(&remoteConfigMock{
				remoteConfigFn: func(context.Context, *protobufs.AgentToServer) (apmconfig.RemoteConfig, error) {
					return apmconfig.RemoteConfig{}, apmconfig.UnidentifiedAgent
				},
			}, zap.NewNop()),
		},
		"agent without config applies remote": {
			opampMessages: []inOutOpamp{
				{
					agentToServer: &protobufs.AgentToServer{
						InstanceUid: []byte("test"),
					},
					expectedServerToAgent: &protobufs.ServerToAgent{
						InstanceUid:  []byte("test"),
						Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
						RemoteConfig: &protobufs.AgentRemoteConfig{
							ConfigHash: []byte("abcd"),
							Config: &protobufs.AgentConfigMap{
								ConfigMap: map[string]*protobufs.AgentConfigFile{
									"": {
										Body:        []byte(`{"test":"aaa"}`),
										ContentType: "text/json",
									},
								},
							},
						},
					},
				},
				{
					agentToServer: &protobufs.AgentToServer{
						InstanceUid: []byte("test"),
						RemoteConfigStatus: &protobufs.RemoteConfigStatus{
							LastRemoteConfigHash: []byte("abcd"),
							Status:               protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED,
						},
					},
					expectedServerToAgent: &protobufs.ServerToAgent{
						InstanceUid:  []byte("test"),
						Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
					},
				},
			},
			callbacks: newRemoteConfigCallbacks(&remoteConfigMock{
				remoteConfigFn: func(context.Context, *protobufs.AgentToServer) (apmconfig.RemoteConfig, error) {
					return apmconfig.RemoteConfig{
						Hash:  []byte("abcd"),
						Attrs: map[string]string{"test": "aaa"},
					}, nil
				},
			}, zap.NewNop()),
		},
		"agent applying remote config": {
			opampMessages: []inOutOpamp{
				{
					agentToServer: &protobufs.AgentToServer{
						InstanceUid: []byte("test"),
						RemoteConfigStatus: &protobufs.RemoteConfigStatus{
							LastRemoteConfigHash: []byte("abcd"),
							Status:               protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLYING,
						},
					},
					expectedServerToAgent: &protobufs.ServerToAgent{
						InstanceUid:  []byte("test"),
						Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
						RemoteConfig: &protobufs.AgentRemoteConfig{
							ConfigHash: []byte("abcd"),
							Config: &protobufs.AgentConfigMap{
								ConfigMap: map[string]*protobufs.AgentConfigFile{
									"": {
										Body:        []byte(`{"test":"aaa"}`),
										ContentType: "text/json",
									},
								},
							},
						},
					},
				},
			},
			callbacks: newRemoteConfigCallbacks(&remoteConfigMock{
				remoteConfigFn: func(context.Context, *protobufs.AgentToServer) (apmconfig.RemoteConfig, error) {
					return apmconfig.RemoteConfig{
						Hash:  []byte("abcd"),
						Attrs: map[string]string{"test": "aaa"},
					}, nil
				},
			}, zap.NewNop()),
		},
		"agent applies with old remote config": {
			opampMessages: []inOutOpamp{
				{
					agentToServer: &protobufs.AgentToServer{
						InstanceUid: []byte("test"),
						RemoteConfigStatus: &protobufs.RemoteConfigStatus{
							LastRemoteConfigHash: []byte("old config"),
							Status:               protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED,
						},
					},
					expectedServerToAgent: &protobufs.ServerToAgent{
						InstanceUid:  []byte("test"),
						Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
						RemoteConfig: &protobufs.AgentRemoteConfig{
							ConfigHash: []byte("abcd"),
							Config: &protobufs.AgentConfigMap{
								ConfigMap: map[string]*protobufs.AgentConfigFile{
									"": {
										Body:        []byte(`{"test":"aaa"}`),
										ContentType: "text/json",
									},
								},
							},
						},
					},
				},
			},
			callbacks: newRemoteConfigCallbacks(&remoteConfigMock{
				remoteConfigFn: func(context.Context, *protobufs.AgentToServer) (apmconfig.RemoteConfig, error) {
					return apmconfig.RemoteConfig{
						Hash:  []byte("abcd"),
						Attrs: map[string]string{"test": "aaa"},
					}, nil
				},
			}, zap.NewNop()),
		},
	}

	for name, tt := range testcases {
		t.Run(name, func(t *testing.T) {
			connectionCallbacks := tt.callbacks.OnConnecting(nil).ConnectionCallbacks
			for i := range tt.opampMessages {
				assert.Equal(t, tt.opampMessages[i].expectedServerToAgent, connectionCallbacks.OnMessage(context.TODO(), nil, tt.opampMessages[i].agentToServer))
			}
		})
	}
}
