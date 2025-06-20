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
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type remoteConfigMock struct {
	remoteConfigFn func(context.Context, apmconfig.IdentifyingAttributes, apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error)
}

func (f *remoteConfigMock) RemoteConfig(ctx context.Context, attrs apmconfig.IdentifyingAttributes, lastHash apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error) {
	return f.remoteConfigFn(ctx, attrs, lastHash)
}

func TestOnMessage(t *testing.T) {
	type inOutOpamp struct {
		agentToServer         *protobufs.AgentToServer
		expectedServerToAgent *protobufs.ServerToAgent
	}

	testcases := map[string]struct {
		opampMessages    []inOutOpamp
		callbacksFactory func() (*remoteConfigCallbacks, error)
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
			callbacksFactory: func() (*remoteConfigCallbacks, error) {
				return newRemoteConfigCallbacks(&remoteConfigMock{
					remoteConfigFn: func(context.Context, apmconfig.IdentifyingAttributes, apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error) {
						return nil, nil
					},
				}, defaultCacheConfig, zap.NewNop())
			},
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
			callbacksFactory: func() (*remoteConfigCallbacks, error) {
				return newRemoteConfigCallbacks(&remoteConfigMock{
					remoteConfigFn: func(context.Context, apmconfig.IdentifyingAttributes, apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error) {
						return nil, errors.New("testing error")
					},
				}, defaultCacheConfig, zap.NewNop())
			},
		},
		"remote config provider unidentified error": {
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
			callbacksFactory: func() (*remoteConfigCallbacks, error) {
				return newRemoteConfigCallbacks(&remoteConfigMock{
					remoteConfigFn: func(context.Context, apmconfig.IdentifyingAttributes, apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error) {
						return nil, apmconfig.UnidentifiedAgent
					},
				}, defaultCacheConfig, zap.NewNop())
			},
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
									"elastic": {
										Body:        []byte(`{"test":"aaa"}`),
										ContentType: "application/json",
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
			callbacksFactory: func() (*remoteConfigCallbacks, error) {
				return newRemoteConfigCallbacks(&remoteConfigMock{
					remoteConfigFn: func() func(context.Context, apmconfig.IdentifyingAttributes, apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error) {
						var cached bool
						return func(context.Context, apmconfig.IdentifyingAttributes, apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error) {
							if cached {
								return nil, nil
							} else {
								cached = true
								return &protobufs.AgentRemoteConfig{
									ConfigHash: []byte("abcd"),
									Config: &protobufs.AgentConfigMap{
										ConfigMap: map[string]*protobufs.AgentConfigFile{
											"elastic": {
												ContentType: "application/json",
												Body:        []byte(`{"test":"aaa"}`),
											},
										},
									},
								}, nil
							}
						}
					}(),
				}, defaultCacheConfig, zap.NewNop())
			},
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
					},
				},
			},
			callbacksFactory: func() (*remoteConfigCallbacks, error) {
				return newRemoteConfigCallbacks(&remoteConfigMock{
					remoteConfigFn: func(context.Context, apmconfig.IdentifyingAttributes, apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error) {
						return nil, nil
					},
				}, defaultCacheConfig, zap.NewNop())
			},
		},
		"agent failed to apply remote config": {
			opampMessages: []inOutOpamp{
				{
					agentToServer: &protobufs.AgentToServer{
						InstanceUid: []byte("test"),
						RemoteConfigStatus: &protobufs.RemoteConfigStatus{
							LastRemoteConfigHash: []byte("abcd"),
							Status:               protobufs.RemoteConfigStatuses_RemoteConfigStatuses_FAILED,
							ErrorMessage:         "oh noes!",
						},
					},
					expectedServerToAgent: &protobufs.ServerToAgent{
						InstanceUid:  []byte("test"),
						Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
					},
				},
			},
			callbacksFactory: func() (*remoteConfigCallbacks, error) {
				return newRemoteConfigCallbacks(&remoteConfigMock{
					remoteConfigFn: func(context.Context, apmconfig.IdentifyingAttributes, apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error) {
						return nil, nil
					},
				}, defaultCacheConfig, zap.NewNop())
			},
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
									"elastic": {
										Body:        []byte(`{"test":"aaa"}`),
										ContentType: "application/json",
									},
								},
							},
						},
					},
				},
			},
			callbacksFactory: func() (*remoteConfigCallbacks, error) {
				return newRemoteConfigCallbacks(&remoteConfigMock{
					remoteConfigFn: func(context.Context, apmconfig.IdentifyingAttributes, apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error) {
						return &protobufs.AgentRemoteConfig{
							ConfigHash: []byte("abcd"),
							Config: &protobufs.AgentConfigMap{
								ConfigMap: map[string]*protobufs.AgentConfigFile{
									"elastic": {
										ContentType: "application/json",
										Body:        []byte(`{"test":"aaa"}`),
									},
								},
							},
						}, nil
					},
				}, defaultCacheConfig, zap.NewNop())
			},
		},
		"agent changes AgentDescription": {
			opampMessages: []inOutOpamp{
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
			callbacksFactory: func() (*remoteConfigCallbacks, error) {
				return newRemoteConfigCallbacks(&remoteConfigMock{
					remoteConfigFn: func(context.Context, apmconfig.IdentifyingAttributes, apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error) {
						return nil, nil
					},
				}, defaultCacheConfig, zap.NewNop())
			},
		},
	}

	for name, tt := range testcases {
		t.Run(name, func(t *testing.T) {
			callbacks, err := tt.callbacksFactory()
			assert.NoError(t, err)
			connectionCallbacks := callbacks.OnConnecting(nil).ConnectionCallbacks
			for i := range tt.opampMessages {
				assert.Equal(t, tt.opampMessages[i].expectedServerToAgent, connectionCallbacks.OnMessage(context.TODO(), nil, tt.opampMessages[i].agentToServer))

				// assert resources cleanup on AgentDisconnect
				if len(tt.opampMessages[i].agentToServer.InstanceUid) > 0 {
					assert.Equal(t, &protobufs.ServerToAgent{
						InstanceUid:  tt.opampMessages[i].agentToServer.InstanceUid,
						Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
					}, connectionCallbacks.OnMessage(context.TODO(), nil, &protobufs.AgentToServer{
						InstanceUid:     tt.opampMessages[i].agentToServer.InstanceUid,
						AgentDisconnect: &protobufs.AgentDisconnect{},
					}))
					assert.False(t, func() bool {
						found := callbacks.agentState.Contains(hex.EncodeToString(tt.opampMessages[i].agentToServer.InstanceUid))
						return found
					}())
				}
			}
		})
	}
}

func TestOnMessageLRU(t *testing.T) {
	callbacksMock, err := newRemoteConfigCallbacks(&remoteConfigMock{
		remoteConfigFn: func(context.Context, apmconfig.IdentifyingAttributes, apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error) {
			return nil, nil
		},
	}, defaultCacheConfig, zap.NewNop())
	assert.NoError(t, err)

	connectionCallbacks := callbacksMock.OnConnecting(nil).ConnectionCallbacks
	instanceUid := []byte("test")
	encodedInstanceUid := hex.EncodeToString(instanceUid)

	// no new agent
	_ = connectionCallbacks.OnMessage(context.TODO(), nil, &protobufs.AgentToServer{})
	assert.Equal(t, 0, callbacksMock.agentState.Len())

	// new agent
	_ = connectionCallbacks.OnMessage(context.TODO(), nil, &protobufs.AgentToServer{
		InstanceUid: instanceUid,
	})
	assert.Equal(t, 1, callbacksMock.agentState.Len())
	cachedAgent, found := callbacksMock.agentState.Get(encodedInstanceUid)
	assert.True(t, found)
	assert.Equal(t, agentInfo{
		agentUid: []byte(instanceUid),
	}, *cachedAgent)

	// updates cached agent
	_ = connectionCallbacks.OnMessage(context.TODO(), nil, &protobufs.AgentToServer{
		InstanceUid: []byte(instanceUid),
		RemoteConfigStatus: &protobufs.RemoteConfigStatus{
			LastRemoteConfigHash: []byte("abcd"),
			Status:               protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLYING,
		},
	})
	assert.Equal(t, 1, callbacksMock.agentState.Len())
	cachedAgent, found = callbacksMock.agentState.Get(encodedInstanceUid)
	assert.True(t, found)
	assert.Equal(t, agentInfo{
		agentUid:       []byte(instanceUid),
		lastConfigHash: []byte("abcd"),
	}, *cachedAgent)

	// new agent description removes cached agent's last hash
	_ = connectionCallbacks.OnMessage(context.TODO(), nil, &protobufs.AgentToServer{
		InstanceUid:      []byte(instanceUid),
		AgentDescription: &protobufs.AgentDescription{},
	})
	assert.Equal(t, 1, callbacksMock.agentState.Len())
	cachedAgent, found = callbacksMock.agentState.Get(encodedInstanceUid)
	assert.True(t, found)
	assert.Equal(t, agentInfo{
		agentUid: []byte(instanceUid),
	}, *cachedAgent)
}

func TestOnMessage_EvictedCapacityLRU(t *testing.T) {
	// testing evicted entries because capacity
	callbacksMock, err := newRemoteConfigCallbacks(&remoteConfigMock{
		remoteConfigFn: func(context.Context, apmconfig.IdentifyingAttributes, apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error) {
			return nil, nil
		},
	}, CacheConfig{
		Capacity: 1,
		TTL:      10 * time.Second,
	}, zap.NewNop())
	assert.NoError(t, err)

	connectionCallbacks := callbacksMock.OnConnecting(nil).ConnectionCallbacks
	firstUid, secondUid2 := []byte("test"), []byte("test2")
	encodedInstanceUid, encodedInstanceUid2 := hex.EncodeToString(firstUid), hex.EncodeToString(secondUid2)

	// new agent
	_ = connectionCallbacks.OnMessage(context.TODO(), nil, &protobufs.AgentToServer{
		InstanceUid: firstUid,
	})
	assert.Equal(t, 1, callbacksMock.agentState.Len())
	cachedAgent, found := callbacksMock.agentState.Get(encodedInstanceUid)
	assert.True(t, found)
	assert.Equal(t, agentInfo{
		agentUid: firstUid,
	}, *cachedAgent)

	// second agent should evict first one
	_ = connectionCallbacks.OnMessage(context.TODO(), nil, &protobufs.AgentToServer{
		InstanceUid: secondUid2,
	})
	assert.Equal(t, 1, callbacksMock.agentState.Len())
	cachedAgent, found = callbacksMock.agentState.Get(encodedInstanceUid2)
	assert.True(t, found)
	assert.Equal(t, agentInfo{
		agentUid: secondUid2,
	}, *cachedAgent)
}

func TestOnMessage_EvictedTTLLRU(t *testing.T) {
	// testing evicted entries because capacity
	callbacksMock, err := newRemoteConfigCallbacks(&remoteConfigMock{
		remoteConfigFn: func(context.Context, apmconfig.IdentifyingAttributes, apmconfig.LastConfigHash) (*protobufs.AgentRemoteConfig, error) {
			return nil, nil
		},
	}, CacheConfig{
		Capacity: 2,
		TTL:      time.Nanosecond,
	}, zap.NewNop())
	assert.NoError(t, err)

	connectionCallbacks := callbacksMock.OnConnecting(nil).ConnectionCallbacks
	firstUid, secondUid2 := []byte("test"), []byte("test2")
	encodedInstanceUid, encodedInstanceUid2 := hex.EncodeToString(firstUid), hex.EncodeToString(secondUid2)

	// new agent
	_ = connectionCallbacks.OnMessage(context.TODO(), nil, &protobufs.AgentToServer{
		InstanceUid: firstUid,
	})
	// second agent
	_ = connectionCallbacks.OnMessage(context.TODO(), nil, &protobufs.AgentToServer{
		InstanceUid: secondUid2,
	})

	assert.Equal(t, 2, callbacksMock.agentState.Len())
	_, found := callbacksMock.agentState.Get(encodedInstanceUid)
	assert.False(t, found)
	assert.Equal(t, 1, callbacksMock.agentState.Len())
	_, found = callbacksMock.agentState.Get(encodedInstanceUid2)
	assert.False(t, found)
	assert.Equal(t, 0, callbacksMock.agentState.Len())
}
