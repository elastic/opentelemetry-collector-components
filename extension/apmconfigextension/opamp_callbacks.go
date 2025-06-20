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
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/cespare/xxhash"
	"github.com/elastic/go-freelru"
	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/server/types"
	"go.uber.org/zap"
)

type remoteConfigCallbacks struct {
	*types.Callbacks
	configClient apmconfig.RemoteConfigClient

	agentState freelru.Cache[string, *agentInfo]
	ttl        time.Duration

	logger *zap.Logger
}

type cacheConfig struct {
	// capacity defines the maximum number of agents
	// results to cache. Once this is reached, the least recently
	// used entries will be evicted.
	capacity uint32
	// TTL defines the duration before the cache key gets evicted
	ttl time.Duration
}

type agentInfo struct {
	agentUid              apmconfig.InstanceUid
	identifyingAttributes apmconfig.IdentifyingAttributes
	lastConfigHash        apmconfig.LastConfigHash
}

func newRemoteConfigCallbacks(configClient apmconfig.RemoteConfigClient, ttlConfig cacheConfig, logger *zap.Logger) (*remoteConfigCallbacks, error) {
	cache, err := freelru.NewSharded[string, *agentInfo](ttlConfig.capacity, func(key string) uint32 {
		return uint32(xxhash.Sum64String(key))
	})
	if err != nil {
		return nil, err
	}
	cache.SetLifetime(ttlConfig.ttl)

	opampCallbacks := &remoteConfigCallbacks{
		configClient: configClient,
		agentState:   cache,
		logger:       logger,
		ttl:          ttlConfig.ttl,
	}

	connectionCallbacks := types.ConnectionCallbacks{}
	connectionCallbacks.SetDefaults()
	connectionCallbacks.OnMessage = opampCallbacks.onMessage

	opampCallbacks.Callbacks = &types.Callbacks{
		OnConnecting: func(request *http.Request) types.ConnectionResponse {
			return types.ConnectionResponse{
				Accept:         true,
				HTTPStatusCode: 200,

				ConnectionCallbacks: connectionCallbacks,
			}
		},
	}

	return opampCallbacks, nil
}

func (rc *remoteConfigCallbacks) serverError(msg string, message *protobufs.ServerToAgent, logFields ...zap.Field) *protobufs.ServerToAgent {
	message.ErrorResponse = &protobufs.ServerErrorResponse{
		ErrorMessage: msg,
		Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_Unknown,
	}
	rc.logger.Error(message.ErrorResponse.ErrorMessage, logFields...)
	return message
}

// OnMessage is called when a message is received from the connection. Can happen
// only after OnConnected(). Must return a ServerToAgent message that will be sent
// as a response to the Agent.
// For plain HTTP requests once OnMessage returns and the response is sent
// to the Agent the OnConnectionClose message will be called immediately.
func (rc *remoteConfigCallbacks) onMessage(ctx context.Context, conn types.Connection, message *protobufs.AgentToServer) *protobufs.ServerToAgent {
	serverToAgent := protobufs.ServerToAgent{}
	serverToAgent.Capabilities = uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig)
	serverToAgent.InstanceUid = message.GetInstanceUid()

	if message.GetInstanceUid() == nil {
		serverToAgent.Flags = uint64(protobufs.ServerToAgentFlags_ServerToAgentFlags_ReportFullState)
		return rc.serverError("instance_uid must be provided", &serverToAgent)
	}

	agentUid := hex.EncodeToString(message.GetInstanceUid())
	if message.GetAgentDescription() != nil {
		// new description might lead to another remote configuration
		_ = rc.agentState.Add(agentUid, &agentInfo{
			agentUid:              message.GetInstanceUid(),
			identifyingAttributes: message.AgentDescription.IdentifyingAttributes,
		})
	}

	agentUidField := zap.String("instance_uid", agentUid)
	if message.GetAgentDisconnect() != nil {
		rc.logger.Info("Disconnecting the agent from the remote configuration service", agentUidField)
		_ = rc.agentState.Remove(agentUid)
		return &serverToAgent
	}

	loadedAgent, found := rc.agentState.GetAndRefresh(agentUid, rc.ttl)
	if !found {
		loadedAgent = &agentInfo{
			agentUid: message.InstanceUid,
		}
		_ = rc.agentState.Add(agentUid, loadedAgent)
	}

	remoteConfigStatus := message.GetRemoteConfigStatus()
	if remoteConfigStatus != nil {
		loadedAgent.lastConfigHash = remoteConfigStatus.GetLastRemoteConfigHash()
		rc.logger.Info("Remote config status", agentUidField, zap.String("lastRemoteConfigHash", hex.EncodeToString(loadedAgent.lastConfigHash)), zap.String("status", remoteConfigStatus.GetStatus().String()), zap.String("errorMessage", remoteConfigStatus.ErrorMessage))
		rc.agentState.Add(agentUid, loadedAgent)
	}

	remoteConfig, err := rc.configClient.RemoteConfig(ctx, loadedAgent.identifyingAttributes, loadedAgent.lastConfigHash)
	if err != nil {
		// remote config client could not identify the agent
		if errors.Is(err, apmconfig.UnidentifiedAgent) {
			serverToAgent.Flags = uint64(protobufs.ServerToAgentFlags_ServerToAgentFlags_ReportFullState)
		}
		return rc.serverError(fmt.Sprintf("error retrieving remote configuration: %s", err), &serverToAgent)
	} else if remoteConfig == nil {
		// nothing to be applied
		return &serverToAgent
	}

	rc.logger.Info("Sending new remote configuration", agentUidField, zap.String("hash", hex.EncodeToString(remoteConfig.ConfigHash)))
	serverToAgent.RemoteConfig = remoteConfig

	return &serverToAgent
}
