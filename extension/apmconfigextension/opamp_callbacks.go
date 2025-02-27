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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/server/types"
	"go.uber.org/zap"
)

type remoteConfigCallbacks struct {
	configClient apmconfig.RemoteConfigClient

	lastConfigHash sync.Map
	logger         *zap.Logger
}

func newRemoteConfigCallbacks(configClient apmconfig.RemoteConfigClient, logger *zap.Logger) *types.Callbacks {
	opampCallbacks := &remoteConfigCallbacks{
		configClient:   configClient,
		lastConfigHash: sync.Map{},
		logger:         logger,
	}

	connectionCallbacks := types.ConnectionCallbacks{}
	connectionCallbacks.SetDefaults()
	connectionCallbacks.OnMessage = opampCallbacks.OnMessage

	remoteConfigCallbacks := types.Callbacks{}
	remoteConfigCallbacks.OnConnecting = func(request *http.Request) types.ConnectionResponse {
		return types.ConnectionResponse{
			Accept:         true,
			HTTPStatusCode: 200,

			ConnectionCallbacks: connectionCallbacks,
		}
	}

	return &remoteConfigCallbacks
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
func (rc *remoteConfigCallbacks) OnMessage(ctx context.Context, conn types.Connection, message *protobufs.AgentToServer) *protobufs.ServerToAgent {
	serverToAgent := protobufs.ServerToAgent{}
	serverToAgent.Capabilities = uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig)
	serverToAgent.InstanceUid = message.GetInstanceUid()

	if message.GetInstanceUid() == nil {
		serverToAgent.Flags = uint64(protobufs.ServerToAgentFlags_ServerToAgentFlags_ReportFullState)
		return rc.serverError("instance_uid must be provided", &serverToAgent)
	}

	agentUid := hex.EncodeToString(message.GetInstanceUid())
	agentUidField := zap.String("instance_uid", agentUid)

	if message.GetAgentDisconnect() != nil {
		rc.logger.Info("Disconnecting the agent from the remote configuration service", agentUidField)
		rc.lastConfigHash.Delete(agentUid)
		return &serverToAgent
	}

	remoteConfig, err := rc.configClient.RemoteConfig(ctx, message)
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

	if message.GetRemoteConfigStatus() != nil && message.GetRemoteConfigStatus().Status == protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED && bytes.Equal(remoteConfig.ConfigHash, message.RemoteConfigStatus.GetLastRemoteConfigHash()) {
		rc.logger.Info("Remote config applied", zap.String("hash", hex.EncodeToString(remoteConfig.ConfigHash)), agentUidField)
		rc.lastConfigHash.Store(agentUid, message.RemoteConfigStatus.GetLastRemoteConfigHash())
	} else if lastHash, found := rc.lastConfigHash.Load(agentUid); !found || !bytes.Equal(lastHash.([]byte), remoteConfig.ConfigHash) {
		rc.logger.Info("Sending new remote configuration", agentUidField, zap.String("hash", hex.EncodeToString(remoteConfig.ConfigHash)))
		serverToAgent.RemoteConfig = remoteConfig
	}

	return &serverToAgent
}
