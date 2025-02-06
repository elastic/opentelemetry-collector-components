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
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/server/types"
	"go.uber.org/zap"
)

type configOpAMPCallbacks struct {
	configClient apmconfig.RemoteConfigClient
	logger       *zap.Logger
}

var _ types.Callbacks = (*configOpAMPCallbacks)(nil)

func newConfigOpAMPCallbacks(configClient apmconfig.RemoteConfigClient, logger *zap.Logger) *configOpAMPCallbacks {
	return &configOpAMPCallbacks{
		configClient,
		logger,
	}
}

func (op *configOpAMPCallbacks) OnConnecting(request *http.Request) types.ConnectionResponse {
	return types.ConnectionResponse{
		Accept:              true,
		HTTPStatusCode:      200,
		ConnectionCallbacks: &configConnectionCallbacks{configClient: op.configClient, logger: op.logger},
	}
}

type configConnectionCallbacks struct {
	configClient apmconfig.RemoteConfigClient
	logger       *zap.Logger
}

var _ types.ConnectionCallbacks = (*configConnectionCallbacks)(nil)

func (rc *configConnectionCallbacks) OnConnected(ctx context.Context, conn types.Connection) {}

func (rc *configConnectionCallbacks) serverError(msg string, message *protobufs.ServerToAgent, logFields ...zap.Field) *protobufs.ServerToAgent {
	message.ErrorResponse = &protobufs.ServerErrorResponse{
		ErrorMessage: msg,
		Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_Unknown,
	}
	rc.logger.Error(message.ErrorResponse.ErrorMessage, logFields...)
	return message
}

func bundleJsonConfig(remoteConfig apmconfig.RemoteConfig, serverToAgent *protobufs.ServerToAgent) error {
	marshallConfig, err := json.Marshal(remoteConfig.Attrs)
	if err != nil {
		return err
	}
	serverToAgent.RemoteConfig = &protobufs.AgentRemoteConfig{
		ConfigHash: remoteConfig.Hash,
		Config: &protobufs.AgentConfigMap{
			ConfigMap: map[string]*protobufs.AgentConfigFile{
				"": {
					Body:        marshallConfig,
					ContentType: "text/json",
				},
			},
		},
	}
	return nil
}

// OnMessage is called when a message is received from the connection. Can happen
// only after OnConnected(). Must return a ServerToAgent message that will be sent
// as a response to the Agent.
// For plain HTTP requests once OnMessage returns and the response is sent
// to the Agent the OnConnectionClose message will be called immediately.
func (rc *configConnectionCallbacks) OnMessage(ctx context.Context, conn types.Connection, message *protobufs.AgentToServer) *protobufs.ServerToAgent {
	serverToAgent := protobufs.ServerToAgent{}
	serverToAgent.Capabilities = uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig)
	serverToAgent.InstanceUid = message.GetInstanceUid()

	agentUid := hex.EncodeToString(message.GetInstanceUid())
	agentUidField := zap.String("instance_uid", agentUid)

	if message.GetAgentDisconnect() != nil {
		rc.logger.Info("Disconnecting the agent from the remote configuration service", agentUidField)
	} else {

		remoteConfig, err := rc.configClient.RemoteConfig(ctx, message)
		if err != nil {
			return rc.serverError(fmt.Sprintf("error retrieving remote configuration: %s", err), &serverToAgent)
		} else if message.RemoteConfigStatus != nil && len(message.RemoteConfigStatus.GetLastRemoteConfigHash()) > 0 && bytes.Equal(remoteConfig.Hash, message.RemoteConfigStatus.GetLastRemoteConfigHash()) {
			rc.logger.Info(fmt.Sprintf("Remote config matches agent config: %v\n", remoteConfig.Hash), agentUidField)
		} else if len(remoteConfig.Attrs) > 0 {
			rc.logger.Info("Received remote configuration", agentUidField, zap.String("config_hash", hex.EncodeToString(remoteConfig.Hash)))

			err := bundleJsonConfig(remoteConfig, &serverToAgent)
			if err != nil {
				return rc.serverError(fmt.Sprintf("error marshaling remote configuration: %s", err), &serverToAgent)
			}
		}
	}

	return &serverToAgent
}

// OnConnectionClose is called when the OpAMP connection is closed.
func (rc *configConnectionCallbacks) OnConnectionClose(conn types.Connection) {}
