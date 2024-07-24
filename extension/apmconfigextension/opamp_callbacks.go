package apmconfigextension

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/server/types"
	semconv "go.opentelemetry.io/collector/semconv/v1.25.0"
	"go.uber.org/zap"
)

type configOpAMPCallbacks struct {
	configClient apmconfig.RemoteClient
	knownAgents  map[string]apmconfig.Params
	logger       *zap.Logger
}

var _ types.Callbacks = (*configOpAMPCallbacks)(nil)

func newConfigOpAMPCallbacks(configClient apmconfig.RemoteClient, logger *zap.Logger) *configOpAMPCallbacks {
	knownAgents := make(map[string]apmconfig.Params)
	return &configOpAMPCallbacks{
		configClient,
		knownAgents,
		logger,
	}
}

func (op *configOpAMPCallbacks) OnConnecting(request *http.Request) types.ConnectionResponse {
	return types.ConnectionResponse{
		Accept:              true,
		HTTPStatusCode:      200,
		ConnectionCallbacks: &configConnectionCallbacks{configClient: op.configClient, knownAgents: op.knownAgents, logger: op.logger},
	}
}

type configConnectionCallbacks struct {
	configClient apmconfig.RemoteClient
	knownAgents  map[string]apmconfig.Params
	logger       *zap.Logger
}

var _ types.ConnectionCallbacks = (*configConnectionCallbacks)(nil)

func (rc *configConnectionCallbacks) OnConnected(ctx context.Context, conn types.Connection) {}

func updateAgentParams(params *apmconfig.Params, description *protobufs.AgentDescription) {
	if description != nil {
		for i := range description.IdentifyingAttributes {
			switch description.IdentifyingAttributes[i].Key {
			case semconv.AttributeServiceName:
				params.Service.Name = description.IdentifyingAttributes[i].Value.GetStringValue()
			case semconv.AttributeDeploymentEnvironment:
				params.Service.Environment = description.IdentifyingAttributes[i].Value.GetStringValue()
			}
		}
	}
}

func (rc *configConnectionCallbacks) serverError(msg string, message *protobufs.ServerToAgent, logFields ...zap.Field) *protobufs.ServerToAgent {
	message.ErrorResponse.ErrorMessage = msg
	message.ErrorResponse.Type = protobufs.ServerErrorResponseType_ServerErrorResponseType_Unknown
	rc.logger.Error(message.ErrorResponse.ErrorMessage, logFields...)
	return message
}

// OnMessage is called when a message is received from the connection. Can happen
// only after OnConnected(). Must return a ServerToAgent message that will be sent
// as a response to the Agent.
// For plain HTTP requests once OnMessage returns and the response is sent
// to the Agent the OnConnectionClose message will be called immediately.
func (rc *configConnectionCallbacks) OnMessage(ctx context.Context, conn types.Connection, message *protobufs.AgentToServer) *protobufs.ServerToAgent {
	serverToAgent := protobufs.ServerToAgent{}
	serverToAgent.InstanceUid = message.GetInstanceUid()

	agentUid := string(message.GetInstanceUid())

	agentParams := rc.knownAgents[agentUid]
	agentParams.AgentUiD = agentUid
	updateAgentParams(&agentParams, message.AgentDescription)
	agentUidLogField := []zap.Field{
		zap.String("instance_uid", agentUid),
		zap.String("service.name", agentParams.Service.Name),
		zap.String("service.environment", agentParams.Service.Environment),
	}

	// set msg flag to resend all data?
	if agentParams.Service.Name == "" {
		return rc.serverError("unidentified agent: service.name attribute must be provided", &serverToAgent, agentUidLogField...)
	}

	// Agent is reporting remote config status
	var agentRemoteConfigHash []byte
	if message.RemoteConfigStatus != nil {
		agentRemoteConfigHash = message.RemoteConfigStatus.GetLastRemoteConfigHash()
	}

	// update; internal state
	rc.knownAgents[agentUid] = agentParams

	remoteConfig, err := rc.configClient.RemoteConfig(ctx, agentParams)
	if err != nil {
		return rc.serverError(fmt.Sprintf("error retrieving remote configuration: %s", err), &serverToAgent)
	} else if len(agentRemoteConfigHash) > 0 && bytes.Equal(remoteConfig.Hash, agentRemoteConfigHash) {
		rc.logger.Info(fmt.Sprintf("Remote config matches agent config: %v\n", remoteConfig.Hash), agentUidLogField...)
		// Agent applied the configuration: update upstream apm-server
		err = rc.configClient.LastConfig(ctx, agentParams, agentRemoteConfigHash)
		if err != nil {
			return rc.serverError(fmt.Sprintf("error notifying the central config about the applied remote configuration: %s", err), &serverToAgent)
		}
	} else if len(remoteConfig.Attrs) > 0 {
		rc.logger.Info(fmt.Sprintf("APM central remote configuration received: %v\n", remoteConfig), agentUidLogField...)

		marshallConfig, err := json.Marshal(remoteConfig.Attrs)
		if err != nil {
			return rc.serverError(fmt.Sprintf("error marshaling remote configuration: %s", err), &serverToAgent)
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
	}

	return &serverToAgent
}

// OnConnectionClose is called when the OpAMP connection is closed.
func (rc *configConnectionCallbacks) OnConnectionClose(conn types.Connection) {}
