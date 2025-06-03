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

//go:build integration

package apmconfigextension

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/internal/testutil"
	"github.com/elastic/opentelemetry-lib/config/configelasticsearch"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"google.golang.org/protobuf/proto"
)

const (
	elasticPort = "9200"
	indexName   = ".apm-agent-configuration"
)

func TestIntegration(t *testing.T) {
	t.Run("8.18.x", apmConfigintegrationTest("8_18_x"))
}

func apmConfigintegrationTest(name string) func(t *testing.T) {
	return func(t *testing.T) {
		dockerFile := fmt.Sprintf("Dockerfile.elasticsearch.%s", name)
		req := testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    filepath.Join("testdata", "integration"),
				Dockerfile: dockerFile,
			},
			ExposedPorts: []string{elasticPort},
			WaitingFor:   wait.ForListeningPort(elasticPort).WithStartupTimeout(2 * time.Minute),
		}

		container, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		defer func() {
			err := testcontainers.TerminateContainer(container)
			require.NoError(t, err)
		}()
		defer require.NoError(t, err)

		type inOutOpamp struct {
			agentToServer         *protobufs.AgentToServer
			expectedServerToAgent *protobufs.ServerToAgent
		}
		tests := []struct {
			name                  string
			opampMessages         []inOutOpamp
			agentCfgIndexModifier func(*testing.T, *elasticsearch.Client)
		}{
			{
				name: "empty AgentToServer message, no instance_uid",
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
				agentCfgIndexModifier: func(t *testing.T, client *elasticsearch.Client) {},
			},
			{
				name: "remote config provider unidentified error",
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
								ErrorMessage: "error retrieving remote configuration: agent could not be identified: service.name attribute must be provided",
								Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_Unknown,
							},
						},
					},
				},
				agentCfgIndexModifier: func(t *testing.T, client *elasticsearch.Client) {},
			},
			{
				name: "agent without config applies remote",
				opampMessages: []inOutOpamp{
					{
						agentToServer: &protobufs.AgentToServer{
							InstanceUid: []byte("test"),
							AgentDescription: &protobufs.AgentDescription{
								IdentifyingAttributes: []*protobufs.KeyValue{
									{
										Key:   "service.name",
										Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "test-agent"}},
									},
								},
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
											Body:        []byte(`{"transaction_max_spans":"124"}`),
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
				agentCfgIndexModifier: func(t *testing.T, client *elasticsearch.Client) {
					err := writeAgentIndex(client, "abcd", map[string]string{"name": "test-agent"}, map[string]string{"transaction_max_spans": "124"})
					require.NoError(t, err)
				},
			},
			{
				name: "agent provides new agent description without service.name and corrects identifying attributes",
				opampMessages: []inOutOpamp{
					{
						agentToServer: &protobufs.AgentToServer{
							InstanceUid: []byte("test-2"),
							AgentDescription: &protobufs.AgentDescription{
								IdentifyingAttributes: []*protobufs.KeyValue{
									{
										Key:   "service.name",
										Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "test-agent2"}},
									},
								},
							},
						},
						expectedServerToAgent: &protobufs.ServerToAgent{
							InstanceUid:  []byte("test-2"),
							Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
							RemoteConfig: &protobufs.AgentRemoteConfig{
								ConfigHash: []byte("abcd"),
								Config: &protobufs.AgentConfigMap{
									ConfigMap: map[string]*protobufs.AgentConfigFile{
										"": {
											Body:        []byte(`{"transaction_max_spans":"124"}`),
											ContentType: "text/json",
										},
									},
								},
							},
						},
					},
					{
						agentToServer: &protobufs.AgentToServer{
							InstanceUid: []byte("test-2"),
							AgentDescription: &protobufs.AgentDescription{
								IdentifyingAttributes: []*protobufs.KeyValue{
									{
										Key:   "deployment.environment.name",
										Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "integration-test"}},
									},
								},
							},
						},
						expectedServerToAgent: &protobufs.ServerToAgent{
							InstanceUid:  []byte("test-2"),
							Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
							Flags:        uint64(protobufs.ServerToAgentFlags_ServerToAgentFlags_ReportFullState),
							ErrorResponse: &protobufs.ServerErrorResponse{
								ErrorMessage: "error retrieving remote configuration: agent could not be identified: service.name attribute must be provided",
								Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_Unknown,
							},
						},
					},
					{
						agentToServer: &protobufs.AgentToServer{
							InstanceUid: []byte("test-2"),
							AgentDescription: &protobufs.AgentDescription{
								IdentifyingAttributes: []*protobufs.KeyValue{
									{
										Key:   "service.name",
										Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "test-agent2"}},
									},
									{
										Key:   "deployment.environment.name",
										Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "integration-test"}},
									},
								},
							},
						},
						expectedServerToAgent: &protobufs.ServerToAgent{
							InstanceUid:  []byte("test-2"),
							Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
							RemoteConfig: &protobufs.AgentRemoteConfig{
								ConfigHash: []byte("abcd"),
								Config: &protobufs.AgentConfigMap{
									ConfigMap: map[string]*protobufs.AgentConfigFile{
										"": {
											Body:        []byte(`{"transaction_max_spans":"2"}`),
											ContentType: "text/json",
										},
									},
								},
							},
						},
					},
				},
				agentCfgIndexModifier: func(t *testing.T, client *elasticsearch.Client) {
					err := writeAgentIndex(client, "abcd", map[string]string{"name": "test-agent2"}, map[string]string{"transaction_max_spans": "124"})
					require.NoError(t, err)
					err = writeAgentIndex(client, "abcd", map[string]string{"name": "test-agent2", "environment": "integration-test"}, map[string]string{"transaction_max_spans": "2"})
					require.NoError(t, err)
				},
			},
			{
				name: "neither service.environment nor deployment.environment is an OpenTelemetry identifying attribute",
				opampMessages: []inOutOpamp{
					{
						agentToServer: &protobufs.AgentToServer{
							InstanceUid: []byte("test-3"),
							AgentDescription: &protobufs.AgentDescription{
								IdentifyingAttributes: []*protobufs.KeyValue{
									{
										Key:   "service.name",
										Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "test-agent3"}},
									},
									{
										Key:   "service.environment",
										Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "integration-test"}},
									},
								},
							},
						},
						expectedServerToAgent: &protobufs.ServerToAgent{
							InstanceUid:  []byte("test-3"),
							Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
						},
					},
					{
						agentToServer: &protobufs.AgentToServer{
							InstanceUid: []byte("test-3"),
							AgentDescription: &protobufs.AgentDescription{
								IdentifyingAttributes: []*protobufs.KeyValue{
									{
										Key:   "service.name",
										Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "test-agent3"}},
									},
									{
										Key:   "deployment.environment",
										Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "integration-test"}},
									},
								},
							},
						},
						expectedServerToAgent: &protobufs.ServerToAgent{
							InstanceUid:  []byte("test-3"),
							Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
						},
					},
					{
						agentToServer: &protobufs.AgentToServer{
							InstanceUid: []byte("test-3"),
							AgentDescription: &protobufs.AgentDescription{
								IdentifyingAttributes: []*protobufs.KeyValue{
									{
										Key:   "service.name",
										Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "test-agent3"}},
									},
									{
										Key:   "deployment.environment.name",
										Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "integration-test"}},
									},
								},
							},
						},
						expectedServerToAgent: &protobufs.ServerToAgent{
							InstanceUid:  []byte("test-3"),
							Capabilities: uint64(protobufs.ServerCapabilities_ServerCapabilities_OffersRemoteConfig),
							RemoteConfig: &protobufs.AgentRemoteConfig{
								ConfigHash: []byte("abcd"),
								Config: &protobufs.AgentConfigMap{
									ConfigMap: map[string]*protobufs.AgentConfigFile{
										"": {
											Body:        []byte(`{"transaction_max_spans":"2"}`),
											ContentType: "text/json",
										},
									},
								},
							},
						},
					},
				},
				agentCfgIndexModifier: func(t *testing.T, client *elasticsearch.Client) {
					err := writeAgentIndex(client, "abcd", map[string]string{"name": "test-agent3", "environment": "integration-test"}, map[string]string{"transaction_max_spans": "2"})
					require.NoError(t, err)
				},
			},
		}

		ttCtx := context.Background()
		opAMPTestEndpoint := testutil.GetAvailableLocalAddress(t)
		containerHost, err := container.Host(ttCtx)
		require.NoError(t, err)
		containerPort, err := container.MappedPort(ttCtx, elasticPort)
		require.NoError(t, err)
		esEndpoint := fmt.Sprintf("http://%s:%s", containerHost, containerPort.Port())

		esClient, err := elasticsearch.NewClient(elasticsearch.Config{
			Addresses: []string{
				esEndpoint,
			},
		})
		require.NoError(t, err)

		// ES writes might are not immediately available
		assert.Eventually(t, func() bool {
			return assert.NoError(t, createApmConfigIndex(esClient, esEndpoint))
		}, 2*time.Minute, 20*time.Second)

		extFactory := NewFactory()
		cfg := &Config{
			AgentConfig: AgentConfig{
				Elasticsearch: configelasticsearch.ClientConfig{
					Endpoints: []string{esEndpoint},
				},
				CacheDuration: 100 * time.Millisecond,
			},
			OpAMP: OpAMPConfig{
				Protocols: Protocols{
					ServerConfig: func() *confighttp.ServerConfig {
						httpCfg := confighttp.NewDefaultServerConfig()
						httpCfg.Endpoint = opAMPTestEndpoint
						httpCfg.TLSSetting = nil
						return &httpCfg
					}(),
				},
			},
		}
		ext, err := extFactory.Create(ttCtx, extensiontest.NewNopSettings(metadata.Type), cfg)
		require.NoError(t, err)

		err = ext.Start(ttCtx, componenttest.NewNopHost())
		require.NoError(t, err)

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tt.agentCfgIndexModifier(t, esClient)

				for i := range tt.opampMessages {
					data, err := proto.Marshal(tt.opampMessages[i].agentToServer)
					require.NoError(t, err)

					// Internal cache takes some time to be modified
					assert.Eventually(t, func() bool {
						r, err := http.NewRequest("POST", "http://"+opAMPTestEndpoint+"/v1/opamp", bytes.NewBuffer(data))
						require.NoError(t, err)

						r.Header.Add("Content-Type", "application/x-protobuf")
						res, err := http.DefaultClient.Do(r)
						require.NoError(t, err)

						bodyBytes, err := io.ReadAll(res.Body)
						require.NoError(t, err)

						require.NoError(t, res.Body.Close())

						var actualMessage protobufs.ServerToAgent
						err = proto.Unmarshal(bodyBytes, &actualMessage)
						require.NoError(t, err)

						expectedMessage := tt.opampMessages[i].expectedServerToAgent
						return assert.Equal(t, expectedMessage.GetInstanceUid(), actualMessage.GetInstanceUid()) && assert.Equal(t, expectedMessage.GetFlags(), actualMessage.GetFlags()) && assert.Equal(t, expectedMessage.GetErrorResponse(), actualMessage.GetErrorResponse()) && assert.Equal(t, expectedMessage.GetCapabilities(), actualMessage.GetCapabilities()) && assert.Equal(t, expectedMessage.GetRemoteConfig(), actualMessage.GetRemoteConfig())
					}, 30*time.Second, 1*time.Second)
				}
			})
		}

		err = ext.Shutdown(ttCtx)
		require.NoError(t, err)
	}
}

// creates and the ".apm-agent-configuration" index
func createApmConfigIndex(client *elasticsearch.Client, endpoint string) error {
	resCreate, err := client.Indices.Create(
		indexName,
	)
	if err != nil {
		return err
	}
	return resCreate.Body.Close()
}

// writes an agent configuration to the ".apm-agent-configuration" index
func writeAgentIndex(client *elasticsearch.Client, etag string, service, settings map[string]string) (retErr error) {
	doc := map[string]interface{}{
		"service":          service,
		"settings":         settings,
		"@timestamp":       time.Now().UnixMilli(),
		"applied_by_agent": false,
		"etag":             etag,
	}

	var buf bytes.Buffer
	retErr = json.NewEncoder(&buf).Encode(doc)
	if retErr != nil {
		return retErr
	}

	res, retErr := client.Index(
		indexName,
		&buf,
		client.Index.WithRefresh("true"), // Ensure the document is available immediately
	)
	if retErr != nil {
		return retErr
	}
	defer func() {
		err := res.Body.Close()
		if err != nil && retErr == nil {
			retErr = err
		}
	}()

	if res.IsError() {
		retErr = fmt.Errorf("error while writing .apm-agent-configuration index")
	}
	return retErr
}
