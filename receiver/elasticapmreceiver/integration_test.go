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

package elasticapmreceiver

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
	"github.com/elastic/opentelemetry-collector-components/internal/testutil"
	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver/internal/metadata"
	"github.com/elastic/opentelemetry-lib/agentcfg"
	"github.com/elastic/opentelemetry-lib/config/configelasticsearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
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

		tests := []struct {
			name                  string
			query                 agentcfg.Query
			agentCfgIndexModifier func(*testing.T, *elasticsearch.Client)

			expectedStatusCode int
			expectedEtagHeader []string
			expectedBody       func(*testing.T, []byte) bool
		}{
			{
				name:                  "empty request, service.name required",
				query:                 agentcfg.Query{},
				agentCfgIndexModifier: func(*testing.T, *elasticsearch.Client) {},

				expectedStatusCode: http.StatusBadRequest,
				expectedBody: func(t *testing.T, b []byte) bool {
					return assert.JSONEq(t, string([]byte(`{"error":"service.name is required"}`)), string(b))
				},
			},
			{
				name: "empty request, no default",
				query: agentcfg.Query{
					Service: agentcfg.Service{
						Name: "all",
					},
				},
				agentCfgIndexModifier: func(*testing.T, *elasticsearch.Client) {},

				expectedStatusCode: http.StatusOK,
				expectedEtagHeader: []string{"\"-\""},
				expectedBody: func(t *testing.T, b []byte) bool {
					return assert.JSONEq(t, string([]byte(`{}`)), string(b))
				},
			},
			{
				name: "empty request, with default",
				query: agentcfg.Query{
					Service: agentcfg.Service{
						Name: "all",
					},
				},
				agentCfgIndexModifier: func(t *testing.T, client *elasticsearch.Client) {
					err := writeAgentIndex(client, "abcd", map[string]string{"name": ""}, map[string]string{"transaction_max_spans": "124"})
					require.NoError(t, err)
				},

				expectedStatusCode: http.StatusOK,
				expectedEtagHeader: []string{"\"abcd\""},
				expectedBody: func(t *testing.T, b []byte) bool {
					return assert.JSONEq(t, string([]byte(`{"transaction_max_spans":"124"}`)), string(b))
				},
			},
			{
				name: "new service configuration",
				query: agentcfg.Query{
					Service: agentcfg.Service{
						Name: "test-agent",
					},
				},
				agentCfgIndexModifier: func(t *testing.T, client *elasticsearch.Client) {
					err := writeAgentIndex(client, "abc", map[string]string{"name": "test-agent"}, map[string]string{"transaction_max_spans": "123"})
					require.NoError(t, err)
				},

				expectedStatusCode: http.StatusOK,
				expectedEtagHeader: []string{"\"abc\""},
				expectedBody: func(t *testing.T, b []byte) bool {
					return assert.JSONEq(t, string([]byte(`{"transaction_max_spans":"123"}`)), string(b))
				},
			},
			{
				name: "new service configuration, with environment",
				query: agentcfg.Query{
					Service: agentcfg.Service{
						Name:        "test-agent-1",
						Environment: "demo",
					},
				},
				agentCfgIndexModifier: func(t *testing.T, client *elasticsearch.Client) {
					err := writeAgentIndex(client, "abc", map[string]string{"name": "test-agent-1", "environment": "not-demo"}, map[string]string{"transaction_max_spans": "124"})
					require.NoError(t, err)
					err = writeAgentIndex(client, "abc", map[string]string{"name": "test-agent-1", "environment": "demo"}, map[string]string{"transaction_max_spans": "125"})
					require.NoError(t, err)
				},

				expectedStatusCode: http.StatusOK,
				expectedEtagHeader: []string{"\"abc\""},
				expectedBody: func(t *testing.T, b []byte) bool {
					return assert.JSONEq(t, string([]byte(`{"transaction_max_spans":"125"}`)), string(b))
				},
			},
			{
				name: "unmodified service configuration",
				query: agentcfg.Query{
					Etag: "test",
					Service: agentcfg.Service{
						Name: "test-agent-2",
					},
				},
				agentCfgIndexModifier: func(t *testing.T, client *elasticsearch.Client) {
					err := writeAgentIndex(client, "test", map[string]string{"name": "test-agent-2"}, map[string]string{"transaction_max_spans": "123"})
					require.NoError(t, err)
				},

				expectedStatusCode: http.StatusNotModified,
				expectedEtagHeader: []string{"\"test\""},
				expectedBody: func(t *testing.T, b []byte) bool {
					return assert.Empty(t, b)
				},
			},
			{
				name: "changed service configuration",
				query: agentcfg.Query{
					Etag: "old",
					Service: agentcfg.Service{
						Name: "test-agent-3",
					},
				},
				agentCfgIndexModifier: func(t *testing.T, client *elasticsearch.Client) {
					err := writeAgentIndex(client, "new", map[string]string{"name": "test-agent-3"}, map[string]string{"transaction_max_spans": "1"})
					require.NoError(t, err)
				},

				expectedStatusCode: http.StatusOK,
				expectedEtagHeader: []string{"\"new\""},
				expectedBody: func(t *testing.T, b []byte) bool {
					return assert.JSONEq(t, string([]byte(`{"transaction_max_spans":"1"}`)), string(b))
				},
			},
		}

		ttCtx := context.Background()
		testEndpoint := testutil.GetAvailableLocalAddress(t)
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

		rcvrFactory := NewFactory()
		cfg := &Config{
			ServerConfig: confighttp.ServerConfig{
				Endpoint: testEndpoint,
			},
			AgentConfig: AgentConfig{
				Enabled: true,
				Elasticsearch: configelasticsearch.ClientConfig{
					Endpoints: []string{esEndpoint},
				},
				CacheDuration: 100 * time.Millisecond,
			},
		}
		rcvr, err := rcvrFactory.CreateMetrics(ttCtx, receivertest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
		require.NoError(t, err)

		err = rcvr.Start(ttCtx, componenttest.NewNopHost())
		require.NoError(t, err)

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tt.agentCfgIndexModifier(t, esClient)

				jsonQuery, err := json.Marshal(tt.query)
				require.NoError(t, err)
				r, err := http.NewRequest("POST", "http://"+testEndpoint+agentConfigPath, bytes.NewBuffer(jsonQuery))
				require.NoError(t, err)

				r.Header.Add("Content-Type", "application/json")

				// Internal cache takes some time to be modified
				assert.Eventually(t, func() bool {
					res, err := http.DefaultClient.Do(r)
					require.NoError(t, err)

					bodyBytes, err := io.ReadAll(res.Body)
					require.NoError(t, err)

					require.NoError(t, res.Body.Close())

					return assert.Equal(t, tt.expectedStatusCode, res.StatusCode) && assert.Equal(t, tt.expectedEtagHeader, res.Header[Etag]) && tt.expectedBody(t, bodyBytes)
				}, 30*time.Second, 1*time.Second)
			})
		}

		err = rcvr.Shutdown(ttCtx)
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
