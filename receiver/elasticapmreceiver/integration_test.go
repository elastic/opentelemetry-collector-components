// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

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
	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver/internal/testutil"
	"github.com/elastic/opentelemetry-lib/agentcfg"
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
	t.Run("8.17.0", apmConfigintegrationTest("8_17_0"))
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
		defer testcontainers.TerminateContainer(container)
		require.NoError(t, err)

		tests := []struct {
			name                  string
			requestHeaders        http.Header
			query                 agentcfg.Query
			agentCfgIndexModifier func(*testing.T, string)

			expectedStatusCode int
			expectedBody       func(*testing.T, []byte) bool
		}{
			{
				name:                  "empty request, service.name required",
				query:                 agentcfg.Query{},
				agentCfgIndexModifier: func(*testing.T, string) {},

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
				agentCfgIndexModifier: func(*testing.T, string) {},

				expectedStatusCode: http.StatusOK,
				expectedBody: func(t *testing.T, b []byte) bool {
					return assert.JSONEq(t, string([]byte(`{"_source":{"settings":{},"etag":"-","agent_name":""}}`)), string(b))
				},
			},
			{
				name: "empty request, with default",
				query: agentcfg.Query{
					Service: agentcfg.Service{
						Name: "all",
					},
				},
				agentCfgIndexModifier: func(t *testing.T, endpoint string) {
					writeAgentIndex(t, endpoint, "abcd", map[string]string{"name": ""}, map[string]string{"transaction_max_spans": "124"})
				},

				expectedStatusCode: http.StatusOK,
				expectedBody: func(t *testing.T, b []byte) bool {
					return assert.JSONEq(t, string([]byte(`{"_source":{"settings":{"transaction_max_spans":"124"},"etag":"abcd","agent_name":""}}`)), string(b))
				},
			},
			{
				name: "new service configuration",
				query: agentcfg.Query{
					Service: agentcfg.Service{
						Name: "test-agent",
					},
				},
				agentCfgIndexModifier: func(t *testing.T, endpoint string) {
					writeAgentIndex(t, endpoint, "abc", map[string]string{"name": "test-agent"}, map[string]string{"transaction_max_spans": "123"})
				},

				expectedStatusCode: http.StatusOK,
				expectedBody: func(t *testing.T, b []byte) bool {
					return assert.JSONEq(t, string([]byte(`{"_source":{"settings":{"transaction_max_spans":"123"},"etag":"abc","agent_name":""}}`)), string(b))
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
				agentCfgIndexModifier: func(t *testing.T, endpoint string) {
					writeAgentIndex(t, endpoint, "abc", map[string]string{"name": "test-agent-1", "environment": "not-demo"}, map[string]string{"transaction_max_spans": "124"})
					writeAgentIndex(t, endpoint, "abc", map[string]string{"name": "test-agent-1", "environment": "demo"}, map[string]string{"transaction_max_spans": "125"})
				},

				expectedStatusCode: http.StatusOK,
				expectedBody: func(t *testing.T, b []byte) bool {
					return assert.JSONEq(t, string([]byte(`{"_source":{"settings":{"transaction_max_spans":"125"},"etag":"abc","agent_name":""}}`)), string(b))
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
				agentCfgIndexModifier: func(t *testing.T, endpoint string) {
					writeAgentIndex(t, endpoint, "test", map[string]string{"name": "test-agent-2"}, map[string]string{"transaction_max_spans": "123"})
				},

				expectedStatusCode: http.StatusNotModified,
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
				agentCfgIndexModifier: func(t *testing.T, endpoint string) {
					writeAgentIndex(t, endpoint, "new", map[string]string{"name": "test-agent-3"}, map[string]string{"transaction_max_spans": "1"})
				},

				expectedStatusCode: http.StatusOK,
				expectedBody: func(t *testing.T, b []byte) bool {
					return assert.JSONEq(t, string([]byte(`{"_source":{"settings":{"transaction_max_spans":"1"},"etag":"new","agent_name":""}}`)), string(b))
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
		createApmConfigIndex(t, esEndpoint)

		rcvrFactory := NewFactory()
		cfg := &Config{
			ServerConfig: confighttp.ServerConfig{
				Endpoint: testEndpoint,
			},
			Elasticsearch: ElasticSearchClient{
				Endpoints:     []string{esEndpoint},
				CacheDuration: 100 * time.Millisecond,
			},
		}
		rcvr, err := rcvrFactory.CreateMetrics(ttCtx, receivertest.NewNopSettings(), cfg, consumertest.NewNop())
		require.NoError(t, err)

		err = rcvr.Start(ttCtx, componenttest.NewNopHost())
		require.NoError(t, err)

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tt.agentCfgIndexModifier(t, esEndpoint)

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

					return assert.Equal(t, tt.expectedStatusCode, res.StatusCode) && tt.expectedBody(t, bodyBytes)
				}, 30*time.Second, 1*time.Second)
			})
		}

		err = rcvr.Shutdown(ttCtx)
		require.NoError(t, err)
	}
}

func createApmConfigIndex(t *testing.T, endpoint string) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			endpoint,
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	resCreate, err := es.Indices.Create(
		indexName,
	)
	require.NoError(t, err)
	defer resCreate.Body.Close()
	require.NoError(t, err)
}

func writeAgentIndex(t *testing.T, endpoint, etag string, service, settings map[string]string) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			endpoint,
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	require.NoError(t, err)

	doc := map[string]interface{}{
		"service":          service,
		"settings":         settings,
		"@timestamp":       time.Now().UnixMilli(),
		"applied_by_agent": false,
		"etag":             etag,
	}

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(doc)
	require.NoError(t, err)

	res, err := es.Index(
		indexName,
		&buf,
		es.Index.WithRefresh("true"), // Ensure the document is available immediately
	)
	require.NoError(t, err)
	defer res.Body.Close()

	require.True(t, !res.IsError())
}
