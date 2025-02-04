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

package elasticapmreceiver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver/internal/testutil"
	"github.com/elastic/opentelemetry-lib/agentcfg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

type fetcherMock struct {
	fetchFn func(context.Context, agentcfg.Query) (agentcfg.Result, error)
}

func (f *fetcherMock) Fetch(ctx context.Context, query agentcfg.Query) (agentcfg.Result, error) {
	return f.fetchFn(ctx, query)
}

func TestAgentCfgHandlerNoFetcher(t *testing.T) {
	tests := []struct {
		name  string
		query agentcfg.Query

		expectedStatusCode int
		expectedBody       []byte
	}{
		{
			name:  "empty request",
			query: agentcfg.Query{},

			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       []byte(`{"error":"no fetcher"}`),
		},
		{
			name: "service name request",
			query: agentcfg.Query{
				Service: agentcfg.Service{
					Name: "test-agent",
				},
			},

			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       []byte(`{"error":"no fetcher"}`),
		},
	}

	invalidFetcher := func(ctx context.Context, h component.Host) (agentcfg.Fetcher, error) {
		return nil, errors.New("no fetcher")
	}

	testEndpoint := testutil.GetAvailableLocalAddress(t)
	rcvr, err := newElasticAPMReceiver(invalidFetcher, &Config{
		ServerConfig: confighttp.ServerConfig{
			Endpoint: testEndpoint,
		},
	}, receivertest.NewNopSettings())
	require.NoError(t, err)

	ttCtx := context.Background()
	err = rcvr.Start(ttCtx, componenttest.NewNopHost())
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonQuery, err := json.Marshal(tt.query)
			require.NoError(t, err)

			r, err := http.NewRequest("POST", "http://"+testEndpoint+agentConfigPath, bytes.NewBuffer(jsonQuery))
			require.NoError(t, err)

			r.Header.Add("Content-Type", "application/json")
			client := http.DefaultClient
			res, err := client.Do(r)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedStatusCode, res.StatusCode)
			bodyBytes, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			assert.JSONEq(t, string(tt.expectedBody), string(bodyBytes))
			require.NoError(t, res.Body.Close())
		})
	}

	err = rcvr.Shutdown(ttCtx)
	require.NoError(t, err)
}

func TestAgentCfgHandler(t *testing.T) {
	assertJsonBody := func(expectedBody, actualBody []byte) {
		assert.JSONEq(t, string(expectedBody), string(actualBody))
	}

	tests := []struct {
		name    string
		query   agentcfg.Query
		fetcher agentCfgFetcherFactory

		expectedStatusCode int
		expectedEtagHeader []string
		assertBodyFn       func([]byte)
	}{
		{
			name:  "empty request, service.name required",
			query: agentcfg.Query{},
			fetcher: func(ctx context.Context, h component.Host) (agentcfg.Fetcher, error) {
				return &fetcherMock{
					fetchFn: func(context.Context, agentcfg.Query) (agentcfg.Result, error) {
						return agentcfg.Result{}, nil
					},
				}, nil
			},

			expectedStatusCode: http.StatusBadRequest,
			assertBodyFn: func(expectedBody []byte) {
				assertJsonBody(expectedBody, []byte(`{"error":"service.name is required"}`))
			},
		},
		{
			name: "empty request, fetcher error",
			query: agentcfg.Query{
				Service: agentcfg.Service{
					Name: "test-agent",
				},
			},
			fetcher: func(ctx context.Context, h component.Host) (agentcfg.Fetcher, error) {
				return &fetcherMock{
					fetchFn: func(context.Context, agentcfg.Query) (agentcfg.Result, error) {
						return agentcfg.Result{}, errors.New("testing error")
					},
				}, nil
			},

			expectedStatusCode: http.StatusBadRequest,
			assertBodyFn: func(expectedBody []byte) {
				assertJsonBody(expectedBody, []byte(`{"error":"testing error"}`))
			},
		},
		{
			name: "not modified error",
			query: agentcfg.Query{
				Etag: "abc",
				Service: agentcfg.Service{
					Name: "test-agent",
				},
			},
			fetcher: func(ctx context.Context, h component.Host) (agentcfg.Fetcher, error) {
				return &fetcherMock{
					fetchFn: func(context.Context, agentcfg.Query) (agentcfg.Result, error) {
						return agentcfg.Result{
							Source: agentcfg.Source{
								Etag: "abc",
							},
						}, nil
					},
				}, nil
			},

			expectedStatusCode: http.StatusNotModified,
			expectedEtagHeader: []string{"\"abc\""},
			assertBodyFn: func(expectedBody []byte) {
				assert.Empty(t, expectedBody)
			},
		},
		{
			name: "new settings",
			query: agentcfg.Query{
				Etag: "abc",
				Service: agentcfg.Service{
					Name: "test-agent",
				},
			},
			fetcher: func(ctx context.Context, h component.Host) (agentcfg.Fetcher, error) {
				return &fetcherMock{
					fetchFn: func(context.Context, agentcfg.Query) (agentcfg.Result, error) {
						return agentcfg.Result{
							Source: agentcfg.Source{
								Settings: agentcfg.Settings{
									"transaction_max_spans": "123",
								},
								Etag: "cba",
							},
						}, nil
					},
				}, nil
			},

			expectedStatusCode: http.StatusOK,
			expectedEtagHeader: []string{"\"cba\""},
			assertBodyFn: func(expectedBody []byte) {
				assertJsonBody(expectedBody, []byte(`{"transaction_max_spans":"123"}`))
			},
		},
		{
			name: "new settings, no etag",
			query: agentcfg.Query{
				Service: agentcfg.Service{
					Name: "test-agent",
				},
			},
			fetcher: func(ctx context.Context, h component.Host) (agentcfg.Fetcher, error) {
				return &fetcherMock{
					fetchFn: func(context.Context, agentcfg.Query) (agentcfg.Result, error) {
						return agentcfg.Result{
							Source: agentcfg.Source{
								Settings: agentcfg.Settings{
									"transaction_max_spans": "123",
								},
								Etag: "cba",
							},
						}, nil
					},
				}, nil
			},

			expectedStatusCode: http.StatusOK,
			expectedEtagHeader: []string{"\"cba\""},
			assertBodyFn: func(expectedBody []byte) {
				assertJsonBody(expectedBody, []byte(`{"transaction_max_spans":"123"}`))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testEndpoint := testutil.GetAvailableLocalAddress(t)
			rcvr, err := newElasticAPMReceiver(tt.fetcher, &Config{
				ServerConfig: confighttp.ServerConfig{
					Endpoint: testEndpoint,
				},
			}, receivertest.NewNopSettings())
			require.NoError(t, err)

			ttCtx := context.Background()
			err = rcvr.Start(ttCtx, componenttest.NewNopHost())
			require.NoError(t, err)

			jsonQuery, err := json.Marshal(tt.query)
			require.NoError(t, err)

			r, err := http.NewRequest("POST", "http://"+testEndpoint+agentConfigPath, bytes.NewBuffer(jsonQuery))
			require.NoError(t, err)

			r.Header.Add("Content-Type", "application/json")
			client := http.DefaultClient
			res, err := client.Do(r)
			require.NoError(t, err)

			bodyBytes, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			tt.assertBodyFn(bodyBytes)
			require.NoError(t, res.Body.Close())

			assert.Equal(t, tt.expectedStatusCode, res.StatusCode)
			assert.Equal(t, tt.expectedEtagHeader, res.Header[Etag])

			err = rcvr.Shutdown(ttCtx)
			require.NoError(t, err)
		})
	}
}
