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

package elasticapmintakereceiver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/ptracetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/internal/testutil"
	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/metadata"
	"github.com/elastic/opentelemetry-lib/agentcfg"
)

var update = flag.Bool("update", false, "Flag to generate/updated the expected yaml files")

const testData = "testdata"

type fetcherMock struct {
	fetchFn func(context.Context, agentcfg.Query) (agentcfg.Result, error)
}

func (f *fetcherMock) Fetch(ctx context.Context, query agentcfg.Query) (agentcfg.Result, error) {
	return f.fetchFn(ctx, query)
}

func TestAgentCfgHandlerNoFetcher(t *testing.T) {
	testEndpoint := testutil.GetAvailableLocalAddress(t)
	rcvr, err := newElasticAPMIntakeReceiver(func(ctx context.Context, h component.Host) (agentcfg.Fetcher, error) {
		return nil, nil
	}, &Config{
		ServerConfig: confighttp.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint:  testEndpoint,
				Transport: confignet.TransportTypeTCP,
			},
		},
	}, receivertest.NewNopSettings(metadata.Type))
	require.NoError(t, err)

	ttCtx := context.Background()
	err = rcvr.Start(ttCtx, componenttest.NewNopHost())
	require.NoError(t, err)

	jsonQuery, err := json.Marshal(agentcfg.Query{})
	require.NoError(t, err)

	r, err := http.NewRequest("POST", "http://"+testEndpoint+agentConfigPath, bytes.NewBuffer(jsonQuery))
	require.NoError(t, err)

	r.Header.Add("Content-Type", "application/json")
	client := http.DefaultClient
	res, err := client.Do(r)
	require.NoError(t, err)

	assert.Equal(t, http.StatusForbidden, res.StatusCode)
	bodyBytes, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	assert.JSONEq(t, string([]byte(`{"error":"remote configuration fetcher not enabled"}`)), string(bodyBytes))
	require.NoError(t, res.Body.Close())

	err = rcvr.Shutdown(ttCtx)
	require.NoError(t, err)
}

func TestAgentCfgHandlerInvalidFetcher(t *testing.T) {
	tests := []struct {
		name  string
		query agentcfg.Query

		expectedStatusCode int
		expectedBody       []byte
	}{
		{
			name:  "empty request",
			query: agentcfg.Query{},

			expectedStatusCode: http.StatusServiceUnavailable,
			expectedBody:       []byte(`{"error":"no fetcher"}`),
		},
		{
			name: "service name request",
			query: agentcfg.Query{
				Service: agentcfg.Service{
					Name: "test-agent",
				},
			},

			expectedStatusCode: http.StatusServiceUnavailable,
			expectedBody:       []byte(`{"error":"no fetcher"}`),
		},
	}

	invalidFetcher := func(ctx context.Context, h component.Host) (agentcfg.Fetcher, error) {
		return nil, errors.New("no fetcher")
	}

	testEndpoint := testutil.GetAvailableLocalAddress(t)
	rcvr, err := newElasticAPMIntakeReceiver(invalidFetcher, &Config{
		ServerConfig: confighttp.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint:  testEndpoint,
				Transport: confignet.TransportTypeTCP,
			},
		},
	}, receivertest.NewNopSettings(metadata.Type))
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
			rcvr, err := newElasticAPMIntakeReceiver(tt.fetcher, &Config{
				ServerConfig: confighttp.ServerConfig{
					NetAddr: confignet.AddrConfig{
						Endpoint:  testEndpoint,
						Transport: confignet.TransportTypeTCP,
					},
				},
				AgentConfig: AgentConfig{
					CacheDuration: 1 * time.Second,
				},
			}, receivertest.NewNopSettings(metadata.Type))
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

func TestInvalidInput(t *testing.T) {
	inputFiles_invalid := []struct {
		inputNdJsonFileName          string
		expectedErrorMessageFileName string
	}{
		{"invalid-event.ndjson", "invalid-event-expected.txt"},
		{"invalid-event-type.ndjson", "invalid-event-type-expected.txt"},
		{"invalid-json-event.ndjson", "invalid-json-event-expected.txt"},
		{"invalid-json-metadata.ndjson", "invalid-json-metadata-expected.txt"},
		{"invalid-metadata-2.ndjson", "invalid-metadata-2-expected.txt"},
		{"invalid-metadata.ndjson", "invalid-metadata-expected.txt"},
		{"invalid-metadata.ndjson", "invalid-metadata-expected.txt"},
		{"missing-agent-metadata.ndjson", "missing-agent-metadata-expected.txt"},
	}
	factory := NewFactory()
	testEndpoint := testutil.GetAvailableLocalAddress(t)
	cfg := &Config{
		ServerConfig: confighttp.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint:  testEndpoint,
				Transport: confignet.TransportTypeTCP,
			},
		},
	}

	set := receivertest.NewNopSettings(metadata.Type)
	nextTrace := new(consumertest.TracesSink)
	receiver, _ := factory.CreateTraces(context.Background(), set, cfg, nextTrace)

	if err := receiver.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Errorf("Starting receiver failed: %v", err)
	}
	defer func() {
		if err := receiver.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown failed: %v", err)
		}
	}()

	for _, tt := range inputFiles_invalid {
		t.Run(tt.inputNdJsonFileName, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(testData, tt.inputNdJsonFileName))
			if err != nil {
				t.Fatalf("failed to read file: %v", err)
			}

			resp, err := http.Post("http://"+testEndpoint+intakeV2EventsPath, "application/x-ndjson", bytes.NewBuffer(data))
			if err != nil {
				t.Fatalf("failed to send HTTP request: %v", err)
			}

			defer func() {
				_ = resp.Body.Close()
			}()

			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			bodyStr := string(bodyBytes)

			expectedErrorFile := filepath.Join(testData, tt.expectedErrorMessageFileName)
			expectedErrorBytes, err := os.ReadFile(expectedErrorFile)
			if err != nil {
				t.Fatalf("failed to read expected error file: %v", err)
			}
			expectedError := string(expectedErrorBytes)
			if bodyStr != expectedError {
				t.Fatalf("unexpected response body: got %q, want %q", bodyStr, expectedError)
			}

			if resp.StatusCode < http.StatusBadRequest {
				t.Fatalf("unexpected status code - this request is invalid and should not be accepted. Status code: %v", resp.StatusCode)
			}
		})
	}
}

func TestErrors(t *testing.T) {
	inputFiles_error := []struct {
		inputNdJsonFileName        string
		outputExpectedYamlFileName string
	}{
		{"errors.ndjson", "errors_expected.yaml"},
	}
	factory := NewFactory()
	testEndpoint := testutil.GetAvailableLocalAddress(t)
	cfg := &Config{
		ServerConfig: confighttp.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint:  testEndpoint,
				Transport: confignet.TransportTypeTCP,
			},
		},
	}

	set := receivertest.NewNopSettings(metadata.Type)
	nextLog := new(consumertest.LogsSink)
	receiver, _ := factory.CreateLogs(context.Background(), set, cfg, nextLog)

	if err := receiver.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Errorf("Starting receiver failed: %v", err)
	}
	defer func() {
		if err := receiver.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown failed: %v", err)
		}
	}()

	for _, tt := range inputFiles_error {
		t.Run(tt.inputNdJsonFileName, func(t *testing.T) {
			runComparisonForLogs(t, tt.inputNdJsonFileName, tt.outputExpectedYamlFileName, nextLog, testEndpoint)
		})
	}
}

func TestMetrics(t *testing.T) {
	inputFiles_error := []struct {
		inputNdJsonFileName        string
		outputExpectedYamlFileName string
		expectedDynamicAttrs       []string
	}{
		{"metricsets.ndjson", "metricsets_expected.yaml", []string{"labels.tag1", "numeric_labels.tag2"}},
		{"multiple_histogram_metrics_samples.ndjson", "multiple_histogram_metrics_samples_expected.yaml", nil},
	}
	factory := NewFactory()
	testEndpoint := testutil.GetAvailableLocalAddress(t)
	cfg := &Config{
		ServerConfig: confighttp.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint:  testEndpoint,
				Transport: confignet.TransportTypeTCP,
			},
		},
	}

	set := receivertest.NewNopSettings(metadata.Type)
	nextMetrics := new(consumertest.MetricsSink)
	receiver, _ := factory.CreateMetrics(context.Background(), set, cfg, nextMetrics)

	if err := receiver.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Errorf("Starting receiver failed: %v", err)
	}
	defer func() {
		if err := receiver.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown failed: %v", err)
		}
	}()

	for _, tt := range inputFiles_error {
		t.Run(tt.inputNdJsonFileName, func(t *testing.T) {
			runComparisonForMetrics(t, tt.inputNdJsonFileName, tt.outputExpectedYamlFileName, nextMetrics, testEndpoint)
			if len(tt.expectedDynamicAttrs) > 0 {
				// validate the at least one global label key has been included in the client metadata
				ctxs := nextMetrics.Contexts()
				require.GreaterOrEqual(t, len(ctxs), 1)
				got := client.FromContext(ctxs[0]).Metadata.Get(elasticattr.MetadataDynamicResourceAttributes)
				require.ElementsMatch(t, tt.expectedDynamicAttrs, got)
			}
		})
	}
}

func TestLogs(t *testing.T) {
	inputFiles := []struct {
		inputNdJsonFileName        string
		outputExpectedYamlFileName string
		expectedDynamicAttrs       []string
	}{
		{"logs.ndjson", "logs_expected.yaml", []string{"labels.ab_testing", "labels.group", "numeric_labels.segment"}},
	}
	factory := NewFactory()
	testEndpoint := testutil.GetAvailableLocalAddress(t)
	cfg := &Config{
		ServerConfig: confighttp.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint:  testEndpoint,
				Transport: confignet.TransportTypeTCP,
			},
		},
	}

	set := receivertest.NewNopSettings(metadata.Type)
	nextLogs := new(consumertest.LogsSink)
	receiver, _ := factory.CreateLogs(context.Background(), set, cfg, nextLogs)

	if err := receiver.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Errorf("Starting receiver failed: %v", err)
	}
	defer func() {
		if err := receiver.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown failed: %v", err)
		}
	}()

	for _, tt := range inputFiles {
		t.Run(tt.inputNdJsonFileName, func(t *testing.T) {
			runComparisonForLogs(t, tt.inputNdJsonFileName, tt.outputExpectedYamlFileName, nextLogs, testEndpoint)
			if len(tt.expectedDynamicAttrs) > 0 {
				// validate the at least one global label key has been included in the client metadata
				ctxs := nextLogs.Contexts()
				require.GreaterOrEqual(t, len(ctxs), 1)
				got := client.FromContext(ctxs[0]).Metadata.Get(elasticattr.MetadataDynamicResourceAttributes)
				require.ElementsMatch(t, tt.expectedDynamicAttrs, got)
			}
		})
	}
}

var inputFiles = []struct {
	inputNdJsonFileName        string
	outputExpectedYamlFileName string
	expectedDynamicAttrs       []string
}{
	{"invalid_ids.ndjson", "invalid_ids_expected.yaml", nil},
	{"transactions.ndjson", "transactions_expected.yaml", []string{"labels.tag1", "numeric_labels.tag2"}},
	{"spans.ndjson", "spans_expected.yaml", []string{"labels.tag1"}},
	{"unknown-span-type.ndjson", "unknown-span-type_expected.yaml", nil},
	{"transactions_spans.ndjson", "transactions_spans_expected.yaml", nil},
	{"language_name_mapping.ndjson", "language_name_mapping_expected.yaml", nil},
	{"span-links.ndjson", "span-links_expected.yaml", nil},
	{"hostdata.ndjson", "hostdata_expected.yaml", nil},
}

func TestTransactionsAndSpans(t *testing.T) {
	factory := NewFactory()
	testEndpoint := testutil.GetAvailableLocalAddress(t)
	cfg := &Config{
		ServerConfig: confighttp.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint:  testEndpoint,
				Transport: confignet.TransportTypeTCP,
			},
		},
	}

	set := receivertest.NewNopSettings(metadata.Type)
	nextTrace := new(consumertest.TracesSink)
	receiver, _ := factory.CreateTraces(context.Background(), set, cfg, nextTrace)

	if err := receiver.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Errorf("Starting receiver failed: %v", err)
	}
	defer func() {
		if err := receiver.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown failed: %v", err)
		}
	}()

	for _, tt := range inputFiles {
		t.Run(tt.inputNdJsonFileName, func(t *testing.T) {
			runComparisonForTraces(t, tt.inputNdJsonFileName, tt.outputExpectedYamlFileName, nextTrace, testEndpoint)
			if len(tt.expectedDynamicAttrs) > 0 {
				// validate the at least one global label key has been included in the client metadata
				ctxs := nextTrace.Contexts()
				require.GreaterOrEqual(t, len(ctxs), 1)
				got := client.FromContext(ctxs[0]).Metadata.Get(elasticattr.MetadataDynamicResourceAttributes)
				require.ElementsMatch(t, tt.expectedDynamicAttrs, got)
			}
		})
	}
}

func TestMetadataPropagation(t *testing.T) {
	table := map[string]struct {
		includeMetadata  bool
		expectedMetadata client.Metadata
	}{
		"when include_metadata is disabled only mappinmapping-mode is propagated": {
			expectedMetadata: client.NewMetadata(map[string][]string{
				"x-elastic-mapping-mode": {"ecs"},
			}),
		},
		"when include_metadata is enabled all request metadata is propagated": {
			includeMetadata: true,
			expectedMetadata: client.NewMetadata(map[string][]string{
				"content-type":           {"application/x-ndjson"},
				"x-elastic-mapping-mode": {"ecs"},
			}),
		},
	}
	for tname, tcase := range table {
		t.Run(tname, func(t *testing.T) {
			factory := NewFactory()
			testEndpoint := testutil.GetAvailableLocalAddress(t)
			cfg := &Config{
				ServerConfig: confighttp.ServerConfig{
					NetAddr: confignet.AddrConfig{
						Endpoint:  testEndpoint,
						Transport: confignet.TransportTypeTCP,
					},
					IncludeMetadata: tcase.includeMetadata,
				},
			}

			set := receivertest.NewNopSettings(metadata.Type)
			nextTrace := new(consumertest.TracesSink)
			receiver, _ := factory.CreateTraces(context.Background(), set, cfg, nextTrace)

			if err := receiver.Start(context.Background(), componenttest.NewNopHost()); err != nil {
				t.Errorf("Starting receiver failed: %v", err)
			}
			defer func() {
				if err := receiver.Shutdown(context.Background()); err != nil {
					t.Errorf("Shutdown failed: %v", err)
				}
			}()

			sendInput(t, "transactions_spans.ndjson", testEndpoint)

			ctxs := nextTrace.Contexts()
			require.GreaterOrEqual(t, len(ctxs), 1)
			md := client.FromContext(ctxs[0]).Metadata
			if tcase.includeMetadata {
				for k := range tcase.expectedMetadata.Keys() {
					require.Equal(t, tcase.expectedMetadata.Get(k), md.Get(k))
				}
			} else {
				require.Equal(t, tcase.expectedMetadata, md)
			}
		})
	}
}

func TestGlobalLabelsMetadataPropagation(t *testing.T) {
	cases := []struct {
		name                 string
		inputFile            string
		signal               string // "traces" or "metrics"
		expectedDynamicAttrs []string
		// expectedPerGroupDynamicAttrs, if set, verifies each group's
		// context independently (one entry per ConsumeX call, in order).
		// When set, expectedDynamicAttrs is ignored.
		expectedPerGroupDynamicAttrs [][]string
		expectedYamlFile             string // if non-empty, compare output against this golden file
	}{
		{
			name:                 "metadata global labels propagated",
			inputFile:            "transactions.ndjson",
			signal:               "traces",
			expectedDynamicAttrs: []string{"labels.tag1", "numeric_labels.tag2"},
		},
		{
			// The apm-data library marks metadata labels as Global: true and
			// clones them onto every event. When an event has a tag with the
			// same key as a metadata label, Labels.Set() replaces the value
			// and resets Global to false on that event only.
			//
			// Events are grouped by their per-event global key set (bitmask)
			// to match apm-aggregation's per-event behavior:
			//   - Event 1: no tags → both globals retained → main batch
			//   - Events 2,3: shadow global_tag → grouped (same mask) → shadowed batch A
			//   - Event 4: shadows num_tag → different mask → shadowed batch B
			name:      "event-level tag shadows metadata global label",
			inputFile: "metric_global_label_shadow.ndjson",
			signal:    "metrics",
			expectedPerGroupDynamicAttrs: [][]string{
				{"labels.global_tag", "numeric_labels.num_tag"}, // main: event 1, both globals
				{"numeric_labels.num_tag"},                      // shadowed batch A: events 2,3 shadow global_tag
				{"labels.global_tag"},                           // shadowed batch B: event 4 shadows num_tag
			},
			expectedYamlFile: "metric_global_label_shadow_expected.yaml",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			factory := NewFactory()
			testEndpoint := testutil.GetAvailableLocalAddress(t)
			cfg := &Config{
				ServerConfig: confighttp.ServerConfig{
					NetAddr: confignet.AddrConfig{
						Endpoint:  testEndpoint,
						Transport: confignet.TransportTypeTCP,
					},
				},
			}
			set := receivertest.NewNopSettings(metadata.Type)

			var rcv component.Component
			var ctxsFn func() []context.Context
			var err error

			var metricsSink *consumertest.MetricsSink
			switch tc.signal {
			case "traces":
				sink := new(consumertest.TracesSink)
				rcv, err = factory.CreateTraces(context.Background(), set, cfg, sink)
				ctxsFn = sink.Contexts
			case "metrics":
				metricsSink = new(consumertest.MetricsSink)
				rcv, err = factory.CreateMetrics(context.Background(), set, cfg, metricsSink)
				ctxsFn = metricsSink.Contexts
			}
			require.NoError(t, err)

			require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
			defer func() {
				require.NoError(t, rcv.Shutdown(context.Background()))
			}()

			sendInput(t, tc.inputFile, testEndpoint)

			ctxs := ctxsFn()
			if tc.expectedPerGroupDynamicAttrs != nil {
				require.Len(t, ctxs, len(tc.expectedPerGroupDynamicAttrs))
				for i, expectedAttrs := range tc.expectedPerGroupDynamicAttrs {
					got := client.FromContext(ctxs[i]).Metadata.Get(elasticattr.MetadataDynamicResourceAttributes)
					assert.ElementsMatch(t, expectedAttrs, got, "mismatch at context index %d", i)
				}
			} else {
				require.GreaterOrEqual(t, len(ctxs), 1)
				got := client.FromContext(ctxs[0]).Metadata.Get(elasticattr.MetadataDynamicResourceAttributes)
				require.ElementsMatch(t, tc.expectedDynamicAttrs, got)
			}

			if tc.expectedYamlFile != "" && metricsSink != nil {
				// Merge metrics from all consume calls for golden file comparison.
				allMetrics := metricsSink.AllMetrics()
				actualMetrics := pmetric.NewMetrics()
				for _, m := range allMetrics {
					m.ResourceMetrics().MoveAndAppendTo(actualMetrics.ResourceMetrics())
				}
				expectedFile := filepath.Join(testData, tc.expectedYamlFile)
				if *update {
					err := golden.WriteMetrics(t, expectedFile, actualMetrics, golden.SkipMetricTimestampNormalization())
					require.NoError(t, err)
				}
				expectedMetrics, err := golden.ReadMetrics(expectedFile)
				require.NoError(t, err)
				require.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actualMetrics,
					pmetrictest.IgnoreMetricsOrder(),
					pmetrictest.IgnoreResourceMetricsOrder(),
				))
			}
		})
	}
}

func sendInput(t *testing.T, inputJsonFileName string, testEndpoint string) {
	data, err := os.ReadFile(filepath.Join(testData, inputJsonFileName))
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	resp, err := http.Post("http://"+testEndpoint+intakeV2EventsPath, "application/x-ndjson", bytes.NewBuffer(data))
	if err != nil {
		t.Fatalf("failed to send HTTP request: %v", err)
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusAccepted {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status code: %v, resp body: %s", resp.StatusCode, bodyBytes)
	}
}

func runComparisonForTraces(t *testing.T, inputJsonFileName string, expectedYamlFileName string,
	nextTrace *consumertest.TracesSink, testEndpoint string,
) {
	nextTrace.Reset()

	sendInput(t, inputJsonFileName, testEndpoint)
	// Merge traces from all consume calls (there may be multiple when
	// events are separated due to global label shadowing).
	allTraces := nextTrace.AllTraces()
	actualTraces := ptrace.NewTraces()
	for _, tr := range allTraces {
		tr.ResourceSpans().MoveAndAppendTo(actualTraces.ResourceSpans())
	}
	expectedFile := filepath.Join(testData, expectedYamlFileName)
	if *update {
		err := golden.WriteTraces(t, expectedFile, actualTraces)
		assert.NoError(t, err)
	}
	expectedTraces, err := golden.ReadTraces(expectedFile)
	require.NoError(t, err)
	require.NoError(t, ptracetest.CompareTraces(expectedTraces, actualTraces,
		ptracetest.IgnoreStartTimestamp(),
		ptracetest.IgnoreEndTimestamp(),
		ptracetest.IgnoreResourceSpansOrder(),
	))
}

func runComparisonForLogs(t *testing.T, inputJsonFileName string, expectedYamlFileName string,
	nextLog *consumertest.LogsSink, testEndpoint string,
) {
	nextLog.Reset()

	sendInput(t, inputJsonFileName, testEndpoint)
	allLogs := nextLog.AllLogs()
	actualLogs := plog.NewLogs()
	for _, l := range allLogs {
		l.ResourceLogs().MoveAndAppendTo(actualLogs.ResourceLogs())
	}
	expectedFile := filepath.Join(testData, expectedYamlFileName)
	if *update {
		err := golden.WriteLogs(t, expectedFile, actualLogs)
		assert.NoError(t, err)
	}
	expectedLogs, err := golden.ReadLogs(expectedFile)
	require.NoError(t, err)
	require.NoError(t, plogtest.CompareLogs(expectedLogs, actualLogs, plogtest.IgnoreLogRecordsOrder()))
}

func runComparisonForMetrics(t *testing.T, inputJsonFileName string, expectedYamlFileName string,
	nextMetric *consumertest.MetricsSink, testEndpoint string,
) {
	nextMetric.Reset()
	sendInput(t, inputJsonFileName, testEndpoint)
	allMetrics := nextMetric.AllMetrics()
	actualMetrics := pmetric.NewMetrics()
	for _, m := range allMetrics {
		m.ResourceMetrics().MoveAndAppendTo(actualMetrics.ResourceMetrics())
	}
	expectedFile := filepath.Join(testData, expectedYamlFileName)
	if *update {
		err := golden.WriteMetrics(t, expectedFile, actualMetrics, golden.SkipMetricTimestampNormalization())
		assert.NoError(t, err)
	}
	expectedMetrics, err := golden.ReadMetrics(expectedFile)
	require.NoError(t, err)
	require.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actualMetrics, pmetrictest.IgnoreMetricsOrder(),
		// golden.WriteMetrics will sort metrics and resource metrics before writing the golden file
		// so we need to ignore order when comparing.
		pmetrictest.IgnoreResourceMetricsOrder(),
	))
}
