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

package centralconfig // import "github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/elastic/centralconfig"

import (
	"context"
	"errors"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension/apmconfig"
	"github.com/elastic/opentelemetry-lib/agentcfg"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type agentFetcherMock struct {
	fetchFn func(context.Context, agentcfg.Query) (agentcfg.Result, error)
}

func (f *agentFetcherMock) Fetch(ctx context.Context, query agentcfg.Query) (agentcfg.Result, error) {
	return f.fetchFn(ctx, query)
}

func TestRemoteConfig(t *testing.T) {
	testcases := map[string]struct {
		query         apmconfig.Query
		mockedFetchFn func(context.Context, agentcfg.Query) (agentcfg.Result, error)

		expectedRemoteConfig *protobufs.AgentRemoteConfig
		expectedError        error
	}{
		"no identifying attributes": {
			query: apmconfig.Query{
				InstanceUid: []byte("test-agent"),
			},
			mockedFetchFn: func(context.Context, agentcfg.Query) (agentcfg.Result, error) {
				return agentcfg.Result{}, nil
			},
			expectedRemoteConfig: nil,
			expectedError:        apmconfig.UnidentifiedAgent,
		},
		"no service.name identifying attribute": {
			query: apmconfig.Query{
				InstanceUid: []byte("test-agent"),
				IdentifyingAttributes: []*protobufs.KeyValue{
					{
						Key:   "deployment.environment",
						Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "dev"}},
					},
				},
			},
			mockedFetchFn: func(context.Context, agentcfg.Query) (agentcfg.Result, error) {
				return agentcfg.Result{}, nil
			},
			expectedRemoteConfig: nil,
			expectedError:        apmconfig.UnidentifiedAgent,
		},
		"valid service.name": {
			query: apmconfig.Query{
				InstanceUid: []byte("test-agent"),
				IdentifyingAttributes: []*protobufs.KeyValue{
					{
						Key:   "service.name",
						Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "dev"}},
					},
				},
			},
			mockedFetchFn: func(context.Context, agentcfg.Query) (agentcfg.Result, error) {
				return agentcfg.Result{
					Source: agentcfg.Source{
						Etag: "abcd",
						Settings: agentcfg.Settings{
							"test": "aaa",
						},
					},
				}, nil
			},
			expectedRemoteConfig: &protobufs.AgentRemoteConfig{
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
			expectedError: nil,
		},
		"empty remote config for all services": {
			query: apmconfig.Query{
				InstanceUid: []byte("test-agent"),
				IdentifyingAttributes: []*protobufs.KeyValue{
					{
						Key:   "service.name",
						Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "dev"}},
					},
				},
			},
			mockedFetchFn: func(context.Context, agentcfg.Query) (agentcfg.Result, error) {
				return agentcfg.Result{
					Source: agentcfg.Source{
						Etag:     "-",
						Settings: agentcfg.Settings{},
					},
				}, nil
			},
			expectedRemoteConfig: &protobufs.AgentRemoteConfig{
				ConfigHash: []byte("-"),
				Config: &protobufs.AgentConfigMap{
					ConfigMap: map[string]*protobufs.AgentConfigFile{
						"elastic": {
							Body:        []byte(`{}`),
							ContentType: "application/json",
						},
					},
				},
			},
			expectedError: nil,
		},
		"last config hash matches etag": {
			query: apmconfig.Query{
				InstanceUid: []byte("test-agent"),
				IdentifyingAttributes: []*protobufs.KeyValue{
					{
						Key:   "service.name",
						Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "dev"}},
					},
				},
				LastConfigHash: []byte("-"),
			},
			mockedFetchFn: func(context.Context, agentcfg.Query) (agentcfg.Result, error) {
				return agentcfg.Result{
					Source: agentcfg.Source{
						Etag:     "-",
						Settings: agentcfg.Settings{},
					},
				}, nil
			},
			expectedRemoteConfig: nil,
			expectedError:        nil,
		},
		"fetcher error": {
			query: apmconfig.Query{
				InstanceUid: []byte("test-agent"),
				IdentifyingAttributes: []*protobufs.KeyValue{
					{
						Key:   "service.name",
						Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "dev"}},
					},
				},
			},
			mockedFetchFn: func(context.Context, agentcfg.Query) (agentcfg.Result, error) {
				return agentcfg.Result{}, errors.New("mocked fetch error")
			},
			expectedRemoteConfig: nil,
			expectedError:        errors.New("mocked fetch error"),
		},
	}

	for name, tt := range testcases {
		t.Run(name, func(t *testing.T) {
			fetcher := NewFetcherAPMWatcher(&agentFetcherMock{
				fetchFn: tt.mockedFetchFn,
			}, zap.NewNop())

			actualRemoteConfig, actualError := fetcher.RemoteConfig(context.Background(), tt.query)
			if tt.expectedError != nil {
				assert.ErrorContains(t, actualError, tt.expectedError.Error())
			}
			assert.Equal(t, tt.expectedRemoteConfig, actualRemoteConfig)
		})
	}
}
