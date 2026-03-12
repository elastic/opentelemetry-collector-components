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

package dynamicroutingconnector

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/elastic/opentelemetry-collector-components/connector/dynamicroutingconnector/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/connector/xconnector"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/pipeline/xpipeline"
	"go.uber.org/zap/zaptest"
)

func TestProfilesRouting(t *testing.T) {
	pipelineDefault := pipeline.NewIDWithName(xpipeline.SignalProfiles, "default")
	pipeline_0_2 := pipeline.NewIDWithName(xpipeline.SignalProfiles, "thershold_0_2")
	pipeline_2_5 := pipeline.NewIDWithName(xpipeline.SignalProfiles, "thershold_2_5")
	pipeline_5_inf := pipeline.NewIDWithName(xpipeline.SignalProfiles, "thershold_5_inf")
	cfg := Config{
		RoutingKeys: RoutingKeys{
			PartitionBy: []string{"x-tenant-id"},
			MeasureBy:   []string{"x-forwarded-for", "user-agent"},
		},
		DefaultPipelines: []pipeline.ID{pipelineDefault},
		RoutingPipelines: []RoutingPipeline{
			{Pipelines: []pipeline.ID{pipeline_0_2}, MaxCardinality: 2},
			{Pipelines: []pipeline.ID{pipeline_2_5}, MaxCardinality: 5},
			{Pipelines: []pipeline.ID{pipeline_5_inf}, MaxCardinality: math.Inf(1)},
		},
	}

	for _, tc := range []struct {
		name              string
		ctx               context.Context
		recordingInterval time.Duration
		initialData       []pprofile.Profiles
		input             pprofile.Profiles
		expectSinkDefault []pprofile.Profiles
		expectSink_0_2    []pprofile.Profiles
		expectSink_2_5    []pprofile.Profiles
		expectSink_5_inf  []pprofile.Profiles
	}{
		{
			name:              "primay_key_missing",
			ctx:               t.Context(),
			recordingInterval: time.Second,
			initialData: []pprofile.Profiles{
				newTestProfiles("1", "1"),
			},
			input: newTestProfiles("2", "2"),
			expectSinkDefault: []pprofile.Profiles{
				newTestProfiles("1", "1"),
				newTestProfiles("2", "2"),
			},
		},
		{
			name: "metadata_attrs_missing",
			ctx: client.NewContext(
				t.Context(),
				client.Info{
					Metadata: client.NewMetadata(map[string][]string{
						"x-forwarded-for": {"10.2.4.2"},
						"user-agent":      {"otel-0.135.0"},
					}),
				},
			),
			recordingInterval: time.Second,
			initialData: []pprofile.Profiles{
				newTestProfiles("1", "1"),
			},
			input: newTestProfiles("2", "2"),
			expectSinkDefault: []pprofile.Profiles{
				newTestProfiles("1", "1"),
				newTestProfiles("2", "2"),
			},
		},
		{
			name: "happy_path",
			ctx: client.NewContext(
				t.Context(),
				client.Info{
					Metadata: client.NewMetadata(map[string][]string{
						"x-tenant-id":     {"tenant-1"},
						"x-forwarded-for": {"10.2.4.2"},
						"user-agent":      {"otel-0.135.0"},
					}),
				},
			),
			recordingInterval: time.Second,
			initialData: []pprofile.Profiles{
				newTestProfiles("1", "1"),
			},
			input: newTestProfiles("2", "2"),
			expectSinkDefault: []pprofile.Profiles{
				newTestProfiles("1", "1"),
			},
			expectSink_0_2: []pprofile.Profiles{
				newTestProfiles("2", "2"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sinkDefault, sink_0_2, sink_2_5, sink_5_inf consumertest.ProfilesSink
			routerAndConsumer := xconnector.NewProfilesRouter(map[pipeline.ID]xconsumer.Profiles{
				pipelineDefault: &sinkDefault,
				pipeline_0_2:    &sink_0_2,
				pipeline_2_5:    &sink_2_5,
				pipeline_5_inf:  &sink_5_inf,
			})

			cfg.RecordingInterval = tc.recordingInterval
			cfg.TTL = 5 * tc.recordingInterval
			connectorSet := connectortest.NewNopSettings(metadata.Type)
			connectorSet.Logger = zaptest.NewLogger(t)
			conn, err := NewFactory().(xconnector.Factory).CreateProfilesToProfiles(
				t.Context(),
				connectorSet,
				&cfg,
				routerAndConsumer.(xconsumer.Profiles),
			)
			require.NoError(t, err)

			ctx := t.Context()
			if tc.ctx != nil {
				ctx = tc.ctx
			}

			router := conn.(*profilesConnector).router
			for _, d := range tc.initialData {
				require.NoError(t, conn.ConsumeProfiles(ctx, d))
			}
			router.updateDecisions()

			require.NoError(t, conn.ConsumeProfiles(ctx, tc.input))
			compareProfilesSlice(t, tc.expectSinkDefault, sinkDefault.AllProfiles())
			compareProfilesSlice(t, tc.expectSink_0_2, sink_0_2.AllProfiles())
			compareProfilesSlice(t, tc.expectSink_2_5, sink_2_5.AllProfiles())
			compareProfilesSlice(t, tc.expectSink_5_inf, sink_5_inf.AllProfiles())
		})
	}
}

func newTestProfiles(resourceIDs, scopeIDs string) pprofile.Profiles {
	pd := pprofile.NewProfiles()
	for resourceN := 0; resourceN < len(resourceIDs); resourceN++ {
		rp := pd.ResourceProfiles().AppendEmpty()
		rp.Resource().Attributes().PutStr("resourceName", "resource"+string(resourceIDs[resourceN]))
		for scopeN := 0; scopeN < len(scopeIDs); scopeN++ {
			sp := rp.ScopeProfiles().AppendEmpty()
			sp.Scope().SetName("scope" + string(scopeIDs[scopeN]))
			sp.Profiles().AppendEmpty()
		}
	}
	return pd
}

func compareProfilesSlice(t *testing.T, expected []pprofile.Profiles, actual []pprofile.Profiles) {
	t.Helper()

	require.Equal(t, len(expected), len(actual))
	marshaler := &pprofile.JSONMarshaler{}
	for i := range expected {
		expectedBytes, err := marshaler.MarshalProfiles(expected[i])
		require.NoError(t, err)
		actualBytes, err := marshaler.MarshalProfiles(actual[i])
		require.NoError(t, err)
		assert.JSONEq(t, string(expectedBytes), string(actualBytes))
	}
}
