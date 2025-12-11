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
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/ptracetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap/zaptest"
)

func TestTracesRouting(t *testing.T) {
	pipelineDefault := pipeline.NewIDWithName(pipeline.SignalTraces, "default")
	pipeline_0_2 := pipeline.NewIDWithName(pipeline.SignalTraces, "thershold_0_2")
	pipeline_2_5 := pipeline.NewIDWithName(pipeline.SignalTraces, "thershold_2_5")
	pipeline_5_inf := pipeline.NewIDWithName(pipeline.SignalTraces, "thershold_5_inf")
	cfg := Config{
		DefaultPipelines: []pipeline.ID{pipelineDefault},
		DynamicPipelines: []DynamicPipeline{
			{Pipelines: []pipeline.ID{pipeline_0_2}, MaxCount: 2},
			{Pipelines: []pipeline.ID{pipeline_2_5}, MaxCount: 5},
			{Pipelines: []pipeline.ID{pipeline_5_inf}, MaxCount: math.Inf(1)},
		},
		PrimaryMetadataKeys: []string{"x-tenant-id"},
		MetadataKeys:        []string{"x-forwarded-for", "user-agent"},
	}

	for _, tc := range []struct {
		name               string
		ctx                context.Context
		evaluationInterval time.Duration
		initialData        []ptrace.Traces
		input              ptrace.Traces
		expectSinkDefault  []ptrace.Traces
		expectSink_0_2     []ptrace.Traces
		expectSink_2_5     []ptrace.Traces
		expectSink_5_inf   []ptrace.Traces
	}{
		{
			name:               "primay_key_missing",
			ctx:                t.Context(),
			evaluationInterval: time.Second,
			initialData: []ptrace.Traces{
				newTestTraces("1", "1", "1", "1"),
			},
			input: newTestTraces("2", "2", "2", "2"),
			expectSinkDefault: []ptrace.Traces{
				newTestTraces("1", "1", "1", "1"),
				newTestTraces("2", "2", "2", "2"),
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
			evaluationInterval: time.Second,
			initialData: []ptrace.Traces{
				newTestTraces("1", "1", "1", "1"),
			},
			input: newTestTraces("2", "2", "2", "2"),
			expectSinkDefault: []ptrace.Traces{
				newTestTraces("1", "1", "1", "1"),
				newTestTraces("2", "2", "2", "2"),
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
			evaluationInterval: time.Second,
			initialData: []ptrace.Traces{
				newTestTraces("1", "1", "1", "1"),
			},
			input: newTestTraces("2", "2", "2", "2"),
			expectSinkDefault: []ptrace.Traces{
				newTestTraces("1", "1", "1", "1"),
			},
			expectSink_0_2: []ptrace.Traces{
				newTestTraces("2", "2", "2", "2"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sinkDefault, sink_0_2, sink_2_5, sink_5_inf consumertest.TracesSink
			routerAndConsumer := connector.NewTracesRouter(map[pipeline.ID]consumer.Traces{
				pipelineDefault: &sinkDefault,
				pipeline_0_2:    &sink_0_2,
				pipeline_2_5:    &sink_2_5,
				pipeline_5_inf:  &sink_5_inf,
			})

			cfg.EvaluationInterval = tc.evaluationInterval
			connectorSet := connectortest.NewNopSettings(metadata.Type)
			connectorSet.TelemetrySettings.Logger = zaptest.NewLogger(t)
			connector, err := NewFactory().CreateTracesToTraces(
				t.Context(),
				connectorSet,
				&cfg,
				routerAndConsumer.(consumer.Traces),
			)
			require.NoError(t, err)

			ctx := t.Context()
			if tc.ctx != nil {
				ctx = tc.ctx
			}

			router := connector.(*tracesConnector).router
			for _, d := range tc.initialData {
				require.NoError(t, connector.ConsumeTraces(ctx, d))
			}
			// Update the decisions to be based on initial data
			router.updateDecisions()

			require.NoError(t, connector.ConsumeTraces(ctx, tc.input))
			compareTracesSlice(t, tc.expectSinkDefault, sinkDefault.AllTraces())
			compareTracesSlice(t, tc.expectSink_0_2, sink_0_2.AllTraces())
			compareTracesSlice(t, tc.expectSink_2_5, sink_2_5.AllTraces())
			compareTracesSlice(t, tc.expectSink_5_inf, sink_5_inf.AllTraces())
		})
	}
}

func newTestTraces(resourceIDs, scopeIDs, spanIDs, spanEventIDs string) ptrace.Traces {
	td := ptrace.NewTraces()
	for resourceN := 0; resourceN < len(resourceIDs); resourceN++ {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("resourceName", "resource"+string(resourceIDs[resourceN]))
		for scopeN := 0; scopeN < len(scopeIDs); scopeN++ {
			ss := rs.ScopeSpans().AppendEmpty()
			ss.Scope().SetName("scope" + string(scopeIDs[scopeN]))
			for spanN := 0; spanN < len(spanIDs); spanN++ {
				s := ss.Spans().AppendEmpty()
				s.SetName("span" + string(spanIDs[spanN]))
				for spanEventN := 0; spanEventN < len(spanEventIDs); spanEventN++ {
					se := s.Events().AppendEmpty()
					se.Attributes().PutStr("spanEventName", "spanEvent"+string(spanEventIDs[spanEventN]))
				}
			}
		}
	}
	return td
}

func compareTracesSlice(t *testing.T, expected []ptrace.Traces, actual []ptrace.Traces) {
	t.Helper()

	require.Equal(t, len(expected), len(actual))
	for i := 0; i < len(expected); i++ {
		assert.NoError(t, ptracetest.CompareTraces(expected[i], actual[i]))
	}
}
