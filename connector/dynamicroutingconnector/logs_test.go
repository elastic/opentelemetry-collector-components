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
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap/zaptest"
)

func TestLogsRouting(t *testing.T) {
	pipelineDefault := pipeline.NewIDWithName(pipeline.SignalTraces, "default")
	pipeline_0_2 := pipeline.NewIDWithName(pipeline.SignalLogs, "thershold_0_2")
	pipeline_2_5 := pipeline.NewIDWithName(pipeline.SignalLogs, "thershold_2_5")
	pipeline_5_inf := pipeline.NewIDWithName(pipeline.SignalLogs, "thershold_5_inf")
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
		name               string
		ctx                context.Context
		evaluationInterval time.Duration
		initialData        []plog.Logs
		input              plog.Logs
		expectSinkDefault  []plog.Logs
		expectSink_0_2     []plog.Logs
		expectSink_2_5     []plog.Logs
		expectSink_5_inf   []plog.Logs
	}{
		{
			name:               "primay_key_missing",
			ctx:                t.Context(),
			evaluationInterval: time.Second,
			initialData: []plog.Logs{
				newTestLogs("1", "1", "1"),
			},
			input: newTestLogs("2", "2", "2"),
			expectSinkDefault: []plog.Logs{
				newTestLogs("1", "1", "1"),
				newTestLogs("2", "2", "2"),
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
			initialData: []plog.Logs{
				newTestLogs("1", "1", "1"),
			},
			input: newTestLogs("2", "2", "2"),
			expectSinkDefault: []plog.Logs{
				newTestLogs("1", "1", "1"),
				newTestLogs("2", "2", "2"),
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
			initialData: []plog.Logs{
				newTestLogs("1", "1", "1"),
			},
			input: newTestLogs("2", "2", "2"),
			expectSinkDefault: []plog.Logs{
				newTestLogs("1", "1", "1"),
			},
			expectSink_0_2: []plog.Logs{
				newTestLogs("2", "2", "2"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sinkDefault, sink_0_2, sink_2_5, sink_5_inf consumertest.LogsSink
			routerAndConsumer := connector.NewLogsRouter(map[pipeline.ID]consumer.Logs{
				pipelineDefault: &sinkDefault,
				pipeline_0_2:    &sink_0_2,
				pipeline_2_5:    &sink_2_5,
				pipeline_5_inf:  &sink_5_inf,
			})

			cfg.EvaluationInterval = tc.evaluationInterval
			connectorSet := connectortest.NewNopSettings(metadata.Type)
			connectorSet.Logger = zaptest.NewLogger(t)
			connector, err := NewFactory().CreateLogsToLogs(
				t.Context(),
				connectorSet,
				&cfg,
				routerAndConsumer.(consumer.Logs),
			)
			require.NoError(t, err)

			ctx := t.Context()
			if tc.ctx != nil {
				ctx = tc.ctx
			}

			router := connector.(*logsConnector).router
			for _, d := range tc.initialData {
				require.NoError(t, connector.ConsumeLogs(ctx, d))
			}
			// Update the decisions to be based on initial data
			router.updateDecisions()

			require.NoError(t, connector.ConsumeLogs(ctx, tc.input))
			compareLogsSlice(t, tc.expectSinkDefault, sinkDefault.AllLogs())
			compareLogsSlice(t, tc.expectSink_0_2, sink_0_2.AllLogs())
			compareLogsSlice(t, tc.expectSink_2_5, sink_2_5.AllLogs())
			compareLogsSlice(t, tc.expectSink_5_inf, sink_5_inf.AllLogs())
		})
	}
}

func newTestLogs(resourceIDs, scopeIDs, logRecordIDs string) plog.Logs {
	ld := plog.NewLogs()
	for resourceN := 0; resourceN < len(resourceIDs); resourceN++ {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("resourceName", "resource"+string(resourceIDs[resourceN]))
		for scopeN := 0; scopeN < len(scopeIDs); scopeN++ {
			sl := rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName("scope" + string(scopeIDs[scopeN]))
			for logN := 0; logN < len(logRecordIDs); logN++ {
				lr := sl.LogRecords().AppendEmpty()
				lr.SetEventName("log" + string(logRecordIDs[logN]))
			}
		}
	}
	return ld
}

func compareLogsSlice(t *testing.T, expected []plog.Logs, actual []plog.Logs) {
	t.Helper()

	require.Equal(t, len(expected), len(actual))
	for i := 0; i < len(expected); i++ {
		assert.NoError(t, plogtest.CompareLogs(expected[i], actual[i]))
	}
}
