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
	"testing"
	"time"

	"github.com/elastic/opentelemetry-collector-components/connector/dynamicroutingconnector/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap/zaptest"
)

func TestMetricsRouting(t *testing.T) {
	pipelineDefault := pipeline.NewIDWithName(pipeline.SignalMetrics, "default")
	pipeline_0_2 := pipeline.NewIDWithName(pipeline.SignalMetrics, "thershold_0_2")
	pipeline_2_5 := pipeline.NewIDWithName(pipeline.SignalMetrics, "thershold_2_5")
	pipeline_5_inf := pipeline.NewIDWithName(pipeline.SignalMetrics, "thershold_5_inf")
	cfg := Config{
		DefaultPipelines: []pipeline.ID{pipelineDefault},
		Pipelines: [][]pipeline.ID{
			{pipeline_0_2},
			{pipeline_2_5},
			{pipeline_5_inf},
		},
		Thresholds:         []int{2, 5},
		PrimaryMetadataKey: "x-tenant-id",
		MetadataKeys:       []string{"x-forwarded-for", "user-agent"},
	}

	for _, tc := range []struct {
		name               string
		ctx                context.Context
		evaluationInterval time.Duration
		initialData        []pmetric.Metrics
		input              pmetric.Metrics
		expectSinkDefault  []pmetric.Metrics
		expectSink_0_2     []pmetric.Metrics
		expectSink_2_5     []pmetric.Metrics
		expectSink_5_inf   []pmetric.Metrics
	}{
		{
			name:               "primay_key_missing",
			ctx:                t.Context(),
			evaluationInterval: time.Second,
			initialData: []pmetric.Metrics{
				newTestMetrics("1", "1", "1", "1"),
			},
			input: newTestMetrics("2", "2", "2", "2"),
			expectSinkDefault: []pmetric.Metrics{
				newTestMetrics("1", "1", "1", "1"),
				newTestMetrics("2", "2", "2", "2"),
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
			initialData: []pmetric.Metrics{
				newTestMetrics("1", "1", "1", "1"),
			},
			input: newTestMetrics("2", "2", "2", "2"),
			expectSinkDefault: []pmetric.Metrics{
				newTestMetrics("1", "1", "1", "1"),
				newTestMetrics("2", "2", "2", "2"),
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
			initialData: []pmetric.Metrics{
				newTestMetrics("1", "1", "1", "1"),
			},
			input: newTestMetrics("2", "2", "2", "2"),
			expectSinkDefault: []pmetric.Metrics{
				newTestMetrics("1", "1", "1", "1"),
			},
			expectSink_0_2: []pmetric.Metrics{
				newTestMetrics("2", "2", "2", "2"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sinkDefault, sink_0_2, sink_2_5, sink_5_inf consumertest.MetricsSink
			routerAndConsumer := connector.NewMetricsRouter(map[pipeline.ID]consumer.Metrics{
				pipelineDefault: &sinkDefault,
				pipeline_0_2:    &sink_0_2,
				pipeline_2_5:    &sink_2_5,
				pipeline_5_inf:  &sink_5_inf,
			})

			cfg.EvaluationInterval = tc.evaluationInterval
			connectorSet := connectortest.NewNopSettings(metadata.Type)
			connectorSet.TelemetrySettings.Logger = zaptest.NewLogger(t)
			connector, err := NewFactory().CreateMetricsToMetrics(
				t.Context(),
				connectorSet,
				&cfg,
				routerAndConsumer.(consumer.Metrics),
			)
			require.NoError(t, err)

			ctx := t.Context()
			if tc.ctx != nil {
				ctx = tc.ctx
			}

			router := connector.(*metricsConnector).router
			for _, d := range tc.initialData {
				require.NoError(t, connector.ConsumeMetrics(ctx, d))
			}
			// Update the decisions to be based on initial data
			router.updateDecisions()

			require.NoError(t, connector.ConsumeMetrics(ctx, tc.input))
			compareMetricsSlice(t, tc.expectSinkDefault, sinkDefault.AllMetrics())
			compareMetricsSlice(t, tc.expectSink_0_2, sink_0_2.AllMetrics())
			compareMetricsSlice(t, tc.expectSink_2_5, sink_2_5.AllMetrics())
			compareMetricsSlice(t, tc.expectSink_5_inf, sink_5_inf.AllMetrics())
		})
	}
}

func newTestMetrics(resourceIDs, scopeIDs, metricIDs, sumDPIDs string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for resourceN := 0; resourceN < len(resourceIDs); resourceN++ {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("resourceName", "resource"+string(resourceIDs[resourceN]))
		for scopeN := 0; scopeN < len(scopeIDs); scopeN++ {
			sm := rm.ScopeMetrics().AppendEmpty()
			sm.Scope().SetName("scope" + string(scopeIDs[scopeN]))
			for metricN := 0; metricN < len(metricIDs); metricN++ {
				m := sm.Metrics().AppendEmpty()
				m.SetName("metric" + string(metricIDs[metricN]))
				sum := m.SetEmptySum()
				for sumDPN := 0; sumDPN < len(sumDPIDs); sumDPN++ {
					dp := sum.DataPoints().AppendEmpty()
					dp.Attributes().PutStr("dpName", "dp"+string(sumDPIDs[sumDPN]))
				}
			}
		}
	}
	return md
}

func compareMetricsSlice(t *testing.T, expected []pmetric.Metrics, actual []pmetric.Metrics) {
	t.Helper()

	require.Equal(t, len(expected), len(actual))
	for i := 0; i < len(expected); i++ {
		assert.NoError(t, pmetrictest.CompareMetrics(expected[i], actual[i]))
	}
}
