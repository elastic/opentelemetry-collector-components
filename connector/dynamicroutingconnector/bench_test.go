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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/connector/dynamicroutingconnector/internal/metadata"
)

// BenchmarkProcess measures the hot path through Process() under three conditions:
//   - no_static_routes: no static routes configured, goes straight to dynamic routing
//   - first_match: first static route matches, exits immediately
//   - no_match/N: N static routes configured, none match, full scan then dynamic routing
func BenchmarkProcess(b *testing.B) {
	const nRoutes = 10

	pipelineDefault := pipeline.NewIDWithName(pipeline.SignalMetrics, "default")
	pipelineStatic := pipeline.NewIDWithName(pipeline.SignalMetrics, "static")
	pipelineInf := pipeline.NewIDWithName(pipeline.SignalMetrics, "inf")

	// noMatchRoutes returns n static routes that never match the benchmark input.
	noMatchRoutes := func(n int) []StaticRoute {
		routes := make([]StaticRoute, n)
		for i := range routes {
			routes[i] = StaticRoute{
				Conditions: []string{fmt.Sprintf(`otelcol.client.metadata["x-tenant-id"][0] == "tenant-%d"`, i)},
				Pipelines:  []pipeline.ID{pipelineStatic},
			}
		}
		return routes
	}

	cases := []struct {
		name         string
		staticRoutes []StaticRoute
		inputMeta    map[string][]string
	}{
		{
			name:         "no_static_routes",
			staticRoutes: nil,
			inputMeta:    map[string][]string{"x-tenant-id": {"gold"}},
		},
		{
			name: "first_match",
			staticRoutes: []StaticRoute{{
				Conditions: []string{`otelcol.client.metadata["x-tenant-id"][0] == "gold"`},
				Pipelines:  []pipeline.ID{pipelineStatic},
			}},
			inputMeta: map[string][]string{"x-tenant-id": {"gold"}},
		},
		{
			name:         fmt.Sprintf("no_match/%d_routes", 1),
			staticRoutes: noMatchRoutes(1),
			inputMeta:    map[string][]string{"x-tenant-id": {"regular"}},
		},
		{
			name:         fmt.Sprintf("no_match/%d_routes", nRoutes),
			staticRoutes: noMatchRoutes(nRoutes),
			inputMeta:    map[string][]string{"x-tenant-id": {"regular"}},
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			routerAndConsumer := connector.NewMetricsRouter(map[pipeline.ID]consumer.Metrics{
				pipelineDefault: consumertest.NewNop(),
				pipelineStatic:  consumertest.NewNop(),
				pipelineInf:     consumertest.NewNop(),
			})

			cfg := &Config{
				RoutingKeys:       RoutingKeys{PartitionBy: []string{"x-tenant-id"}, MeasureBy: []string{"x-forwarded-for"}},
				DefaultPipelines:  []pipeline.ID{pipelineDefault},
				RoutingPipelines:  []RoutingPipeline{{Pipelines: []pipeline.ID{pipelineInf}, MaxCardinality: math.Inf(1)}},
				RecordingInterval: time.Second,
				TTL:               5 * time.Second,
				StaticRoutes:      tc.staticRoutes,
			}

			connSet := connectortest.NewNopSettings(metadata.Type)
			connSet.Logger = zap.NewNop()
			conn, err := NewFactory().CreateMetricsToMetrics(
				context.Background(), connSet, cfg, routerAndConsumer.(consumer.Metrics),
			)
			require.NoError(b, err)
			require.NoError(b, conn.Start(context.Background(), nil))
			b.Cleanup(func() { _ = conn.Shutdown(context.Background()) })

			ctx := contextWithMetadata(tc.inputMeta)
			md := newTestMetrics("1", "1", "1", "1")

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = conn.ConsumeMetrics(ctx, md)
			}
		})
	}
}
