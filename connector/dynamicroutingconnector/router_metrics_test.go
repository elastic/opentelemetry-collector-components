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

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	"go.uber.org/zap/zaptest"

	"github.com/elastic/opentelemetry-collector-components/connector/dynamicroutingconnector/internal/metadatatest"
)

func TestDynamicroutingRoutedTelemetry(t *testing.T) {
	cfg := Config{
		RoutingKeys: RoutingKeys{
			PartitionBy: []string{"x-tenant-id"},
			MeasureBy:   []string{"x-forwarded-for"},
		},
		DefaultPipelines: []pipeline.ID{
			pipeline.NewIDWithName(pipeline.SignalMetrics, "default"),
		},
		RoutingPipelines: []RoutingPipeline{
			{Pipelines: []pipeline.ID{pipeline.NewIDWithName(pipeline.SignalMetrics, "bucket_0_2")}, MaxCardinality: 2},
			{Pipelines: []pipeline.ID{pipeline.NewIDWithName(pipeline.SignalMetrics, "bucket_2_5")}, MaxCardinality: 5},
			{Pipelines: []pipeline.ID{pipeline.NewIDWithName(pipeline.SignalMetrics, "bucket_5_inf")}, MaxCardinality: math.Inf(1)},
		},
	}

	tests := []struct {
		name       string
		ctxs       []context.Context
		setup      func(*router[consumer.Metrics], context.Context) string
		wantBucket string
	}{
		{
			name: "default_bucket",
			ctxs: []context.Context{
				context.Background(),
			},
			wantBucket: defaultCardinalityBucket,
		},
		{
			name: "cardinality_bucket",
			ctxs: []context.Context{
				client.NewContext(
					context.Background(),
					client.Info{
						Metadata: client.NewMetadata(map[string][]string{
							"x-tenant-id":     {"tenant-1"},
							"x-forwarded-for": {"10.2.4.2"},
						}),
					},
				),
			},
			setup: func(r *router[consumer.Metrics], ctx context.Context) string {
				pk := r.estimateCardinality(ctx)
				r.updateDecisions()
				return pk
			},
			wantBucket: "0_2",
		},
		{
			name: "multiple_tenants",
			ctxs: []context.Context{
				client.NewContext(
					context.Background(),
					client.Info{
						Metadata: client.NewMetadata(map[string][]string{
							"x-tenant-id":     {"tenant-1"},
							"x-forwarded-for": {"10.2.4.2"},
						}),
					},
				),
				client.NewContext(
					context.Background(),
					client.Info{
						Metadata: client.NewMetadata(map[string][]string{
							"x-tenant-id":     {"tenant-2"},
							"x-forwarded-for": {"10.2.4.3"},
						}),
					},
				),
			},
			setup: func(r *router[consumer.Metrics], ctx context.Context) string {
				pk := r.estimateCardinality(ctx)
				r.updateDecisions()
				return pk
			},
			wantBucket: "0_2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testTel := componenttest.NewTelemetry()
			t.Cleanup(func() {
				require.NoError(t, testTel.Shutdown(context.Background()))
			})

			settings := testTel.NewTelemetrySettings()
			settings.Logger = zaptest.NewLogger(t)
			router, err := newRouter(
				&cfg,
				settings,
				func(...pipeline.ID) (consumer.Metrics, error) {
					return consumertest.NewNop(), nil
				},
				pipeline.SignalMetrics,
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, router.Shutdown(context.Background()))
			})

			var dps []metricdata.DataPoint[int64]
			for _, ctx := range tt.ctxs {
				var pk string
				if tt.setup != nil {
					pk = tt.setup(router, ctx)
				}

				router.Process(ctx)

				dps = append(dps, metricdata.DataPoint[int64]{
					Value: 1,
					Attributes: attribute.NewSet(
						attribute.String("cardinality_bucket", tt.wantBucket),
						attribute.String("partition_key", pk),
						attribute.String("signal", pipeline.SignalMetrics.String()),
					),
				})
			}

			metadatatest.AssertEqualDynamicroutingRouted(
				t,
				testTel,
				dps,
				metricdatatest.IgnoreTimestamp(),
			)
		})
	}
}
