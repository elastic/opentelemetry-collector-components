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
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	"go.uber.org/zap/zaptest"

	"github.com/elastic/opentelemetry-collector-components/connector/dynamicroutingconnector/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/connector/dynamicroutingconnector/internal/metadatatest"
)

func TestDynamicroutingRoutedTelemetry(t *testing.T) {
	pipelineDefault := pipeline.NewIDWithName(pipeline.SignalMetrics, "default")
	pipelineBucket := pipeline.NewIDWithName(pipeline.SignalMetrics, "bucket_0_2")
	pipelineBucket2 := pipeline.NewIDWithName(pipeline.SignalMetrics, "bucket_2_5")
	pipelineBucketInf := pipeline.NewIDWithName(pipeline.SignalMetrics, "bucket_5_inf")

	cfg := Config{
		RoutingKeys: RoutingKeys{
			PartitionBy: []string{"x-tenant-id"},
			MeasureBy:   []string{"x-forwarded-for"},
		},
		DefaultPipelines: []pipeline.ID{pipelineDefault},
		RoutingPipelines: []RoutingPipeline{
			{Pipelines: []pipeline.ID{pipelineBucket}, MaxCardinality: 2},
			{Pipelines: []pipeline.ID{pipelineBucket2}, MaxCardinality: 5},
			{Pipelines: []pipeline.ID{pipelineBucketInf}, MaxCardinality: math.Inf(1)},
		},
		RecordingInterval: time.Second,
		TTL:               5 * time.Second,
	}

	tenant1Ctx := client.NewContext(
		context.Background(),
		client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":     {"tenant-1"},
				"x-forwarded-for": {"10.2.4.2"},
			}),
		},
	)
	tenant2Ctx := client.NewContext(
		context.Background(),
		client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":     {"tenant-2"},
				"x-forwarded-for": {"10.2.4.3"},
			}),
		},
	)

	tests := []struct {
		name      string
		setupCtxs []context.Context // sent before time advancement to form decisions
		testCtxs  []context.Context // sent after decisions formed
		wantDPs   []metricdata.DataPoint[int64]
	}{
		{
			name:     "default_bucket",
			testCtxs: []context.Context{context.Background()},
			wantDPs: []metricdata.DataPoint[int64]{
				{
					Value: 1,
					Attributes: attribute.NewSet(
						attribute.String("cardinality_bucket", defaultCardinalityBucket),
						attribute.String("partition_key", ""),
					),
				},
			},
		},
		{
			name:      "cardinality_bucket",
			setupCtxs: []context.Context{tenant1Ctx},
			testCtxs:  []context.Context{tenant1Ctx},
			wantDPs: []metricdata.DataPoint[int64]{
				{
					Value: 1,
					Attributes: attribute.NewSet(
						attribute.String("cardinality_bucket", defaultCardinalityBucket),
						attribute.String("partition_key", "tenant-1:;"),
					),
				},
				{
					Value: 1,
					Attributes: attribute.NewSet(
						attribute.String("cardinality_bucket", "0_2"),
						attribute.String("partition_key", "tenant-1:;"),
					),
				},
			},
		},
		{
			name:      "multiple_tenants",
			setupCtxs: []context.Context{tenant1Ctx, tenant2Ctx},
			testCtxs:  []context.Context{tenant1Ctx, tenant2Ctx},
			wantDPs: []metricdata.DataPoint[int64]{
				{
					Value: 1,
					Attributes: attribute.NewSet(
						attribute.String("cardinality_bucket", defaultCardinalityBucket),
						attribute.String("partition_key", "tenant-1:;"),
					),
				},
				{
					Value: 1,
					Attributes: attribute.NewSet(
						attribute.String("cardinality_bucket", defaultCardinalityBucket),
						attribute.String("partition_key", "tenant-2:;"),
					),
				},
				{
					Value: 1,
					Attributes: attribute.NewSet(
						attribute.String("cardinality_bucket", "0_2"),
						attribute.String("partition_key", "tenant-1:;"),
					),
				},
				{
					Value: 1,
					Attributes: attribute.NewSet(
						attribute.String("cardinality_bucket", "0_2"),
						attribute.String("partition_key", "tenant-2:;"),
					),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				testTel := componenttest.NewTelemetry()
				defer func() { require.NoError(t, testTel.Shutdown(context.Background())) }()

				routerAndConsumer := connector.NewMetricsRouter(map[pipeline.ID]consumer.Metrics{
					pipelineDefault:   consumertest.NewNop(),
					pipelineBucket:    consumertest.NewNop(),
					pipelineBucket2:   consumertest.NewNop(),
					pipelineBucketInf: consumertest.NewNop(),
				})

				connSet := connectortest.NewNopSettings(metadata.Type)
				connSet.TelemetrySettings = testTel.NewTelemetrySettings()
				connSet.TelemetrySettings.Logger = zaptest.NewLogger(t)
				conn, err := NewFactory().CreateMetricsToMetrics(
					context.Background(),
					connSet,
					&cfg,
					routerAndConsumer.(consumer.Metrics),
				)
				require.NoError(t, err)
				require.NoError(t, conn.Start(context.Background(), nil))
				defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

				md := newTestMetrics("1", "1", "1", "1")
				for _, ctx := range tt.setupCtxs {
					require.NoError(t, conn.ConsumeMetrics(ctx, md))
				}
				time.Sleep(cfg.RecordingInterval)
				synctest.Wait()

				for _, ctx := range tt.testCtxs {
					require.NoError(t, conn.ConsumeMetrics(ctx, md))
				}

				metadatatest.AssertEqualDynamicroutingRouted(
					t,
					testTel,
					tt.wantDPs,
					metricdatatest.IgnoreTimestamp(),
				)
			})
		})
	}
}
