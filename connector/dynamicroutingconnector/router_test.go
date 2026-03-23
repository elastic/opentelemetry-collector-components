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

func TestDecisionTTLExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			interval = 10 * time.Millisecond
			ttl      = 40 * time.Millisecond
		)
		sinkDefault, sinkBucket, conn := newTestMetricsConnector(t, interval, ttl)
		require.NoError(t, conn.Start(context.Background(), nil))
		defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

		ctx := contextWithMetadata(map[string][]string{
			"x-tenant-id":     {"tenant-1"},
			"x-forwarded-for": {"10.2.4.2"},
		})
		md := newTestMetrics("1", "1", "1", "1")

		// No decision yet: routes to default
		require.NoError(t, conn.ConsumeMetrics(ctx, md))
		require.Len(t, sinkDefault.AllMetrics(), 1)
		require.Empty(t, sinkBucket.AllMetrics())

		// Advance past recording interval to form decision
		time.Sleep(interval)
		synctest.Wait()

		// Decision formed: routes to bucket
		require.NoError(t, conn.ConsumeMetrics(ctx, md))
		require.Len(t, sinkDefault.AllMetrics(), 1)
		require.Len(t, sinkBucket.AllMetrics(), 1)

		// Advance past TTL without sending data
		time.Sleep(ttl + interval)
		synctest.Wait()

		// Decision expired: routes to default again
		require.NoError(t, conn.ConsumeMetrics(ctx, md))
		require.Len(t, sinkDefault.AllMetrics(), 2)
		require.Len(t, sinkBucket.AllMetrics(), 1)
	})
}

func TestDecisionRefreshOnNearExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			interval = 10 * time.Millisecond
			ttl      = 40 * time.Millisecond
		)
		sinkDefault, sinkBucket, conn := newTestMetricsConnector(t, interval, ttl)
		require.NoError(t, conn.Start(context.Background(), nil))
		defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

		ctx := contextWithMetadata(map[string][]string{
			"x-tenant-id":     {"tenant-1"},
			"x-forwarded-for": {"10.2.4.2"},
		})
		md := newTestMetrics("1", "1", "1", "1")

		// Send initial data and form decision
		require.NoError(t, conn.ConsumeMetrics(ctx, md))
		time.Sleep(interval) // decision forms, expires at t=interval+ttl=50ms
		synctest.Wait()

		// Verify bucket routing
		require.NoError(t, conn.ConsumeMetrics(ctx, md))
		require.Len(t, sinkBucket.AllMetrics(), 1)

		// Advance to near-expiry window:
		// Decision expires at t=50ms.
		// Near-expiry when time.Until(expiresAt) <= interval, i.e. t >= 40ms.
		time.Sleep(3 * interval) // t=40ms
		synctest.Wait()

		// Send data in near-expiry window (re-records cardinality)
		require.NoError(t, conn.ConsumeMetrics(ctx, md))
		require.Len(t, sinkBucket.AllMetrics(), 2)

		// updateDecisions refreshes the decision with a new TTL
		time.Sleep(interval) // t=50ms
		synctest.Wait()

		// Advance past original expiry
		time.Sleep(interval) // t=60ms
		synctest.Wait()

		// Decision was refreshed: still routes to bucket
		require.NoError(t, conn.ConsumeMetrics(ctx, md))
		require.Len(t, sinkBucket.AllMetrics(), 3)
		require.Len(t, sinkDefault.AllMetrics(), 1)
	})
}

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

	tenant1Ctx := contextWithMetadata(map[string][]string{
		"x-tenant-id":     {"tenant-1"},
		"x-forwarded-for": {"10.2.4.2"},
	})
	tenant2Ctx := contextWithMetadata(map[string][]string{
		"x-tenant-id":     {"tenant-2"},
		"x-forwarded-for": {"10.2.4.3"},
	})

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
				connSet.Logger = zaptest.NewLogger(t)
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

// Helpers

func newTestMetricsConnector(t *testing.T, recordingInterval, ttl time.Duration) (
	*consumertest.MetricsSink, *consumertest.MetricsSink, connector.Metrics,
) {
	t.Helper()
	pipelineDefault := pipeline.NewIDWithName(pipeline.SignalMetrics, "default")
	pipelineBucket := pipeline.NewIDWithName(pipeline.SignalMetrics, "bucket_0_2")
	pipelineInf := pipeline.NewIDWithName(pipeline.SignalMetrics, "bucket_inf")

	var sinkDefault, sinkBucket, sinkInf consumertest.MetricsSink
	routerAndConsumer := connector.NewMetricsRouter(map[pipeline.ID]consumer.Metrics{
		pipelineDefault: &sinkDefault,
		pipelineBucket:  &sinkBucket,
		pipelineInf:     &sinkInf,
	})

	cfg := &Config{
		RoutingKeys: RoutingKeys{
			PartitionBy: []string{"x-tenant-id"},
			MeasureBy:   []string{"x-forwarded-for"},
		},
		DefaultPipelines: []pipeline.ID{pipelineDefault},
		RoutingPipelines: []RoutingPipeline{
			{Pipelines: []pipeline.ID{pipelineBucket}, MaxCardinality: 2},
			{Pipelines: []pipeline.ID{pipelineInf}, MaxCardinality: math.Inf(1)},
		},
		RecordingInterval: recordingInterval,
		TTL:               ttl,
	}

	connSet := connectortest.NewNopSettings(metadata.Type)
	connSet.Logger = zaptest.NewLogger(t)
	conn, err := NewFactory().CreateMetricsToMetrics(
		context.Background(),
		connSet,
		cfg,
		routerAndConsumer.(consumer.Metrics),
	)
	require.NoError(t, err)

	return &sinkDefault, &sinkBucket, conn
}

func contextWithMetadata(md map[string][]string) context.Context {
	return client.NewContext(
		context.Background(),
		client.Info{Metadata: client.NewMetadata(md)},
	)
}
