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

	"github.com/axiomhq/hyperloglog"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pipeline"
)

func TestGetNextConsumerAndMaybeRecordByTTLWindow(t *testing.T) {
	r := newTestRouterForTTL(t, 100*time.Millisecond, time.Second)
	ctx := contextWithTenantAndMeasure("tenant-1", "10.2.4.2")
	pk := "tenant-1:;"
	choice := ct[consumer.Metrics]{cardinalityBucket: "0_2"}

	_, emptyBucket := r.getNextConsumerAndMaybeRecord(ctx, "")
	require.Equal(t, defaultCardinalityBucket, emptyBucket)
	requireMapDoesNotContainPartition(t, r, pk)

	r.decision.Set(pk, choice, time.Second)
	_, stableBucket := r.getNextConsumerAndMaybeRecord(ctx, pk)
	require.Equal(t, "0_2", stableBucket)
	requireMapDoesNotContainPartition(t, r, pk)

	r.decision.Set(pk, choice, 50*time.Millisecond)
	_, nearExpiryBucket := r.getNextConsumerAndMaybeRecord(ctx, pk)
	require.Equal(t, "0_2", nearExpiryBucket)
	requireMapContainsPartition(t, r, pk)
}

func TestProcessRecordsOnMissingOrNearExpiryDecision(t *testing.T) {
	r := newTestRouterForTTL(t, 100*time.Millisecond, time.Second)
	ctx := contextWithTenantAndMeasure("tenant-1", "10.2.4.2")
	pk := "tenant-1:;"
	choice := ct[consumer.Metrics]{cardinalityBucket: "0_2"}

	r.decision.Set(pk, choice, time.Second)
	r.Process(ctx)
	requireMapDoesNotContainPartition(t, r, pk)

	r.decision.Set(pk, choice, 50*time.Millisecond)
	r.Process(ctx)
	requireMapContainsPartition(t, r, pk)

	r.mu.Lock()
	r.m = make(map[string]*hyperloglog.Sketch)
	r.mu.Unlock()
	r.decision.DeleteAll()
	r.Process(ctx)
	requireMapContainsPartition(t, r, pk)
}

func TestDecisionPersistsUntilTTLExpiry(t *testing.T) {
	r := newTestRouterForTTL(t, 30*time.Millisecond, 120*time.Millisecond)
	ctx := contextWithTenantAndMeasure("tenant-1", "10.2.4.2")
	pk := r.partitionKey(ctx)
	r.recordCardinality(pk, r.hashSum(ctx))
	r.updateDecisions()

	_, initialBucket := r.getNextConsumerAndMaybeRecord(ctx, pk)
	require.Equal(t, "0_2", initialBucket)

	time.Sleep(40 * time.Millisecond)
	r.updateDecisions()
	_, midBucket := r.getNextConsumerAndMaybeRecord(ctx, pk)
	require.Equal(t, "0_2", midBucket)

	time.Sleep(100 * time.Millisecond)
	_, expiredBucket := r.getNextConsumerAndMaybeRecord(ctx, pk)
	require.Equal(t, defaultCardinalityBucket, expiredBucket)
}

func newTestRouterForTTL(t *testing.T, recordingInterval time.Duration, ttl time.Duration) *router[consumer.Metrics] {
	t.Helper()

	cfg := &Config{
		RoutingKeys: RoutingKeys{
			PartitionBy: []string{"x-tenant-id"},
			MeasureBy:   []string{"x-forwarded-for"},
		},
		DefaultPipelines: []pipeline.ID{
			pipeline.NewIDWithName(pipeline.SignalMetrics, "default"),
		},
		RoutingPipelines: []RoutingPipeline{
			{Pipelines: []pipeline.ID{pipeline.NewIDWithName(pipeline.SignalMetrics, "bucket_0_2")}, MaxCardinality: 2},
			{Pipelines: []pipeline.ID{pipeline.NewIDWithName(pipeline.SignalMetrics, "bucket_inf")}, MaxCardinality: math.Inf(1)},
		},
		RecordingInterval: recordingInterval,
		TTL:               ttl,
	}

	testTel := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, testTel.Shutdown(context.Background()))
	})

	router, err := newRouter(
		cfg,
		testTel.NewTelemetrySettings(),
		func(...pipeline.ID) (consumer.Metrics, error) {
			return consumertest.NewNop(), nil
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, router.Shutdown(context.Background()))
	})

	return router
}

func contextWithTenantAndMeasure(tenant string, measure string) context.Context {
	return client.NewContext(
		context.Background(),
		client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":     {tenant},
				"x-forwarded-for": {measure},
			}),
		},
	)
}

func requireMapContainsPartition(t *testing.T, r *router[consumer.Metrics], pk string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.m[pk]
	require.True(t, ok)
}

func requireMapDoesNotContainPartition(t *testing.T, r *router[consumer.Metrics], pk string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.m[pk]
	require.False(t, ok)
}
