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
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap"
)

var benchRoutingKeySets = []struct {
	name        string
	partitionBy []string
	measureBy   []string
	metadata    map[string][]string
}{
	{
		name:        "1pk_1mk",
		partitionBy: []string{"x-tenant-id"},
		measureBy:   []string{"x-forwarded-for"},
		metadata: map[string][]string{
			"x-tenant-id":     {"tenant-1"},
			"x-forwarded-for": {"10.2.4.2"},
		},
	},
	{
		name:        "3pk_3mk",
		partitionBy: []string{"x-tenant-id", "x-tenant-type", "x-region"},
		measureBy:   []string{"x-forwarded-for", "user-agent", "content-type"},
		metadata: map[string][]string{
			"x-tenant-id":     {"tenant-1"},
			"x-tenant-type":   {"premium"},
			"x-region":        {"us-east-1"},
			"x-forwarded-for": {"10.2.4.2"},
			"user-agent":      {"otel-0.135.0"},
			"content-type":    {"application/grpc"},
		},
	},
}

func BenchmarkRouter(b *testing.B) {
	for _, keys := range benchRoutingKeySets {
		b.Run(keys.name, func(b *testing.B) {
			ctx := client.NewContext(
				context.Background(),
				client.Info{Metadata: client.NewMetadata(keys.metadata)},
			)
			b.Run("traces", func(b *testing.B) { benchTraces(b, ctx, keys.partitionBy, keys.measureBy) })
			b.Run("metrics", func(b *testing.B) { benchMetrics(b, ctx, keys.partitionBy, keys.measureBy) })
			b.Run("logs", func(b *testing.B) { benchLogs(b, ctx, keys.partitionBy, keys.measureBy) })
		})
	}
}

func makeBenchConfig(signal pipeline.Signal, partitionBy, measureBy []string) Config {
	return Config{
		RoutingKeys: RoutingKeys{
			PartitionBy: partitionBy,
			MeasureBy:   measureBy,
		},
		DefaultPipelines: []pipeline.ID{
			pipeline.NewIDWithName(signal, "default"),
		},
		RoutingPipelines: []RoutingPipeline{
			{Pipelines: []pipeline.ID{pipeline.NewIDWithName(signal, "threshold_0_2")}, MaxCardinality: 2},
			{Pipelines: []pipeline.ID{pipeline.NewIDWithName(signal, "threshold_2_5")}, MaxCardinality: 5},
			{Pipelines: []pipeline.ID{pipeline.NewIDWithName(signal, "threshold_5_inf")}, MaxCardinality: math.Inf(1)},
		},
		RecordingInterval: time.Hour,
		TTL:               6 * time.Hour,
	}
}

func pipelineIDsFromConfig(cfg *Config) []pipeline.ID {
	ids := append([]pipeline.ID(nil), cfg.DefaultPipelines...)
	for _, rp := range cfg.RoutingPipelines {
		ids = append(ids, rp.Pipelines...)
	}
	return ids
}

func benchTraces(b *testing.B, ctx context.Context, partitionBy, measureBy []string) {
	cfg := makeBenchConfig(pipeline.SignalTraces, partitionBy, measureBy)
	consumerMap := make(map[pipeline.ID]consumer.Traces)
	for _, id := range pipelineIDsFromConfig(&cfg) {
		consumerMap[id] = consumertest.NewNop()
	}
	routerAndConsumer := connector.NewTracesRouter(consumerMap)

	settings := connectortest.NewNopSettings(metadata.Type)
	settings.Logger = zap.NewNop()
	conn, err := NewFactory().CreateTracesToTraces(
		context.Background(),
		settings,
		&cfg,
		routerAndConsumer.(consumer.Traces),
	)
	if err != nil {
		b.Fatal(err)
	}

	// Populate HLL and run updateDecisions so decision map has entry for tenant-1
	tConn := conn.(*tracesConnector)
	_ = conn.ConsumeTraces(ctx, newTestTraces("1", "1", "1", "1"))
	tConn.router.updateDecisions()

	td := newTestTraces("2", "2", "2", "2")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = conn.ConsumeTraces(ctx, td)
	}
}

func benchMetrics(b *testing.B, ctx context.Context, partitionBy, measureBy []string) {
	cfg := makeBenchConfig(pipeline.SignalMetrics, partitionBy, measureBy)
	consumerMap := make(map[pipeline.ID]consumer.Metrics)
	for _, id := range pipelineIDsFromConfig(&cfg) {
		consumerMap[id] = consumertest.NewNop()
	}
	routerAndConsumer := connector.NewMetricsRouter(consumerMap)

	settings := connectortest.NewNopSettings(metadata.Type)
	settings.Logger = zap.NewNop()
	conn, err := NewFactory().CreateMetricsToMetrics(
		context.Background(),
		settings,
		&cfg,
		routerAndConsumer.(consumer.Metrics),
	)
	if err != nil {
		b.Fatal(err)
	}

	mConn := conn.(*metricsConnector)
	_ = conn.ConsumeMetrics(ctx, newTestMetrics("1", "1", "1", "1"))
	mConn.router.updateDecisions()

	md := newTestMetrics("2", "2", "2", "2")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = conn.ConsumeMetrics(ctx, md)
	}
}

func benchLogs(b *testing.B, ctx context.Context, partitionBy, measureBy []string) {
	cfg := makeBenchConfig(pipeline.SignalLogs, partitionBy, measureBy)
	consumerMap := make(map[pipeline.ID]consumer.Logs)
	for _, id := range pipelineIDsFromConfig(&cfg) {
		consumerMap[id] = consumertest.NewNop()
	}
	routerAndConsumer := connector.NewLogsRouter(consumerMap)

	settings := connectortest.NewNopSettings(metadata.Type)
	settings.Logger = zap.NewNop()
	conn, err := NewFactory().CreateLogsToLogs(
		context.Background(),
		settings,
		&cfg,
		routerAndConsumer.(consumer.Logs),
	)
	if err != nil {
		b.Fatal(err)
	}

	lConn := conn.(*logsConnector)
	_ = conn.ConsumeLogs(ctx, newTestLogs("1", "1", "1"))
	lConn.router.updateDecisions()

	ld := newTestLogs("2", "2", "2")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = conn.ConsumeLogs(ctx, ld)
	}
}
