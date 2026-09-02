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

package dynamicbatchprocessor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/elastic/opentelemetry-collector-components/processor/dynamicbatchprocessor/internal/metadata"
)

func testConfig(idleTimeout time.Duration, cardinalityLimit int) *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.MetadataKeys = []string{"x-tenant"}
	cfg.IdleTimeout = idleTimeout
	cfg.MetadataCardinalityLimit = cardinalityLimit
	return cfg
}

func ctxWithTenant(tenant string) context.Context {
	return client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{"x-tenant": {tenant}}),
	})
}

func oneSpanTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("test")
	return td
}

func newTestTracesProcessor(t *testing.T, cfg *Config, sink *consumertest.TracesSink) *tracesProcessor {
	t.Helper()
	factory := NewFactory()
	proc, err := factory.CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	err = proc.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	t.Cleanup(func() { _ = proc.Shutdown(context.Background()) })
	return proc.(*tracesProcessor)
}

func TestSingleMetadataKey(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(5*time.Minute, 1000)
	proc := newTestTracesProcessor(t, cfg, sink)

	ctx := ctxWithTenant("acme")
	require.NoError(t, proc.ConsumeTraces(ctx, oneSpanTraces()))
	require.NoError(t, proc.ConsumeTraces(ctx, oneSpanTraces()))
	require.NoError(t, proc.ConsumeTraces(ctx, oneSpanTraces()))

	assert.Equal(t, 1, proc.shardCount())
	require.NoError(t, proc.Shutdown(context.Background()))
	assert.Equal(t, 3, sink.SpanCount())
}

func TestMultipleMetadataValues(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(5*time.Minute, 1000)
	proc := newTestTracesProcessor(t, cfg, sink)

	for _, tenant := range []string{"alpha", "beta", "gamma"} {
		require.NoError(t, proc.ConsumeTraces(ctxWithTenant(tenant), oneSpanTraces()))
	}
	assert.Equal(t, 3, proc.shardCount())

	require.NoError(t, proc.Shutdown(context.Background()))
	assert.Equal(t, 3, sink.SpanCount())
}

func TestCardinalityLimitExceeded(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(5*time.Minute, 2)
	proc := newTestTracesProcessor(t, cfg, sink)

	require.NoError(t, proc.ConsumeTraces(ctxWithTenant("a"), oneSpanTraces()))
	require.NoError(t, proc.ConsumeTraces(ctxWithTenant("b"), oneSpanTraces()))
	err := proc.ConsumeTraces(ctxWithTenant("c"), oneSpanTraces())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many metadata combinations")
}

func TestIdleShardGC(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(50*time.Millisecond, 1000)
	proc := newTestTracesProcessor(t, cfg, sink)

	require.NoError(t, proc.ConsumeTraces(ctxWithTenant("tenant1"), oneSpanTraces()))
	assert.Equal(t, 1, proc.shardCount())

	// Wait long enough for GC to run (idleTimeout/2 tick + idleTimeout age).
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 0, proc.shardCount())
}

func TestShutdownDrainsShards(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(5*time.Minute, 1000)
	proc := newTestTracesProcessor(t, cfg, sink)

	for i := 0; i < 10; i++ {
		require.NoError(t, proc.ConsumeTraces(ctxWithTenant("acme"), oneSpanTraces()))
	}
	require.NoError(t, proc.Shutdown(context.Background()))
	assert.Equal(t, 10, sink.SpanCount())
}

func TestMissingMetadataKey(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(5*time.Minute, 1000)
	proc := newTestTracesProcessor(t, cfg, sink)

	// No x-tenant header — should land on a shard with empty key, not an error.
	require.NoError(t, proc.ConsumeTraces(context.Background(), oneSpanTraces()))
	assert.Equal(t, 1, proc.shardCount())

	require.NoError(t, proc.Shutdown(context.Background()))
	assert.Equal(t, 1, sink.SpanCount())
}

func TestShardRecreatedAfterGC(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(50*time.Millisecond, 1000)
	proc := newTestTracesProcessor(t, cfg, sink)

	require.NoError(t, proc.ConsumeTraces(ctxWithTenant("acme"), oneSpanTraces()))
	assert.Equal(t, 1, proc.shardCount())

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 0, proc.shardCount())

	require.NoError(t, proc.ConsumeTraces(ctxWithTenant("acme"), oneSpanTraces()))
	assert.Equal(t, 1, proc.shardCount())

	require.NoError(t, proc.Shutdown(context.Background()))
	assert.Equal(t, 2, sink.SpanCount())
}

func TestMultipleMetadataKeyConfig(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := createDefaultConfig().(*Config)
	cfg.MetadataKeys = []string{"x-tenant", "x-region"}
	cfg.IdleTimeout = 5 * time.Minute
	cfg.MetadataCardinalityLimit = 1000
	proc := newTestTracesProcessor(t, cfg, sink)

	ctx1 := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{"x-tenant": {"acme"}, "x-region": {"us-east"}})})
	ctx2 := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{"x-tenant": {"acme"}, "x-region": {"eu-west"}})})
	ctx3 := client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{"x-tenant": {"acme"}, "x-region": {"us-east"}})})

	require.NoError(t, proc.ConsumeTraces(ctx1, oneSpanTraces()))
	require.NoError(t, proc.ConsumeTraces(ctx2, oneSpanTraces()))
	require.NoError(t, proc.ConsumeTraces(ctx3, oneSpanTraces()))
	assert.Equal(t, 2, proc.shardCount())

	require.NoError(t, proc.Shutdown(context.Background()))
	assert.Equal(t, 3, sink.SpanCount())
}

func TestUnlimitedCardinality(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(5*time.Minute, 0)
	proc := newTestTracesProcessor(t, cfg, sink)

	const n = 50
	for i := 0; i < n; i++ {
		require.NoError(t, proc.ConsumeTraces(ctxWithTenant(fmt.Sprintf("tenant-%d", i)), oneSpanTraces()))
	}
	assert.Equal(t, n, proc.shardCount())
	require.NoError(t, proc.Shutdown(context.Background()))
	assert.Equal(t, n, sink.SpanCount())
}

func TestConcurrentConsumeAndGC(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(20*time.Millisecond, 1000)
	proc := newTestTracesProcessor(t, cfg, sink)

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := ctxWithTenant(fmt.Sprintf("tenant-%d", id%3))
			for s := 0; s < 50; s++ {
				assert.NoError(t, proc.ConsumeTraces(ctx, oneSpanTraces()))
			}
		}(g)
	}
	wg.Wait()
	require.NoError(t, proc.Shutdown(context.Background()))
}

func TestBuildMetadataKey(t *testing.T) {
	mk := func(md map[string][]string) client.Info {
		return client.Info{Metadata: client.NewMetadata(md)}
	}
	assert.Equal(t, "x-tenant=acme",
		buildMetadataKey(mk(map[string][]string{"x-tenant": {"acme"}}), []string{"x-tenant"}))
	assert.Equal(t, "x-tenant=acme&x-region=us-east",
		buildMetadataKey(mk(map[string][]string{"x-tenant": {"acme"}, "x-region": {"us-east"}}), []string{"x-tenant", "x-region"}))
	assert.Equal(t, "x-tenant=",
		buildMetadataKey(mk(nil), []string{"x-tenant"}))
	assert.Equal(t, "x-tenant=a,b",
		buildMetadataKey(mk(map[string][]string{"x-tenant": {"a", "b"}}), []string{"x-tenant"}))
}

func TestMetricsProcessor(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	cfg := createDefaultConfig().(*Config)
	cfg.MetadataKeys = []string{"x-tenant"}
	cfg.IdleTimeout = 5 * time.Minute
	cfg.MetadataCardinalityLimit = 1000

	proc, err := NewFactory().CreateMetrics(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { _ = proc.Shutdown(context.Background()) })

	md := pmetric.NewMetrics()
	md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty()

	require.NoError(t, proc.ConsumeMetrics(ctxWithTenant("acme"), md))
	require.NoError(t, proc.Shutdown(context.Background()))
	assert.Equal(t, 1, sink.DataPointCount())
}

func TestLogsProcessor(t *testing.T) {
	sink := new(consumertest.LogsSink)
	cfg := createDefaultConfig().(*Config)
	cfg.MetadataKeys = []string{"x-tenant"}
	cfg.IdleTimeout = 5 * time.Minute
	cfg.MetadataCardinalityLimit = 1000

	proc, err := NewFactory().CreateLogs(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { _ = proc.Shutdown(context.Background()) })

	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	require.NoError(t, proc.ConsumeLogs(ctxWithTenant("acme"), ld))
	require.NoError(t, proc.Shutdown(context.Background()))
	assert.Equal(t, 1, sink.LogRecordCount())
}

// benchConfig returns a config suitable for benchmarks: WaitForResult=true prevents
// queue overflow without needing a huge QueueSize, making ConsumeTraces synchronous.
func benchConfig(cardinalityLimit int) *Config {
	cfg := testConfig(5*time.Minute, cardinalityLimit)
	cfg.WaitForResult = true
	return cfg
}

// --- Benchmarks ---

func BenchmarkConsumeTracesHotShard(b *testing.B) {
	sink := new(consumertest.TracesSink)
	cfg := benchConfig(1000)
	proc, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	if err != nil {
		b.Fatal(err)
	}
	if err := proc.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = proc.Shutdown(context.Background()) })

	ctx := ctxWithTenant("acme")
	td := oneSpanTraces()
	// warm shard
	if err := proc.ConsumeTraces(ctx, td); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := proc.ConsumeTraces(ctx, td); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConsumeTracesNShards(b *testing.B) {
	for _, n := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			sink := new(consumertest.TracesSink)
			cfg := benchConfig(n + 10)
			proc, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
			if err != nil {
				b.Fatal(err)
			}
			if err := proc.Start(context.Background(), componenttest.NewNopHost()); err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = proc.Shutdown(context.Background()) })

			ctxs := make([]context.Context, n)
			td := oneSpanTraces()
			for i := 0; i < n; i++ {
				ctxs[i] = ctxWithTenant(fmt.Sprintf("tenant-%d", i))
				if err := proc.ConsumeTraces(ctxs[i], td); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := proc.ConsumeTraces(ctxs[i%n], td); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkConsumeTracesConcurrent(b *testing.B) {
	sink := new(consumertest.TracesSink)
	cfg := benchConfig(1000)
	proc, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	if err != nil {
		b.Fatal(err)
	}
	if err := proc.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = proc.Shutdown(context.Background()) })

	ctx := ctxWithTenant("acme")
	td := oneSpanTraces()
	if err := proc.ConsumeTraces(ctx, td); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := proc.ConsumeTraces(ctx, td); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBuildMetadataKey(b *testing.B) {
	info := client.Info{Metadata: client.NewMetadata(map[string][]string{
		"x-tenant": {"acme"},
		"x-region": {"us-east"},
	})}
	keys := []string{"x-tenant", "x-region"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buildMetadataKey(info, keys)
	}
}
