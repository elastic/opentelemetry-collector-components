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

package elasticapmconnector // import "github.com/elastic/opentelemetry-collector-components/connector/elasticapmconnector"

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/elastic/opentelemetry-collector-components/connector/elasticapmconnector/internal/metadata"
)

var ecsStatement = `set(attributes["data_stream.dataset"], Concat(["apm.", attributes["data_stream.dataset"]], "")) where otelcol.client.metadata["x-elastic-mapping-mode"][0] == "ecs"`

func BenchmarkConnectorOTELTraffic(b *testing.B) {
	// Benchmarks the connector with non-ECS (OTEL) traffic to measure
	// the overhead of the ECS prefix OTTL statement's where clause
	// evaluation on the hot path that should short-circuit.
	for _, bc := range []struct {
		name       string
		statements []string
	}{
		{name: "no_statements", statements: nil},
		{name: "with_ecs_statement", statements: []string{ecsStatement}},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.Run("traces/transaction_metrics", func(b *testing.B) {
				input, err := golden.ReadTraces(filepath.Join(
					"testdata", "traces", "transaction_metrics", "input.yaml",
				))
				require.NoError(b, err)
				benchmarkTracesConnector(b, bc.statements, input)
			})
			b.Run("traces/span_metrics", func(b *testing.B) {
				input, err := golden.ReadTraces(filepath.Join(
					"testdata", "traces", "span_metrics", "input.yaml",
				))
				require.NoError(b, err)
				benchmarkTracesConnector(b, bc.statements, input)
			})
			b.Run("logs/service_summary", func(b *testing.B) {
				input, err := golden.ReadLogs(filepath.Join(
					"testdata", "logs", "service_summary", "input.yaml",
				))
				require.NoError(b, err)
				benchmarkLogsConnector(b, bc.statements, input)
			})
			b.Run("metrics/service_summary", func(b *testing.B) {
				input, err := golden.ReadMetrics(filepath.Join(
					"testdata", "metrics", "service_summary", "input.yaml",
				))
				require.NoError(b, err)
				benchmarkMetricsConnector(b, bc.statements, input)
			})
		})
	}
}

// otelClientCtx returns a context with OTEL (non-ECS) client metadata,
// simulating traffic that should not trigger the ECS prefix statement.
func otelClientCtx() context.Context {
	return client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"x-elastic-mapping-mode": {"otel"},
		}),
	})
}

func newBenchConfig(statements []string) *Config {
	return &Config{
		Aggregation: &AggregationConfig{
			MetadataKeys: []string{"x-elastic-mapping-mode"},
			Intervals:    []time.Duration{time.Minute},
			Statements:   statements,
		},
	}
}

func benchmarkTracesConnector(b *testing.B, statements []string, input ptrace.Traces) {
	b.Helper()
	next := &consumertest.MetricsSink{}
	factory := NewFactory()
	settings := connectortest.NewNopSettings(metadata.Type)

	c, err := factory.CreateTracesToMetrics(context.Background(), settings, newBenchConfig(statements), next)
	require.NoError(b, err)
	require.NoError(b, c.Start(context.Background(), componenttest.NewNopHost()))

	ctx := otelClientCtx()
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		if err := c.ConsumeTraces(ctx, input); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	require.NoError(b, c.Shutdown(context.Background()))
}

func benchmarkLogsConnector(b *testing.B, statements []string, input plog.Logs) {
	b.Helper()
	next := &consumertest.MetricsSink{}
	factory := NewFactory()
	settings := connectortest.NewNopSettings(metadata.Type)

	c, err := factory.CreateLogsToMetrics(context.Background(), settings, newBenchConfig(statements), next)
	require.NoError(b, err)
	require.NoError(b, c.Start(context.Background(), componenttest.NewNopHost()))

	ctx := otelClientCtx()
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		if err := c.ConsumeLogs(ctx, input); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	require.NoError(b, c.Shutdown(context.Background()))
}

func benchmarkMetricsConnector(b *testing.B, statements []string, input pmetric.Metrics) {
	b.Helper()
	next := &consumertest.MetricsSink{}
	factory := NewFactory()
	settings := connectortest.NewNopSettings(metadata.Type)

	c, err := factory.CreateMetricsToMetrics(context.Background(), settings, newBenchConfig(statements), next)
	require.NoError(b, err)
	require.NoError(b, c.Start(context.Background(), componenttest.NewNopHost()))

	ctx := otelClientCtx()
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		if err := c.ConsumeMetrics(ctx, input); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	require.NoError(b, c.Shutdown(context.Background()))
}
