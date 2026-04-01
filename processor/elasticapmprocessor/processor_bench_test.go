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

package elasticapmprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/elastictraceprocessor"

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// cloneTraces returns a deep copy of td so benchmark iterations start from unenriched spans.
func cloneTraces(td ptrace.Traces) ptrace.Traces {
	c := ptrace.NewTraces()
	td.CopyTo(c)
	return c
}

// makeHTTPTracesForBench creates trace data representing HTTP spans
// (a common MOTel workload): numResources services × numScopes × numSpans spans.
func makeHTTPTracesForBench(numResources, numScopes, numSpans int) ptrace.Traces {
	td := ptrace.NewTraces()
	for i := 0; i < numResources; i++ {
		rs := td.ResourceSpans().AppendEmpty()
		res := rs.Resource()
		res.Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", i))
		res.Attributes().PutStr("deployment.environment", "production")
		res.Attributes().PutStr("telemetry.sdk.language", "python")
		res.Attributes().PutStr("agent.name", "go")
		for j := 0; j < numScopes; j++ {
			ss := rs.ScopeSpans().AppendEmpty()
			ss.Scope().SetName(fmt.Sprintf("scope-%d", j))
			for k := 0; k < numSpans; k++ {
				span := ss.Spans().AppendEmpty()
				span.SetName(fmt.Sprintf("/api/v%d/resource", k))
				span.SetKind(ptrace.SpanKindServer)
				span.SetStartTimestamp(pcommon.Timestamp(1000000000))
				span.SetEndTimestamp(pcommon.Timestamp(1010000000))
				if k > 0 {
					// Non-root spans have a parent
					span.SetParentSpanID(pcommon.SpanID([8]byte{1, 0, 0, 0, 0, 0, 0, 0}))
				}
				// Typical HTTP span attributes
				span.Attributes().PutStr(string(semconv.HTTPRequestMethodKey), "GET")
				span.Attributes().PutInt(string(semconv.HTTPResponseStatusCodeKey), 200)
				span.Attributes().PutStr(string(semconv.URLFullKey), fmt.Sprintf("http://backend/api/%d", k))
				span.Attributes().PutStr(string(semconv.ServerAddressKey), "backend")
				span.Attributes().PutInt(string(semconv.ServerPortKey), 8080)
			}
		}
	}
	return td
}

// BenchmarkProcessorConsumeTraces_WithPooling benchmarks the ECS path with pdata.useProtoPooling
// enabled — same finding as in elasticapmconnector: pooling eliminates per-attribute allocations.
func BenchmarkProcessorConsumeTraces_WithPooling(b *testing.B) {
	require := func(err error) {
		if err != nil {
			b.Fatal(err)
		}
	}
	const poolingGateID = "pdata.useProtoPooling"
	require(featuregate.GlobalRegistry().Set(poolingGateID, true))
	b.Cleanup(func() { _ = featuregate.GlobalRegistry().Set(poolingGateID, false) })

	cfg := NewDefaultConfig().(*Config)
	cfg.HostIPEnabled = false
	next := &consumertest.TracesSink{}
	p := NewTraceProcessor(cfg, next, zap.NewNop())
	td := makeHTTPTracesForBench(10, 5, 100)
	ecsCtx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		fresh := cloneTraces(td)
		b.StartTimer()
		if err := p.ConsumeTraces(ecsCtx, fresh); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*5000), "ns/span")
}

// BenchmarkProcessorConsumeTraces_OTel benchmarks ConsumeTraces in non-ECS (OTLP) mode.
// This is the default MOTel path for OTLP-sourced traces.
func BenchmarkProcessorConsumeTraces_OTel(b *testing.B) {
	cases := []struct {
		resources, scopes, spans int
	}{
		{1, 1, 100},
		{10, 5, 100},
	}
	for _, tc := range cases {
		name := fmt.Sprintf("r%d_s%d_sp%d", tc.resources, tc.scopes, tc.spans)
		b.Run(name, func(b *testing.B) {
			cfg := NewDefaultConfig().(*Config)
			next := &consumertest.TracesSink{}
			p := NewTraceProcessor(cfg, next, zap.NewNop())
			td := makeHTTPTracesForBench(tc.resources, tc.scopes, tc.spans)
			ctx := context.Background()
			totalSpans := tc.resources * tc.scopes * tc.spans
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				fresh := cloneTraces(td)
				b.StartTimer()
				if err := p.ConsumeTraces(ctx, fresh); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*totalSpans), "ns/span")
		})
	}
}

// BenchmarkProcessorConsumeTraces_ECS benchmarks ConsumeTraces in ECS mode.
// This is the MOTel path for APM intake receiver traces (the high-volume path).
func BenchmarkProcessorConsumeTraces_ECS(b *testing.B) {
	cases := []struct {
		resources, scopes, spans int
	}{
		{1, 1, 100},
		{10, 5, 100},
	}
	for _, tc := range cases {
		name := fmt.Sprintf("r%d_s%d_sp%d", tc.resources, tc.scopes, tc.spans)
		b.Run(name, func(b *testing.B) {
			cfg := NewDefaultConfig().(*Config)
			cfg.HostIPEnabled = false // disable host IP to avoid network lookup in bench
			next := &consumertest.TracesSink{}
			p := NewTraceProcessor(cfg, next, zap.NewNop())
			td := makeHTTPTracesForBench(tc.resources, tc.scopes, tc.spans)
			ecsCtx := client.NewContext(context.Background(), client.Info{
				Metadata: client.NewMetadata(map[string][]string{
					"x-elastic-mapping-mode": {"ecs"},
				}),
			})
			totalSpans := tc.resources * tc.scopes * tc.spans
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Clone outside timed section so enrichment starts from unenriched spans.
				// Without this, iteration 0 writes all elastic attrs and subsequent
				// iterations see hasPresetElastic=true (already-enriched path), which
				// benchmarks a blend of OTLP and intake-receiver paths rather than
				// pure OTLP throughput.
				b.StopTimer()
				fresh := cloneTraces(td)
				b.StartTimer()
				if err := p.ConsumeTraces(ecsCtx, fresh); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*totalSpans), "ns/span")
		})
	}
}
