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

package elasticapmconnector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestSetAgentName(t *testing.T) {
	for name, tc := range map[string]struct {
		attrs    map[string]string
		expected string
	}{
		"sdk name and language": {
			attrs: map[string]string{
				"telemetry.sdk.name":     "opentelemetry",
				"telemetry.sdk.language": "nodejs",
			},
			expected: "opentelemetry/nodejs",
		},
		"language only": {
			attrs: map[string]string{
				"telemetry.sdk.language": "go",
			},
			expected: "otlp/go",
		},
		"distro name": {
			attrs: map[string]string{
				"telemetry.sdk.name":     "opentelemetry",
				"telemetry.sdk.language": "java",
				"telemetry.distro.name":  "elastic",
			},
			expected: "opentelemetry/java/elastic",
		},
		"distro name without language": {
			attrs: map[string]string{
				"telemetry.sdk.name":    "opentelemetry",
				"telemetry.distro.name": "elastic",
			},
			expected: "opentelemetry/unknown/elastic",
		},
		"distro name without sdk name": {
			attrs: map[string]string{
				"telemetry.sdk.language": "java",
				"telemetry.distro.name":  "elastic",
			},
			expected: "otlp/java/elastic",
		},
		"sdk name only": {
			attrs: map[string]string{
				"telemetry.sdk.name": "opentelemetry",
			},
			expected: "opentelemetry",
		},
		"no telemetry attributes defaults to otlp": {
			attrs:    map[string]string{},
			expected: "otlp",
		},
		"already set is preserved": {
			attrs: map[string]string{
				"agent.name":             "my-custom-agent",
				"telemetry.sdk.language": "go",
			},
			expected: "my-custom-agent",
		},
		"already set to empty string is preserved": {
			attrs: map[string]string{
				"agent.name":             "",
				"telemetry.sdk.language": "go",
			},
			expected: "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			resource := pcommon.NewResource()
			for k, v := range tc.attrs {
				resource.Attributes().PutStr(k, v)
			}

			setAgentName(resource)

			got, ok := resource.Attributes().Get("agent.name")
			assert.True(t, ok, "agent.name should always be set")
			assert.Equal(t, tc.expected, got.Str())
		})
	}
}

func TestMetricsResourceEnricher_MultipleResources(t *testing.T) {
	md := pmetric.NewMetrics()
	rm1 := md.ResourceMetrics().AppendEmpty()
	rm1.Resource().Attributes().PutStr("telemetry.sdk.language", "go")
	rm2 := md.ResourceMetrics().AppendEmpty()
	rm2.Resource().Attributes().PutStr("agent.name", "my-custom-agent")

	var got pmetric.Metrics
	sink := &metricsSinkFunc{fn: func(_ context.Context, md pmetric.Metrics) error {
		got = md
		return nil
	}}
	enricher := &metricsResourceEnricher{next: sink}
	require.NoError(t, enricher.ConsumeMetrics(context.Background(), md))

	rms := got.ResourceMetrics()
	require.Equal(t, 2, rms.Len())
	name1, ok := rms.At(0).Resource().Attributes().Get("agent.name")
	require.True(t, ok)
	assert.Equal(t, "otlp/go", name1.Str())
	name2, ok := rms.At(1).Resource().Attributes().Get("agent.name")
	require.True(t, ok)
	assert.Equal(t, "my-custom-agent", name2.Str())
}

func TestLogsResourceEnricher_MultipleResources(t *testing.T) {
	ld := plog.NewLogs()
	rl1 := ld.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("telemetry.sdk.language", "go")
	rl2 := ld.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("agent.name", "my-custom-agent")

	var got plog.Logs
	sink := &logsSinkFunc{fn: func(_ context.Context, ld plog.Logs) error {
		got = ld
		return nil
	}}
	enricher := &logsResourceEnricher{next: sink}
	require.NoError(t, enricher.ConsumeLogs(context.Background(), ld))

	rls := got.ResourceLogs()
	require.Equal(t, 2, rls.Len())
	name1, ok := rls.At(0).Resource().Attributes().Get("agent.name")
	require.True(t, ok)
	assert.Equal(t, "otlp/go", name1.Str())
	name2, ok := rls.At(1).Resource().Attributes().Get("agent.name")
	require.True(t, ok)
	assert.Equal(t, "my-custom-agent", name2.Str())
}

type metricsSinkFunc struct {
	fn func(context.Context, pmetric.Metrics) error
}

func (s *metricsSinkFunc) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return s.fn(ctx, md)
}

func (s *metricsSinkFunc) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

type logsSinkFunc struct {
	fn func(context.Context, plog.Logs) error
}

func (s *logsSinkFunc) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return s.fn(ctx, ld)
}

func (s *logsSinkFunc) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
