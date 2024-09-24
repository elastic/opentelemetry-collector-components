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

package spanmetricsconnectorv2

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/config"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestConnector(t *testing.T) {
	testCases := []string{
		"with_default",
		"with_attributes",
		"with_missing_attribute",
		"with_missing_attribute_default_value",
		"with_custom_histogram_configs",
		"with_identical_metric_name_different_attrs",
		"with_identical_metric_name_desc_different_attrs",
		"with_summary",
		"with_counters",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			factory := NewFactory()
			settings := connectortest.NewNopSettings()
			settings.TelemetrySettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
			next := &consumertest.MetricsSink{}

			dir := filepath.Join("testdata", tc)
			cfg := createDefaultConfig()
			cm, err := confmaptest.LoadConf(filepath.Join(dir, "config.yaml"))
			require.NoError(t, err)
			sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(&cfg))
			require.NoError(t, component.ValidateConfig(cfg))

			connector, err := factory.CreateTracesToMetrics(ctx, settings, cfg, next)
			require.NoError(t, err)
			require.IsType(t, &spanMetrics{}, connector)

			inputTraces, err := golden.ReadTraces(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)
			expectedMetrics, err := golden.ReadMetrics(filepath.Join(dir, "output.yaml"))
			require.NoError(t, err)

			require.NoError(t, connector.ConsumeTraces(ctx, inputTraces))
			require.Len(t, next.AllMetrics(), 1)
			assert.NoError(t, pmetrictest.CompareMetrics(
				expectedMetrics,
				next.AllMetrics()[0],
				pmetrictest.IgnoreMetricDataPointsOrder(),
				pmetrictest.IgnoreMetricsOrder(),
				pmetrictest.IgnoreTimestamp(),
			))
		})
	}
}

func TestCalculateAdjustedCount(t *testing.T) {
	for _, tc := range []struct {
		tracestate string
		expected   uint64
	}{
		{"", 1},
		{"invalid=p:8;th:8", 1},
		{"ot=404:8", 1},
		{"ot=th:0", 1}, // 100% sampling
		{"ot=th:8", 2}, // 50% sampling
		{"ot=th:c", 4}, // 25% sampling
	} {
		t.Run("tracestate/"+tc.tracestate, func(t *testing.T) {
			assert.Equal(t, tc.expected, calculateAdjustedCount(tc.tracestate))
		})
	}
}

func BenchmarkConnector(b *testing.B) {
	factory := NewFactory()
	settings := connectortest.NewNopSettings()
	settings.TelemetrySettings.Logger = zaptest.NewLogger(b, zaptest.Level(zapcore.DebugLevel))
	next, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error {
		return nil
	})
	require.NoError(b, err)

	cfg := &config.Config{
		Spans: []config.MetricInfo{
			{
				Name:        "http.trace.span.duration",
				Description: "Span duration for HTTP spans",
				Attributes: []config.Attribute{
					{
						Key: "http.response.status_code",
					},
				},
				Histogram: config.Histogram{
					Explicit:    &config.ExplicitHistogram{},
					Exponential: &config.ExponentialHistogram{},
				},
				Summary: &config.Summary{},
			},
			{
				Name:        "db.trace.span.duration",
				Description: "Span duration for DB spans",
				Attributes: []config.Attribute{
					{
						Key: "msg.trace.span.duration",
					},
				},
				Histogram: config.Histogram{
					Explicit:    &config.ExplicitHistogram{},
					Exponential: &config.ExponentialHistogram{},
				},
				Summary: &config.Summary{},
			},
			{
				Name:        "msg.trace.span.duration",
				Description: "Span duration for DB spans",
				Attributes: []config.Attribute{
					{
						Key: "messaging.system",
					},
				},
				Histogram: config.Histogram{
					Explicit:    &config.ExplicitHistogram{},
					Exponential: &config.ExponentialHistogram{},
				},
				Summary: &config.Summary{},
			},
			{
				Name:        "404.span.duration",
				Description: "Span duration for missing attribute in input",
				Attributes: []config.Attribute{
					{
						Key: "404.attribute",
					},
				},
				Histogram: config.Histogram{
					Explicit:    &config.ExplicitHistogram{},
					Exponential: &config.ExponentialHistogram{},
				},
				Summary: &config.Summary{},
			},
			{
				Name:        "404.span.duration.default",
				Description: "Span duration with attribute default configured in input",
				Attributes: []config.Attribute{
					{
						Key:          "404.attribute.default",
						DefaultValue: "any",
					},
				},
				Histogram: config.Histogram{
					Explicit:    &config.ExplicitHistogram{},
					Exponential: &config.ExponentialHistogram{},
				},
				Summary: &config.Summary{},
			},
		},
	}
	require.NoError(b, cfg.Unmarshal(confmap.New())) // set required fields to default
	require.NoError(b, cfg.Validate())
	connector, err := factory.CreateTracesToMetrics(context.Background(), settings, cfg, next)
	require.NoError(b, err)
	inputTraces, err := golden.ReadTraces("testdata/traces.yaml")
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, connector.ConsumeTraces(context.Background(), inputTraces))
	}
}
