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

package signaltometricsconnector

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/config"
	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

const testDataDir = "testdata"

func TestConnectorWithTraces(t *testing.T) {
	testCases := []string{
		"sum",
		"histograms",
		"exponential_histograms",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			traceTestDataDir := filepath.Join(testDataDir, "traces")
			inputTraces, err := golden.ReadTraces(filepath.Join(traceTestDataDir, "traces.yaml"))
			require.NoError(t, err)

			next := &consumertest.MetricsSink{}
			tcTestDataDir := filepath.Join(traceTestDataDir, tc)
			factory, settings, cfg := setupConnector(t, tcTestDataDir)
			connector, err := factory.CreateTracesToMetrics(ctx, settings, cfg, next)
			require.NoError(t, err)
			require.IsType(t, &signalToMetrics{}, connector)
			expectedMetrics, err := golden.ReadMetrics(filepath.Join(tcTestDataDir, "output.yaml"))
			require.NoError(t, err)

			require.NoError(t, connector.ConsumeTraces(ctx, inputTraces))
			require.Len(t, next.AllMetrics(), 1)
			assertAggregatedMetrics(t, expectedMetrics, next.AllMetrics()[0])
		})
	}
}

func TestConnectorWithMetrics(t *testing.T) {
	testCases := []string{
		"sum",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			metricTestDataDir := filepath.Join(testDataDir, "metrics")
			inputMetrics, err := golden.ReadMetrics(filepath.Join(metricTestDataDir, "metrics.yaml"))
			require.NoError(t, err)

			next := &consumertest.MetricsSink{}
			tcTestDataDir := filepath.Join(metricTestDataDir, tc)
			factory, settings, cfg := setupConnector(t, tcTestDataDir)
			connector, err := factory.CreateMetricsToMetrics(ctx, settings, cfg, next)
			require.NoError(t, err)
			require.IsType(t, &signalToMetrics{}, connector)
			expectedMetrics, err := golden.ReadMetrics(filepath.Join(tcTestDataDir, "output.yaml"))
			require.NoError(t, err)

			require.NoError(t, connector.ConsumeMetrics(ctx, inputMetrics))
			require.Len(t, next.AllMetrics(), 1)
			assertAggregatedMetrics(t, expectedMetrics, next.AllMetrics()[0])
		})
	}
}

func TestConnectorWithLogs(t *testing.T) {
	testCases := []string{
		"sum",
		"histograms",
		"exponential_histograms",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			logTestDataDir := filepath.Join(testDataDir, "logs")
			inputLogs, err := golden.ReadLogs(filepath.Join(logTestDataDir, "logs.yaml"))
			require.NoError(t, err)

			next := &consumertest.MetricsSink{}
			tcTestDataDir := filepath.Join(logTestDataDir, tc)
			factory, settings, cfg := setupConnector(t, tcTestDataDir)
			connector, err := factory.CreateLogsToMetrics(ctx, settings, cfg, next)
			require.NoError(t, err)
			require.IsType(t, &signalToMetrics{}, connector)
			expectedMetrics, err := golden.ReadMetrics(filepath.Join(tcTestDataDir, "output.yaml"))
			require.NoError(t, err)

			require.NoError(t, connector.ConsumeLogs(ctx, inputLogs))
			require.Len(t, next.AllMetrics(), 1)
			assertAggregatedMetrics(t, expectedMetrics, next.AllMetrics()[0])
		})
	}
}

func TestCalculateAdjustedCount(t *testing.T) {
	for _, tc := range []struct {
		tracestate string
		expected   int64
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

func BenchmarkConnectorWithTraces(b *testing.B) {
	factory := NewFactory()
	settings := connectortest.NewNopSettings()
	settings.TelemetrySettings.Logger = zaptest.NewLogger(b, zaptest.Level(zapcore.DebugLevel))
	next, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error {
		return nil
	})
	require.NoError(b, err)

	cfg := &config.Config{Spans: testMetricInfo(b)}
	require.NoError(b, cfg.Unmarshal(confmap.New())) // set required fields to default
	require.NoError(b, cfg.Validate())
	connector, err := factory.CreateTracesToMetrics(context.Background(), settings, cfg, next)
	require.NoError(b, err)
	inputTraces, err := golden.ReadTraces("testdata/traces/traces.yaml")
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := connector.ConsumeTraces(context.Background(), inputTraces); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnectorWithMetrics(b *testing.B) {
	factory := NewFactory()
	settings := connectortest.NewNopSettings()
	settings.TelemetrySettings.Logger = zaptest.NewLogger(b, zaptest.Level(zapcore.DebugLevel))
	next, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error {
		return nil
	})
	require.NoError(b, err)

	cfg := &config.Config{Datapoints: testMetricInfo(b)}
	require.NoError(b, cfg.Unmarshal(confmap.New())) // set required fields to default
	require.NoError(b, cfg.Validate())
	connector, err := factory.CreateMetricsToMetrics(context.Background(), settings, cfg, next)
	require.NoError(b, err)
	inputMetrics, err := golden.ReadMetrics("testdata/metrics/metrics.yaml")
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := connector.ConsumeMetrics(context.Background(), inputMetrics); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnectorWithLogs(b *testing.B) {
	factory := NewFactory()
	settings := connectortest.NewNopSettings()
	settings.TelemetrySettings.Logger = zaptest.NewLogger(b, zaptest.Level(zapcore.DebugLevel))
	next, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error {
		return nil
	})
	require.NoError(b, err)

	cfg := &config.Config{Logs: testMetricInfo(b)}
	require.NoError(b, cfg.Unmarshal(confmap.New())) // set required fields to default
	require.NoError(b, cfg.Validate())
	connector, err := factory.CreateLogsToMetrics(context.Background(), settings, cfg, next)
	require.NoError(b, err)
	inputLogs, err := golden.ReadLogs("testdata/logs/logs.yaml")
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := connector.ConsumeLogs(context.Background(), inputLogs); err != nil {
			b.Fatal(err)
		}
	}
}

// testMetricInfo creates a metric info with all metric types that could be used
// for all the supported signals. To do this, it uses common OTTL funcs and literals.
func testMetricInfo(b *testing.B) []config.MetricInfo {
	b.Helper()

	return []config.MetricInfo{
		{
			Name:        "test.histogram",
			Description: "Test histogram",
			Unit:        "ms",
			IncludeResourceAttributes: []config.Attribute{
				{
					Key: "resource.foo",
				},
				{
					Key:          "404.attribute",
					DefaultValue: "test_404_attribute",
				},
			},
			Attributes: []config.Attribute{
				{
					Key: "http.response.status_code",
				},
			},
			Histogram: &config.Histogram{
				Buckets: []float64{2, 4, 6, 8, 10, 50, 100, 200, 400, 800, 1000, 1400, 2000, 5000, 10_000, 15_000},
				Value:   "1.4",
			},
		},
		{
			Name:        "test.exphistogram",
			Description: "Test exponential histogram",
			Unit:        "ms",
			IncludeResourceAttributes: []config.Attribute{
				{
					Key: "resource.foo",
				},
				{
					Key:          "404.attribute",
					DefaultValue: "test_404_attribute",
				},
			},
			Attributes: []config.Attribute{
				{
					Key: "http.response.status_code",
				},
			},
			ExponentialHistogram: &config.ExponentialHistogram{
				Value:   "2.4",
				MaxSize: 160,
			},
		},
		{
			Name:        "test.sum",
			Description: "Test sum",
			Unit:        "ms",
			IncludeResourceAttributes: []config.Attribute{
				{
					Key: "resource.foo",
				},
				{
					Key:          "404.attribute",
					DefaultValue: "test_404_attribute",
				},
			},
			Attributes: []config.Attribute{
				{
					Key: "http.response.status_code",
				},
			},
			Sum: &config.Sum{
				Value: "5.4",
			},
		},
	}
}

func setupConnector(
	t *testing.T, testFilePath string,
) (connector.Factory, connector.Settings, component.Config) {
	t.Helper()
	factory := NewFactory()
	settings := connectortest.NewNopSettings()
	settings.TelemetrySettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))

	cfg := createDefaultConfig()
	cm, err := confmaptest.LoadConf(filepath.Join(testFilePath, "config.yaml"))
	require.NoError(t, err)
	sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(&cfg))
	require.NoError(t, component.ValidateConfig(cfg))

	return factory, settings, cfg
}

func assertAggregatedMetrics(t *testing.T, expected, actual pmetric.Metrics) bool {
	t.Helper()
	return assert.NoError(t, pmetrictest.CompareMetrics(
		expected, actual,
		pmetrictest.IgnoreMetricDataPointsOrder(),
		pmetrictest.IgnoreMetricsOrder(),
		pmetrictest.IgnoreTimestamp(),
	))
}
