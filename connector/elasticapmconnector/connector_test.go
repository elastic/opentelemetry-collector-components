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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
)

func TestConnector_LogsToMetrics(t *testing.T) {
	testCases := []struct {
		name string
	}{
		{name: "logs/service_summary"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nextMetrics := &consumertest.MetricsSink{}

			cfg := &Config{}
			l2m := newLogsToMetrics(t, connectortest.NewNopSettings(), cfg, nextMetrics)

			dir := filepath.Join("testdata", tc.name)
			input, err := golden.ReadLogs(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)
			expectedMetricsFile := filepath.Join(dir, "aggregated_metrics.yaml")

			err = l2m.ConsumeLogs(context.Background(), input)
			require.NoError(t, err)

			// Shut down to force metrics publication.
			err = l2m.Shutdown(context.Background())
			require.NoError(t, err)

			allMetrics := nextMetrics.AllMetrics()
			require.NotEmpty(t, allMetrics)
			assert.Equal(t, 0, allMetrics[0].MetricCount()) // should be one empty "next" metric from lsm
			compareAggregatedMetrics(t, expectedMetricsFile, allMetrics[1:], false)
		})
	}
}

func TestConnector_MetricsToMetrics(t *testing.T) {
	testCases := []struct {
		name string
	}{
		{name: "metrics/service_summary"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nextMetrics := &consumertest.MetricsSink{}

			cfg := &Config{}
			m2m := newMetricsConnector(t, connectortest.NewNopSettings(), cfg, nextMetrics)

			dir := filepath.Join("testdata", tc.name)
			input, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)
			expectedMetricsFile := filepath.Join(dir, "aggregated_metrics.yaml")

			err = m2m.ConsumeMetrics(context.Background(), input)
			require.NoError(t, err)

			// Shut down force metrics publication.
			err = m2m.Shutdown(context.Background())
			require.NoError(t, err)

			allMetrics := nextMetrics.AllMetrics()
			require.NotEmpty(t, allMetrics)
			assert.Equal(t, 0, allMetrics[0].MetricCount()) // should be one empty "next" metric from lsm
			compareAggregatedMetrics(t, expectedMetricsFile, allMetrics[1:], false)
		})
	}
}

func TestConnector_TracesToMetrics(t *testing.T) {
	testCases := []struct {
		name string
	}{
		{name: "traces/transaction_metrics"},
		{name: "traces/span_metrics"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nextMetrics := &consumertest.MetricsSink{}

			cfg := &Config{}
			t2m := newTracesConnector(t, connectortest.NewNopSettings(), cfg, nextMetrics)

			dir := filepath.Join("testdata", tc.name)
			input, err := golden.ReadTraces(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)
			expectedMetricsFile := filepath.Join(dir, "aggregated_metrics.yaml")

			err = t2m.ConsumeTraces(context.Background(), input)
			require.NoError(t, err)

			// Shut down to force metrics publication.
			err = t2m.Shutdown(context.Background())
			require.NoError(t, err)

			allMetrics := nextMetrics.AllMetrics()
			require.NotEmpty(t, allMetrics)
			assert.Equal(t, 0, allMetrics[0].MetricCount()) // should be one empty "next" metric from lsm
			compareAggregatedMetrics(t, expectedMetricsFile, allMetrics[1:], false)
		})
	}
}

func TestConnector_AggregationDirectory(t *testing.T) {
	aggdir := t.TempDir()
	entries, err := os.ReadDir(aggdir)
	require.NoError(t, err)
	require.Empty(t, entries)

	cfg := &Config{Aggregation: &AggregationConfig{
		Directory: aggdir,
	}}
	l2m := newLogsToMetrics(t, connectortest.NewNopSettings(), cfg, &consumertest.MetricsSink{})
	err = l2m.Shutdown(context.Background())
	require.NoError(t, err)

	entries, err = os.ReadDir(aggdir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
}

func TestConnector_AggregationMetadataKeys(t *testing.T) {
	cfg := &Config{Aggregation: &AggregationConfig{MetadataKeys: []string{"k"}}}

	var callInfo []client.Info
	nextConsumer, _ := consumer.NewMetrics(func(ctx context.Context, metrics pmetric.Metrics) error {
		callInfo = append(callInfo, client.FromContext(ctx))
		return nil
	})
	l2m := newLogsToMetrics(t, connectortest.NewNopSettings(), cfg, nextConsumer)

	input := plog.NewLogs()
	record := input.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.Body().SetStr("the log body")

	client1Info := client.Info{Metadata: client.NewMetadata(map[string][]string{"k": {"client1"}})}
	client2Info := client.Info{Metadata: client.NewMetadata(map[string][]string{"k": {"client2"}})}
	client1ctx := client.NewContext(context.Background(), client1Info)
	client2ctx := client.NewContext(context.Background(), client2Info)

	err := l2m.ConsumeLogs(client1ctx, input)
	require.NoError(t, err)
	err = l2m.ConsumeLogs(client2ctx, input)
	require.NoError(t, err)
	err = l2m.ConsumeLogs(client2ctx, input)
	require.NoError(t, err)

	// Shut down to force metrics publication.
	err = l2m.Shutdown(context.Background())
	require.NoError(t, err)

	// There should be three calls to the next metrics consumer:
	// - one for each call to ConsumeLogs above with any metrics
	//   that lsminterval doesn't understand: total of 3
	// - one for each interval (3) for each client (2): total of 6
	require.Len(t, callInfo, 9)
	assert.Equal(t, callInfo, []client.Info{
		client1Info,              // remainder
		client2Info, client2Info, // remainder
		client1Info, client2Info, // 1m
		client1Info, client2Info, // 10m
		client1Info, client2Info, // 60m
	})
}

func compareAggregatedMetrics(t testing.TB, expectedFile string, allMetrics []pmetric.Metrics, update bool) {
	t.Helper()

	// We should have 3 metrics from the lsminterval processor:
	// one empty (anything unprocessed by lsmintervalprocessor), and one each for 1m, 10m, 60m.
	require.Len(t, allMetrics, 3)

	// Concatenate the metrics into one ResourceMetrics & ScopeMetrics
	// and then compare together. Resource and scope attributes should
	// be identical.
	for i, metrics := range allMetrics {
		rms := metrics.ResourceMetrics()
		require.Equal(t, 1, rms.Len())

		rm := rms.At(0)
		require.Equal(t, 1, rm.ScopeMetrics().Len())
		if i > 0 {
			rm0 := allMetrics[0].ResourceMetrics().At(0)
			assert.Equal(t, rm0.Resource(), rm.Resource())

			sm := rm.ScopeMetrics().At(0)
			sm0 := rm0.ScopeMetrics().At(0)
			assert.Equal(t, sm0.Scope(), sm.Scope())
			sm.Metrics().At(0).CopyTo(sm0.Metrics().AppendEmpty())
		}
	}
	allMetrics[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().Sort(func(a, b pmetric.Metric) bool {
		return a.Name() < b.Name()
	})

	if update {
		assert.NoError(t, golden.WriteMetrics(t, expectedFile, allMetrics[0]))
	} else {
		expected, err := golden.ReadMetrics(expectedFile)
		require.NoError(t, err)
		assert.NoError(t, pmetrictest.CompareMetrics(expected, allMetrics[0],
			pmetrictest.IgnoreTimestamp(),
			pmetrictest.IgnoreMetricDataPointsOrder(),
			pmetrictest.IgnoreDatapointAttributesOrder(),
		))
	}
}

func newLogsToMetrics(t testing.TB, set connector.Settings, cfg *Config, next consumer.Metrics) connector.Logs {
	t.Helper()

	factory := NewFactory()
	c, err := factory.CreateLogsToMetrics(context.Background(), set, cfg, next)
	require.NoError(t, err)
	err = c.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Shutdown(context.Background())) })
	return c
}

func newMetricsConnector(t testing.TB, set connector.Settings, cfg *Config, next consumer.Metrics) connector.Metrics {
	t.Helper()

	factory := NewFactory()
	c, err := factory.CreateMetricsToMetrics(context.Background(), set, cfg, next)
	require.NoError(t, err)
	err = c.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Shutdown(context.Background())) })
	return c
}

func newTracesConnector(t testing.TB, set connector.Settings, cfg *Config, next consumer.Metrics) connector.Traces {
	t.Helper()

	factory := NewFactory()
	c, err := factory.CreateTracesToMetrics(context.Background(), set, cfg, next)
	require.NoError(t, err)
	err = c.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Shutdown(context.Background())) })
	return c
}
