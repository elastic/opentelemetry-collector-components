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

package elasticapmprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor"

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/ptracetest"
)

func TestProcessor_ConsumeLogs(t *testing.T) {
	testCases := []struct {
		name string
	}{
		{name: "logs/service_summary"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nextLogs := &consumertest.LogsSink{}
			nextMetrics := &consumertest.MetricsSink{}

			settings := processortest.NewNopSettings()
			cfg := &Config{}
			lp := newLogsProcessor(t, settings, cfg, nextLogs)
			mp := newMetricsProcessor(t, settings, cfg, nextMetrics)
			err := lp.Start(context.Background(), componenttest.NewNopHost())
			require.NoError(t, err)
			err = mp.Start(context.Background(), componenttest.NewNopHost())
			require.NoError(t, err)

			dir := filepath.Join("testdata", tc.name)
			input, err := golden.ReadLogs(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)
			expectedMetricsFile := filepath.Join(dir, "aggregated_metrics.yaml")

			err = lp.ConsumeLogs(context.Background(), input)
			require.NoError(t, err)

			// Shut down processors to force metrics publication.
			err = lp.Shutdown(context.Background())
			require.NoError(t, err)
			err = mp.Shutdown(context.Background())
			require.NoError(t, err)

			allMetrics := nextMetrics.AllMetrics()
			assert.Equal(t, []plog.Logs{input}, nextLogs.AllLogs())
			require.NotEmpty(t, allMetrics)
			assert.Equal(t, 0, allMetrics[0].MetricCount()) // should be one empty "next" metric from lsm
			compareAggregatedMetrics(t, expectedMetricsFile, allMetrics[1:], false)
		})
	}
}

func TestProcessor_ConsumeMetrics(t *testing.T) {
	testCases := []struct {
		name string
	}{
		{name: "metrics/service_summary"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nextMetrics := &consumertest.MetricsSink{}

			settings := processortest.NewNopSettings()
			cfg := &Config{}
			mp := newMetricsProcessor(t, settings, cfg, nextMetrics)
			err := mp.Start(context.Background(), componenttest.NewNopHost())
			require.NoError(t, err)

			dir := filepath.Join("testdata", tc.name)
			input, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)
			expectedMetricsFile := filepath.Join(dir, "aggregated_metrics.yaml")

			err = mp.ConsumeMetrics(context.Background(), input)
			require.NoError(t, err)

			// Shut down processors to force metrics publication.
			err = mp.Shutdown(context.Background())
			require.NoError(t, err)

			allMetrics := nextMetrics.AllMetrics()
			require.NotEmpty(t, allMetrics)
			assert.Equal(t, 0, allMetrics[0].MetricCount()) // should be one empty "next" metric from lsm
			assert.NoError(t, pmetrictest.CompareMetrics(input, allMetrics[1]))
			compareAggregatedMetrics(t, expectedMetricsFile, allMetrics[2:], false)
		})
	}
}

func TestProcessor_ConsumeTraces(t *testing.T) {
	testCases := []struct {
		name string
	}{
		{name: "traces/transaction_metrics"},
		{name: "traces/span_metrics"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nextTraces := &consumertest.TracesSink{}
			nextMetrics := &consumertest.MetricsSink{}

			settings := processortest.NewNopSettings()
			cfg := &Config{}
			tp := newTracesProcessor(t, settings, cfg, nextTraces)
			mp := newMetricsProcessor(t, settings, cfg, nextMetrics)
			err := tp.Start(context.Background(), componenttest.NewNopHost())
			require.NoError(t, err)
			err = mp.Start(context.Background(), componenttest.NewNopHost())
			require.NoError(t, err)

			dir := filepath.Join("testdata", tc.name)
			input, err := golden.ReadTraces(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)
			expectedNext, err := golden.ReadTraces(filepath.Join(dir, "next.yaml"))
			require.NoError(t, err)
			expectedMetricsFile := filepath.Join(dir, "aggregated_metrics.yaml")

			// Clone input as it will be mutated by the elastictrace processor.
			inputClone := ptrace.NewTraces()
			input.CopyTo(inputClone)
			err = tp.ConsumeTraces(context.Background(), inputClone)
			require.NoError(t, err)

			// Shut down processors to force metrics publication.
			err = tp.Shutdown(context.Background())
			require.NoError(t, err)
			err = mp.Shutdown(context.Background())
			require.NoError(t, err)

			allTraces := nextTraces.AllTraces()
			allMetrics := nextMetrics.AllMetrics()
			require.Len(t, allTraces, 1)
			require.NotEmpty(t, allMetrics)
			assert.Equal(t, 0, allMetrics[0].MetricCount()) // should be one empty "next" metric from lsm
			assert.NoError(t, ptracetest.CompareTraces(expectedNext, allTraces[0]))
			compareAggregatedMetrics(t, expectedMetricsFile, allMetrics[1:], false)
		})
	}
}

/*
func TestProcessor_MetadataKeys(t *testing.T) {
}
*/

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

func newLogsProcessor(t testing.TB, set processor.Settings, cfg *Config, next consumer.Logs) processor.Logs {
	t.Helper()

	factory := NewFactory()
	p, err := factory.CreateLogs(context.Background(), set, cfg, next)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Shutdown(context.Background())) })
	return p
}

func newMetricsProcessor(t testing.TB, set processor.Settings, cfg *Config, next consumer.Metrics) processor.Metrics {
	t.Helper()

	factory := NewFactory()
	p, err := factory.CreateMetrics(context.Background(), set, cfg, next)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Shutdown(context.Background())) })
	return p
}

func newTracesProcessor(t testing.TB, set processor.Settings, cfg *Config, next consumer.Traces) processor.Traces {
	t.Helper()

	factory := NewFactory()
	p, err := factory.CreateTraces(context.Background(), set, cfg, next)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Shutdown(context.Background())) })
	return p
}
