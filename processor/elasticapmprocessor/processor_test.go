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
	"flag"
	"net"
	"path/filepath"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/ptracetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/metadata"
)

var update = flag.Bool("update", false, "Flag to generate/updated the expected yaml files")

// TestProcessor does a basic test to check if traces, logs, and metrics
// are processed correctly.
func TestProcessor(t *testing.T) {
	defaultCfg := NewDefaultConfig().(*Config)

	// apmConfig is configuration that mimics APM Server behaviour,
	// which is expected to be used with ECS mapping mode.
	apmConfig := NewDefaultConfig().(*Config)
	apmConfig.HostIPEnabled = true
	apmConfig.ServiceNameInDataStreamDataset = true

	disableHostNameEnrichmentConfig := NewDefaultConfig().(*Config)
	disableHostNameEnrichmentConfig.Resource.OverrideHostName.Enabled = false

	testCases := map[string]struct {
		input       string
		output      string
		mappingMode string
		testType    string
		cfg         *Config
	}{
		"elastic_txn_http": {
			input:    "testdata/elastic_txn_http/input.yaml",
			output:   "testdata/elastic_txn_http/output.yaml",
			testType: "traces",
			cfg:      defaultCfg,
		},
		"elastic_txn_messaging": {
			input:    "testdata/elastic_txn_messaging/input.yaml",
			output:   "testdata/elastic_txn_messaging/output.yaml",
			testType: "traces",
			cfg:      defaultCfg,
		},
		"elastic_txn_db": {
			input:    "testdata/elastic_txn_db/input.yaml",
			output:   "testdata/elastic_txn_db/output.yaml",
			testType: "traces",
			cfg:      defaultCfg,
		},
		"elastic_span_http": {
			input:    "testdata/elastic_span_http/input.yaml",
			output:   "testdata/elastic_span_http/output.yaml",
			testType: "traces",
			cfg:      defaultCfg,
		},
		"elastic_span_messaging": {
			input:    "testdata/elastic_span_messaging/input.yaml",
			output:   "testdata/elastic_span_messaging/output.yaml",
			testType: "traces",
			cfg:      defaultCfg,
		},
		"elastic_span_db": {
			input:    "testdata/elastic_span_db/input.yaml",
			output:   "testdata/elastic_span_db/output.yaml",
			testType: "traces",
			cfg:      defaultCfg,
		},
		"ecs_span": {
			input:       "testdata/ecs/elastic_span_db/input.yaml",
			output:      "testdata/ecs/elastic_span_db/output.yaml",
			mappingMode: "ecs",
			testType:    "traces",
			cfg:         apmConfig,
		},
		"ecs_log": {
			input:       "testdata/ecs/elastic_log/input.yaml",
			output:      "testdata/ecs/elastic_log/output.yaml",
			mappingMode: "ecs",
			testType:    "logs",
			cfg:         apmConfig,
		},
		"ecs_metrics": {
			input:       "testdata/ecs/elastic_metric/input.yaml",
			output:      "testdata/ecs/elastic_metric/output.yaml",
			mappingMode: "ecs",
			testType:    "metrics",
			cfg:         apmConfig,
		},
		"ecs_span_hostname": {
			input:       "testdata/ecs/elastic_hostname/spans_input.yaml",
			output:      "testdata/ecs/elastic_hostname/spans_output.yaml",
			mappingMode: "ecs",
			testType:    "traces",
			cfg:         disableHostNameEnrichmentConfig,
		},
		"ecs_log_hostname": {
			input:       "testdata/ecs/elastic_hostname/logs_input.yaml",
			output:      "testdata/ecs/elastic_hostname/logs_output.yaml",
			mappingMode: "ecs",
			testType:    "logs",
			cfg:         disableHostNameEnrichmentConfig,
		},
		"ecs_metric_hostname": {
			input:       "testdata/ecs/elastic_hostname/metrics_input.yaml",
			output:      "testdata/ecs/elastic_hostname/metrics_output.yaml",
			mappingMode: "ecs",
			testType:    "metrics",
			cfg:         disableHostNameEnrichmentConfig,
		},
		"ecs_internal_metrics": {
			input:       "testdata/ecs/elastic_internal_metrics/input.yaml",
			output:      "testdata/ecs/elastic_internal_metrics/output.yaml",
			mappingMode: "ecs",
			testType:    "metrics",
			cfg:         apmConfig,
		},
	}

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			clientInfo := client.Info{
				Addr: &net.IPAddr{IP: net.IPv4(1, 2, 3, 4)},
			}
			if tc.mappingMode != "" {
				clientInfo.Metadata = client.NewMetadata(map[string][]string{
					"x-elastic-mapping-mode": {tc.mappingMode},
				})
			}

			ctx := client.NewContext(context.Background(), clientInfo)
			switch tc.testType {
			case "traces":
				testTraces(t, ctx, factory, settings, tc.cfg, tc.input, tc.output)
			case "logs":
				testLogs(t, ctx, factory, settings, tc.cfg, tc.input, tc.output)
			case "metrics":
				testMetrics(t, ctx, factory, settings, tc.cfg, tc.input, tc.output)
			}
		})
	}
}

func testTraces(t *testing.T, ctx context.Context, factory processor.Factory, settings processor.Settings, cfg *Config, inputFile, outputFile string) {
	next := &consumertest.TracesSink{}
	tp, err := factory.CreateTraces(ctx, settings, cfg, next)
	require.NoError(t, err)
	require.IsType(t, &TraceProcessor{}, tp)

	inputTraces, err := golden.ReadTraces(inputFile)
	require.NoError(t, err)

	require.NoError(t, tp.ConsumeTraces(ctx, inputTraces))
	actual := next.AllTraces()[0]
	if *update {
		err := golden.WriteTraces(t, outputFile, actual)
		assert.NoError(t, err)
	}
	expectedTraces, err := golden.ReadTraces(outputFile)
	require.NoError(t, err)
	assert.NoError(t, ptracetest.CompareTraces(expectedTraces, actual))
}

func testLogs(t *testing.T, ctx context.Context, factory processor.Factory, settings processor.Settings, cfg *Config, inputFile, outputFile string) {
	next := &consumertest.LogsSink{}
	lp, err := factory.CreateLogs(ctx, settings, cfg, next)
	require.NoError(t, err)

	inputLogs, err := golden.ReadLogs(inputFile)
	require.NoError(t, err)

	require.NoError(t, lp.ConsumeLogs(ctx, inputLogs))
	actual := next.AllLogs()[0]
	if *update {
		err := golden.WriteLogs(t, outputFile, actual)
		assert.NoError(t, err)
	}
	expectedLogs, err := golden.ReadLogs(outputFile)
	require.NoError(t, err)
	assert.NoError(t, plogtest.CompareLogs(expectedLogs, actual))
}

func testMetrics(t *testing.T, ctx context.Context, factory processor.Factory, settings processor.Settings, cfg *Config, inputFile, outputFile string) {
	next := &consumertest.MetricsSink{}
	mp, err := factory.CreateMetrics(ctx, settings, cfg, next)
	require.NoError(t, err)

	inputMetrics, err := golden.ReadMetrics(inputFile)
	require.NoError(t, err)

	require.NoError(t, mp.ConsumeMetrics(ctx, inputMetrics))
	actual := next.AllMetrics()[0]
	if *update {
		err := golden.WriteMetrics(t, outputFile, actual)
		assert.NoError(t, err)
	}
	expectedMetrics, err := golden.ReadMetrics(outputFile)
	require.NoError(t, err)
	assert.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actual, pmetrictest.IgnoreMetricsOrder(), pmetrictest.IgnoreResourceMetricsOrder(), pmetrictest.IgnoreTimestamp()))
}

// TestSkipEnrichmentLogs tests that logs are only enriched when skipEnrichment is false or when mapping mode is ecs
func TestSkipEnrichmentLogs(t *testing.T) {
	testCases := []struct {
		name           string
		skipEnrichment bool
		mappingMode    string
	}{
		{
			name:           "logs_false",
			skipEnrichment: false,
			mappingMode:    "",
		},
		{
			name:           "logs_false_ecs",
			skipEnrichment: false,
			mappingMode:    "ecs",
		},
		{
			name:           "logs_true_ecs",
			skipEnrichment: true,
			mappingMode:    "ecs",
		},
		{
			name:           "logs_true_no_ecs",
			skipEnrichment: true,
			mappingMode:    "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.mappingMode != "" {
				ctx = client.NewContext(ctx, client.Info{
					Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {tc.mappingMode}}),
				})
			}

			factory := NewFactory()
			settings := processortest.NewNopSettings(metadata.Type)
			next := &consumertest.LogsSink{}

			cfg := NewDefaultConfig().(*Config)
			cfg.SkipEnrichment = tc.skipEnrichment

			lp, err := factory.CreateLogs(ctx, settings, cfg, next)
			require.NoError(t, err)

			dir := filepath.Join("testdata", "skip_enrichment")
			inputLogs, err := golden.ReadLogs(filepath.Join(dir, tc.name+"_input.yaml"))
			require.NoError(t, err)

			outputFile := filepath.Join(dir, tc.name+"_output.yaml")
			require.NoError(t, lp.ConsumeLogs(ctx, inputLogs))
			actual := next.AllLogs()[0]
			if *update {
				err := golden.WriteLogs(t, outputFile, actual)
				assert.NoError(t, err)
			}
			expectedLogs, err := golden.ReadLogs(outputFile)
			require.NoError(t, err)
			assert.NoError(t, plogtest.CompareLogs(expectedLogs, actual))
		})
	}
}

// TestSkipEnrichmentMetrics tests that metrics are only enriched when skipEnrichment is false or when mapping mode is ecs
func TestSkipEnrichmentMetrics(t *testing.T) {
	testCases := []struct {
		name           string
		skipEnrichment bool
		mappingMode    string
	}{
		{
			name:           "metrics_false",
			skipEnrichment: false,
			mappingMode:    "",
		},
		{
			name:           "metrics_false_ecs",
			skipEnrichment: false,
			mappingMode:    "ecs",
		},
		{
			name:           "metrics_true_ecs",
			skipEnrichment: true,
			mappingMode:    "ecs",
		},
		{
			name:           "metrics_true_no_ecs",
			skipEnrichment: true,
			mappingMode:    "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.mappingMode != "" {
				ctx = client.NewContext(ctx, client.Info{
					Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {tc.mappingMode}}),
				})
			}

			factory := NewFactory()
			settings := processortest.NewNopSettings(metadata.Type)
			next := &consumertest.MetricsSink{}

			cfg := NewDefaultConfig().(*Config)
			cfg.SkipEnrichment = tc.skipEnrichment

			mp, err := factory.CreateMetrics(ctx, settings, cfg, next)
			require.NoError(t, err)

			dir := filepath.Join("testdata", "skip_enrichment")
			inputMetrics, err := golden.ReadMetrics(filepath.Join(dir, tc.name+"_input.yaml"))
			require.NoError(t, err)

			outputFile := filepath.Join(dir, tc.name+"_output.yaml")
			require.NoError(t, mp.ConsumeMetrics(ctx, inputMetrics))
			actual := next.AllMetrics()[0]
			if *update {
				err := golden.WriteMetrics(t, outputFile, actual)
				assert.NoError(t, err)
			}
			expectedMetrics, err := golden.ReadMetrics(outputFile)
			require.NoError(t, err)
			assert.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actual, pmetrictest.IgnoreMetricsOrder(), pmetrictest.IgnoreResourceMetricsOrder()))
		})
	}
}

// TestErrorLogsRouting tests that error logs are routed to apm.error data stream
func TestECSErrorRouting(t *testing.T) {
	testcases := map[string]struct {
		input  string
		output string
		cfg    *Config
	}{
		"error-logs-default": {
			input:  "testdata/ecs/elastic_error/logs_input.yaml",
			output: "testdata/ecs/elastic_error/logs_output.yaml",
			cfg: func() *Config {
				cfg := NewDefaultConfig().(*Config)
				return cfg
			}(),
		},
		"error-logs-with-servicename": {
			input:  "testdata/ecs/elastic_error/logs_servicename_input.yaml",
			output: "testdata/ecs/elastic_error/logs_servicename_output.yaml",
			cfg: func() *Config {
				cfg := NewDefaultConfig().(*Config)
				cfg.ServiceNameInDataStreamDataset = true
				return cfg
			}(),
		},
		"otlp-exception-logs": {
			input:  "testdata/ecs/elastic_error/logs_otlp_exception_input.yaml",
			output: "testdata/ecs/elastic_error/logs_otlp_exception_output.yaml",
			cfg: func() *Config {
				cfg := NewDefaultConfig().(*Config)
				return cfg
			}(),
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ctx = client.NewContext(ctx, client.Info{
				Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
			})

			factory := NewFactory()
			settings := processortest.NewNopSettings(metadata.Type)
			next := &consumertest.LogsSink{}

			lp, err := factory.CreateLogs(ctx, settings, tc.cfg, next)
			require.NoError(t, err)

			inputLogs, err := golden.ReadLogs(tc.input)
			require.NoError(t, err)

			require.NoError(t, lp.ConsumeLogs(ctx, inputLogs))
			actual := next.AllLogs()[0]

			if *update {
				err := golden.WriteLogs(t, tc.output, actual)
				assert.NoError(t, err)
			}

			expectedLogs, err := golden.ReadLogs(tc.output)
			require.NoError(t, err)
			assert.NoError(t, plogtest.CompareLogs(expectedLogs, actual))
		})
	}
}

// TestInternalMetricsUnitClearing tests that internal metrics have their unit field cleared.
// This matches the behavior in apm-data:
// https://github.com/elastic/apm-data/blob/main/model/modelprocessor/datastream_test.go#L241-L260
func TestInternalMetricsUnitClearing(t *testing.T) {
	ctx := context.Background()
	ctx = client.NewContext(ctx, client.Info{
		Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
	})

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	next := &consumertest.MetricsSink{}

	cfg := NewDefaultConfig().(*Config)
	cfg.ServiceNameInDataStreamDataset = true
	mp, err := factory.CreateMetrics(ctx, settings, cfg, next)
	require.NoError(t, err)

	inputMetrics, err := golden.ReadMetrics("testdata/ecs/elastic_internal_metrics/input.yaml")
	require.NoError(t, err)

	require.NoError(t, mp.ConsumeMetrics(ctx, inputMetrics))
	actual := next.AllMetrics()[0]

	// Verify that internal metrics have their unit cleared
	resourceMetrics := actual.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		scopeMetrics := resourceMetrics.At(i).ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			metrics := scopeMetrics.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				metricName := metric.Name()

				// Check data points to determine if this is an internal metric
				isInternal := false
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dataPoints := metric.Gauge().DataPoints()
					if dataPoints.Len() > 0 {
						dp := dataPoints.At(0)
						if dataset, ok := dp.Attributes().Get("data_stream.dataset"); ok {
							if dataset.Str() == "apm.internal" || dataset.Str() == "apm.transaction.1m" {
								isInternal = true
							}
						}
					}
				case pmetric.MetricTypeSum:
					dataPoints := metric.Sum().DataPoints()
					if dataPoints.Len() > 0 {
						dp := dataPoints.At(0)
						if dataset, ok := dp.Attributes().Get("data_stream.dataset"); ok {
							if dataset.Str() == "apm.internal" || dataset.Str() == "apm.transaction.1m" {
								isInternal = true
							}
						}
					}
				}

				if isInternal {
					assert.Empty(t, metric.Unit(), "internal metric %s should have empty unit", metricName)
				}
			}
		}
	}
}

// TestECSSpanEventErrorRouting tests that span events with errors are routed to apm.error data stream
func TestECSSpanEventErrorRouting(t *testing.T) {
	ctx := context.Background()
	ctx = client.NewContext(ctx, client.Info{
		Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
	})

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	next := &consumertest.TracesSink{}

	cfg := NewDefaultConfig().(*Config)
	tp, err := factory.CreateTraces(ctx, settings, cfg, next)
	require.NoError(t, err)

	inputTraces, err := golden.ReadTraces("testdata/ecs/elastic_error/span_otlp_exception_input.yaml")
	require.NoError(t, err)

	require.NoError(t, tp.ConsumeTraces(ctx, inputTraces))
	actual := next.AllTraces()[0]

	// Verify resource-level data stream attributes
	require.Equal(t, 1, actual.ResourceSpans().Len())
	resourceSpan := actual.ResourceSpans().At(0)
	resourceAttrs := resourceSpan.Resource().Attributes()

	dataStreamType, _ := resourceAttrs.Get("data_stream.type")
	assert.Equal(t, "traces", dataStreamType.Str())
	dataStreamDataset, _ := resourceAttrs.Get("data_stream.dataset")
	assert.Equal(t, "apm", dataStreamDataset.Str())
	dataStreamNamespace, _ := resourceAttrs.Get("data_stream.namespace")
	assert.Equal(t, "default", dataStreamNamespace.Str())

	// Verify span events
	require.Equal(t, 1, resourceSpan.ScopeSpans().Len())
	scopeSpan := resourceSpan.ScopeSpans().At(0)
	require.Equal(t, 2, scopeSpan.Spans().Len())

	// First span should have an error event with data stream routing
	span1 := scopeSpan.Spans().At(0)
	assert.Equal(t, "process-order", span1.Name())
	require.Equal(t, 2, span1.Events().Len())

	// Check exception event
	exceptionEvent := span1.Events().At(0)
	assert.Equal(t, "exception", exceptionEvent.Name())
	exceptionAttrs := exceptionEvent.Attributes()

	// Verify error data stream attributes on the event
	eventDataStreamType, _ := exceptionAttrs.Get("data_stream.type")
	assert.Equal(t, "traces", eventDataStreamType.Str())
	eventDataStreamDataset, _ := exceptionAttrs.Get("data_stream.dataset")
	assert.Equal(t, "apm.error", eventDataStreamDataset.Str())
	eventDataStreamNamespace, _ := exceptionAttrs.Get("data_stream.namespace")
	assert.Equal(t, "default", eventDataStreamNamespace.Str())

	// Verify exception attributes are present
	exceptionType, _ := exceptionAttrs.Get("exception.type")
	assert.Equal(t, "java.lang.NullPointerException", exceptionType.Str())
	exceptionMessage, _ := exceptionAttrs.Get("exception.message")
	assert.Equal(t, "Null pointer exception", exceptionMessage.Str())

	// Check regular event (should not have error data stream)
	regularEvent := span1.Events().At(1)
	assert.Equal(t, "regular-event", regularEvent.Name())
	regularAttrs := regularEvent.Attributes()
	_, hasErrorDataStream := regularAttrs.Get("data_stream.dataset")
	assert.False(t, hasErrorDataStream)

	// Second span should have no error events
	span2 := scopeSpan.Spans().At(1)
	assert.Equal(t, "successful-span", span2.Name())
	require.Equal(t, 1, span2.Events().Len())

	infoEvent := span2.Events().At(0)
	assert.Equal(t, "info", infoEvent.Name())
	infoAttrs := infoEvent.Attributes()
	_, hasInfoErrorDataStream := infoAttrs.Get("data_stream.dataset")
	assert.False(t, hasInfoErrorDataStream)
}
