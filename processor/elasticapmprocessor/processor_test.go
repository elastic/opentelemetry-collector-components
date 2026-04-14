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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
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
		"ecs_txn_db_non_intake": {
			input:       "testdata/ecs/txn_db/input.yaml",
			output:      "testdata/ecs/txn_db/output.yaml",
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
		"ecs_log_device_event": {
			input:       "testdata/ecs/elastic_log_device_event/input.yaml",
			output:      "testdata/ecs/elastic_log_device_event/output.yaml",
			mappingMode: "ecs",
			testType:    "logs",
			cfg:         apmConfig,
		},
		"ecs_log_device_crash_event_java": {
			input:       "testdata/ecs/elastic_log_device_crash_event/java_crash_input.yaml",
			output:      "testdata/ecs/elastic_log_device_crash_event/java_crash_output.yaml",
			mappingMode: "ecs",
			testType:    "logs",
			cfg:         apmConfig,
		},
		"ecs_log_device_crash_event_swift": {
			input:       "testdata/ecs/elastic_log_device_crash_event/swift_crash_input.yaml",
			output:      "testdata/ecs/elastic_log_device_crash_event/swift_crash_output.yaml",
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
		// ecs_intake test cases are meant to represent apm events ingested by the receiver/elasticapmintake
		// since there is different logic for otlp events with the ecs mapping mode
		"ecs_intake_txn_db": {
			input:       "testdata/ecs/intake/traces_txn_db_input.yaml",
			output:      "testdata/ecs/intake/traces_txn_db_output.yaml",
			mappingMode: "ecs",
			testType:    "traces",
			cfg:         apmConfig,
		},
		"ecs_intake_traces": {
			input:       "testdata/ecs/intake/traces_input.yaml",
			output:      "testdata/ecs/intake/traces_output.yaml",
			mappingMode: "ecs",
			testType:    "traces",
			cfg:         apmConfig,
		},
		"ecs_intake_logs_error": {
			input:       "testdata/ecs/intake/logs_error_input.yaml",
			output:      "testdata/ecs/intake/logs_error_output.yaml",
			mappingMode: "ecs",
			testType:    "logs",
			cfg:         apmConfig,
		},
		"ecs_intake_logs": {
			input:       "testdata/ecs/intake/logs_input.yaml",
			output:      "testdata/ecs/intake/logs_output.yaml",
			mappingMode: "ecs",
			testType:    "logs",
			cfg:         apmConfig,
		},
		"ecs_intake_metrics": {
			input:       "testdata/ecs/intake/metrics_input.yaml",
			output:      "testdata/ecs/intake/metrics_output.yaml",
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

func TestConsumeLogs_ECSOTLPFallbacks(t *testing.T) {
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
	})

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	next := &consumertest.LogsSink{}
	cfg := NewDefaultConfig().(*Config)

	lp, err := factory.CreateLogs(ctx, settings, cfg, next)
	require.NoError(t, err)

	logs := plog.NewLogs()
	resourceLog := logs.ResourceLogs().AppendEmpty()
	resource := resourceLog.Resource()
	resource.Attributes().PutStr("service.name", "test-service")
	resource.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "opentelemetry")

	logRecord := resourceLog.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	logAttrs := logRecord.Attributes()
	logAttrs.PutStr("http.method", "GET")
	logAttrs.PutStr("event.name", "user-login")

	require.NoError(t, lp.ConsumeLogs(ctx, logs))
	actual := next.AllLogs()[0]

	require.Equal(t, 1, actual.ResourceLogs().Len())
	actualResource := actual.ResourceLogs().At(0).Resource().Attributes()
	lang, ok := actualResource.Get(string(semconv.TelemetrySDKLanguageKey))
	require.True(t, ok)
	assert.Equal(t, "unknown", lang.Str())

	agentName, ok := actualResource.Get("agent.name")
	require.True(t, ok)
	assert.Equal(t, "opentelemetry", agentName.Str())

	actualLogAttrs := actual.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes()
	value, ok := actualLogAttrs.Get("labels.http_method")
	require.True(t, ok)
	assert.Equal(t, "GET", value.Str())

	value, ok = actualLogAttrs.Get("event.name")
	require.True(t, ok)
	assert.Equal(t, "user-login", value.Str())

	_, ok = actualLogAttrs.Get("http.method")
	assert.False(t, ok)
}

func TestConsumeLogs_ECSIntakeSkipsOTLPFallbacks(t *testing.T) {
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
	})

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	next := &consumertest.LogsSink{}
	cfg := NewDefaultConfig().(*Config)

	lp, err := factory.CreateLogs(ctx, settings, cfg, next)
	require.NoError(t, err)

	logs := plog.NewLogs()
	resourceLog := logs.ResourceLogs().AppendEmpty()
	resource := resourceLog.Resource()
	resource.Attributes().PutStr("service.name", "test-service")
	resource.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "ElasticAPM")

	logRecord := resourceLog.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	logRecord.Attributes().PutStr("http.method", "GET")

	require.NoError(t, lp.ConsumeLogs(ctx, logs))
	actual := next.AllLogs()[0]

	actualResource := actual.ResourceLogs().At(0).Resource().Attributes()
	_, ok := actualResource.Get(string(semconv.TelemetrySDKLanguageKey))
	assert.False(t, ok)

	actualLogAttrs := actual.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes()
	value, ok := actualLogAttrs.Get("http.method")
	require.True(t, ok)
	assert.Equal(t, "GET", value.Str())
	_, ok = actualLogAttrs.Get("labels.http_method")
	assert.False(t, ok)
}

func TestConsumeLogs_ECSMixedOriginUsesPerResourceEnricher(t *testing.T) {
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
	})

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	next := &consumertest.LogsSink{}
	cfg := NewDefaultConfig().(*Config)

	lp, err := factory.CreateLogs(ctx, settings, cfg, next)
	require.NoError(t, err)

	logs := plog.NewLogs()

	intakeResourceLog := logs.ResourceLogs().AppendEmpty()
	intakeResource := intakeResourceLog.Resource()
	intakeResource.Attributes().PutStr("service.name", "intake-service")
	intakeResource.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "ElasticAPM")
	intakeResourceLog.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	otlpResourceLog := logs.ResourceLogs().AppendEmpty()
	otlpResource := otlpResourceLog.Resource()
	otlpResource.Attributes().PutStr("service.name", "otlp-service")
	otlpResource.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "opentelemetry")
	otlpLogRecord := otlpResourceLog.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	otlpLogRecord.Attributes().PutStr("http.method", "GET")

	require.NoError(t, lp.ConsumeLogs(ctx, logs))
	actual := next.AllLogs()[0]

	require.Equal(t, 2, actual.ResourceLogs().Len())

	intakeAttrs := actual.ResourceLogs().At(0).Resource().Attributes()
	_, ok := intakeAttrs.Get(string(semconv.TelemetrySDKLanguageKey))
	assert.False(t, ok)

	otlpAttrs := actual.ResourceLogs().At(1).Resource().Attributes()
	value, ok := otlpAttrs.Get(string(semconv.TelemetrySDKLanguageKey))
	require.True(t, ok)
	assert.Equal(t, "unknown", value.Str())

	otlpLogAttrs := actual.ResourceLogs().At(1).ScopeLogs().At(0).LogRecords().At(0).Attributes()
	value, ok = otlpLogAttrs.Get("labels.http_method")
	require.True(t, ok)
	assert.Equal(t, "GET", value.Str())
	_, ok = otlpLogAttrs.Get("http.method")
	assert.False(t, ok)
}

func TestConsumeMetrics_ECSOTLPFallbacks(t *testing.T) {
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
	})

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	next := &consumertest.MetricsSink{}
	cfg := NewDefaultConfig().(*Config)

	mp, err := factory.CreateMetrics(ctx, settings, cfg, next)
	require.NoError(t, err)

	metrics := pmetric.NewMetrics()
	resourceMetric := metrics.ResourceMetrics().AppendEmpty()
	resource := resourceMetric.Resource()
	resource.Attributes().PutStr("service.name", "test-service")
	resource.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "opentelemetry")

	scopeMetrics := resourceMetric.ScopeMetrics().AppendEmpty()
	scopeMetrics.Scope().SetName("metrics-instrumentation")
	scopeMetrics.Scope().SetVersion("1.0.0")

	httpMetric := scopeMetrics.Metrics().AppendEmpty()
	httpMetric.SetName("http.requests.total")
	httpDP := httpMetric.SetEmptySum().DataPoints().AppendEmpty()
	httpDP.SetIntValue(1)
	httpAttrs := httpDP.Attributes()
	httpAttrs.PutStr("http.request.method", "GET")
	httpAttrs.PutStr("http.route", "/api/users")
	httpAttrs.PutInt("http.response.status_code", 200)

	memoryMetric := scopeMetrics.Metrics().AppendEmpty()
	memoryMetric.SetName("system.memory.usage")
	memoryDP := memoryMetric.SetEmptyGauge().DataPoints().AppendEmpty()
	memoryDP.SetDoubleValue(2048.5)
	memoryAttrs := memoryDP.Attributes()
	memoryAttrs.PutStr("host", "server-01")
	memoryAttrs.PutStr("state", "used")

	require.NoError(t, mp.ConsumeMetrics(ctx, metrics))
	actual := next.AllMetrics()[0]

	require.Equal(t, 1, actual.ResourceMetrics().Len())
	actualResource := actual.ResourceMetrics().At(0).Resource().Attributes()
	lang, ok := actualResource.Get(string(semconv.TelemetrySDKLanguageKey))
	require.True(t, ok)
	assert.Equal(t, "unknown", lang.Str())

	agentName, ok := actualResource.Get("agent.name")
	require.True(t, ok)
	assert.Equal(t, "opentelemetry", agentName.Str())

	actualMetrics := actual.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	actualHTTPAttrs := actualMetrics.At(0).Sum().DataPoints().At(0).Attributes()
	value, ok := actualHTTPAttrs.Get("labels.http_request_method")
	require.True(t, ok)
	assert.Equal(t, "GET", value.Str())
	value, ok = actualHTTPAttrs.Get("labels.http_route")
	require.True(t, ok)
	assert.Equal(t, "/api/users", value.Str())
	value, ok = actualHTTPAttrs.Get("numeric_labels.http_response_status_code")
	require.True(t, ok)
	assert.InDelta(t, 200, value.Double(), 1e-9)
	value, ok = actualHTTPAttrs.Get(elasticattr.ServiceFrameworkName)
	require.True(t, ok)
	assert.Equal(t, "metrics-instrumentation", value.Str())
	value, ok = actualHTTPAttrs.Get(elasticattr.ServiceFrameworkVersion)
	require.True(t, ok)
	assert.Equal(t, "1.0.0", value.Str())
	_, ok = actualHTTPAttrs.Get("http.request.method")
	assert.False(t, ok)
	_, ok = actualHTTPAttrs.Get("http.route")
	assert.False(t, ok)
	_, ok = actualHTTPAttrs.Get("http.response.status_code")
	assert.False(t, ok)

	actualMemoryAttrs := actualMetrics.At(1).Gauge().DataPoints().At(0).Attributes()
	value, ok = actualMemoryAttrs.Get("labels.host")
	require.True(t, ok)
	assert.Equal(t, "server-01", value.Str())
	value, ok = actualMemoryAttrs.Get("labels.state")
	require.True(t, ok)
	assert.Equal(t, "used", value.Str())
	value, ok = actualMemoryAttrs.Get(elasticattr.ServiceFrameworkName)
	require.True(t, ok)
	assert.Equal(t, "metrics-instrumentation", value.Str())
	value, ok = actualMemoryAttrs.Get(elasticattr.ServiceFrameworkVersion)
	require.True(t, ok)
	assert.Equal(t, "1.0.0", value.Str())
	_, ok = actualMemoryAttrs.Get("host")
	assert.False(t, ok)
	_, ok = actualMemoryAttrs.Get("state")
	assert.False(t, ok)
}

func TestConsumeMetrics_ECSIntakeSkipsOTLPFallbacks(t *testing.T) {
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
	})

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	next := &consumertest.MetricsSink{}
	cfg := NewDefaultConfig().(*Config)

	mp, err := factory.CreateMetrics(ctx, settings, cfg, next)
	require.NoError(t, err)

	metrics := pmetric.NewMetrics()
	resourceMetric := metrics.ResourceMetrics().AppendEmpty()
	resource := resourceMetric.Resource()
	resource.Attributes().PutStr("service.name", "test-service")
	resource.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "ElasticAPM")

	scopeMetrics := resourceMetric.ScopeMetrics().AppendEmpty()
	scopeMetrics.Scope().SetName("metrics-instrumentation")
	scopeMetrics.Scope().SetVersion("1.0.0")
	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName("http.requests.total")
	dp := metric.SetEmptySum().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	attrs := dp.Attributes()
	attrs.PutStr("http.request.method", "GET")
	attrs.PutStr("http.route", "/api/users")
	attrs.PutInt("http.response.status_code", 200)
	attrs.PutStr("host", "server-01")
	attrs.PutStr("state", "used")

	require.NoError(t, mp.ConsumeMetrics(ctx, metrics))
	actual := next.AllMetrics()[0]

	actualResource := actual.ResourceMetrics().At(0).Resource().Attributes()
	_, ok := actualResource.Get(string(semconv.TelemetrySDKLanguageKey))
	assert.False(t, ok)

	actualAttrs := actual.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0).Attributes()
	value, ok := actualAttrs.Get("http.request.method")
	require.True(t, ok)
	assert.Equal(t, "GET", value.Str())
	value, ok = actualAttrs.Get("http.route")
	require.True(t, ok)
	assert.Equal(t, "/api/users", value.Str())
	value, ok = actualAttrs.Get("http.response.status_code")
	require.True(t, ok)
	assert.EqualValues(t, 200, value.Int())
	value, ok = actualAttrs.Get("host")
	require.True(t, ok)
	assert.Equal(t, "server-01", value.Str())
	value, ok = actualAttrs.Get("state")
	require.True(t, ok)
	assert.Equal(t, "used", value.Str())
	_, ok = actualAttrs.Get("labels.http_request_method")
	assert.False(t, ok)
	_, ok = actualAttrs.Get("labels.http_route")
	assert.False(t, ok)
	_, ok = actualAttrs.Get("numeric_labels.http_response_status_code")
	assert.False(t, ok)
	_, ok = actualAttrs.Get("labels.host")
	assert.False(t, ok)
	_, ok = actualAttrs.Get("labels.state")
	assert.False(t, ok)
	_, ok = actualAttrs.Get(elasticattr.ServiceFrameworkName)
	assert.False(t, ok)
	_, ok = actualAttrs.Get(elasticattr.ServiceFrameworkVersion)
	assert.False(t, ok)
}

func TestConsumeMetrics_ECSMixedOriginUsesPerResourceEnricher(t *testing.T) {
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
	})

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	next := &consumertest.MetricsSink{}
	cfg := NewDefaultConfig().(*Config)

	mp, err := factory.CreateMetrics(ctx, settings, cfg, next)
	require.NoError(t, err)

	metrics := pmetric.NewMetrics()

	intakeResourceMetric := metrics.ResourceMetrics().AppendEmpty()
	intakeResource := intakeResourceMetric.Resource()
	intakeResource.Attributes().PutStr("service.name", "intake-service")
	intakeResource.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "ElasticAPM")
	intakeScopeMetrics := intakeResourceMetric.ScopeMetrics().AppendEmpty()
	intakeScopeMetrics.Scope().SetName("metrics-instrumentation")
	intakeScopeMetrics.Scope().SetVersion("1.0.0")
	intakeMetric := intakeScopeMetrics.Metrics().AppendEmpty()
	intakeMetric.SetName("intake.metric")
	intakeMetric.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1.0)

	otlpResourceMetric := metrics.ResourceMetrics().AppendEmpty()
	otlpResource := otlpResourceMetric.Resource()
	otlpResource.Attributes().PutStr("service.name", "otlp-service")
	otlpResource.Attributes().PutStr(string(semconv.TelemetrySDKNameKey), "opentelemetry")
	otlpScopeMetrics := otlpResourceMetric.ScopeMetrics().AppendEmpty()
	otlpScopeMetrics.Scope().SetName("metrics-instrumentation")
	otlpScopeMetrics.Scope().SetVersion("1.0.0")
	otlpMetric := otlpScopeMetrics.Metrics().AppendEmpty()
	otlpMetric.SetName("http.requests.total")
	otlpDP := otlpMetric.SetEmptySum().DataPoints().AppendEmpty()
	otlpDP.SetIntValue(1)
	otlpAttrs := otlpDP.Attributes()
	otlpAttrs.PutStr("http.request.method", "GET")
	otlpAttrs.PutStr("host", "server-01")

	require.NoError(t, mp.ConsumeMetrics(ctx, metrics))
	actual := next.AllMetrics()[0]

	require.Equal(t, 2, actual.ResourceMetrics().Len())

	intakeAttrs := actual.ResourceMetrics().At(0).Resource().Attributes()
	_, ok := intakeAttrs.Get(string(semconv.TelemetrySDKLanguageKey))
	assert.False(t, ok)

	otlpAttrsAfter := actual.ResourceMetrics().At(1).Resource().Attributes()
	value, ok := otlpAttrsAfter.Get(string(semconv.TelemetrySDKLanguageKey))
	require.True(t, ok)
	assert.Equal(t, "unknown", value.Str())

	actualDPAttrs := actual.ResourceMetrics().At(1).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0).Attributes()
	value, ok = actualDPAttrs.Get("labels.http_request_method")
	require.True(t, ok)
	assert.Equal(t, "GET", value.Str())
	value, ok = actualDPAttrs.Get("labels.host")
	require.True(t, ok)
	assert.Equal(t, "server-01", value.Str())
	_, ok = actualDPAttrs.Get("http.request.method")
	assert.False(t, ok)
	_, ok = actualDPAttrs.Get("host")
	assert.False(t, ok)
	value, ok = actualDPAttrs.Get(elasticattr.ServiceFrameworkName)
	require.True(t, ok)
	assert.Equal(t, "metrics-instrumentation", value.Str())
	value, ok = actualDPAttrs.Get(elasticattr.ServiceFrameworkVersion)
	require.True(t, ok)
	assert.Equal(t, "1.0.0", value.Str())
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

func TestIsIntakeECS(t *testing.T) {
	sdkKey := string(semconv.TelemetrySDKNameKey)

	cases := []struct {
		name  string
		attrs map[string]string
		want  bool
	}{
		{
			name:  "elastic_apm",
			attrs: map[string]string{sdkKey: "ElasticAPM"},
			want:  true,
		},
		{
			name:  "otlp_sdk",
			attrs: map[string]string{sdkKey: "opentelemetry"},
			want:  false,
		},
		{
			name:  "no_sdk_attribute",
			attrs: nil,
			want:  false,
		},
		{
			name:  "empty_sdk_value",
			attrs: map[string]string{sdkKey: ""},
			want:  false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := pcommon.NewResource()
			for k, v := range tc.attrs {
				res.Attributes().PutStr(k, v)
			}
			require.Equal(t, tc.want, isIntakeECS(res))
		})
	}
}
