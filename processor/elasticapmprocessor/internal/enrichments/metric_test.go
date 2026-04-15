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

package enrichments

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/sanitize"
)

func TestEnrichMetric(t *testing.T) {
	getMetric := func() pmetric.ResourceMetrics {
		metrics := pmetric.NewMetrics()
		resourceMetrics := metrics.ResourceMetrics().AppendEmpty()

		// Add a metric data point to make it a valid metric
		scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("test.metric")
		metric.SetUnit("1")
		metric.SetEmptyGauge()
		metric.Gauge().DataPoints().AppendEmpty().SetDoubleValue(1.0)

		return resourceMetrics
	}

	for _, tc := range []struct {
		name          string
		input         pmetric.ResourceMetrics
		config        config.Config
		expectedAttrs map[string]any
	}{
		{
			name: "existing_attributes_not_overridden",
			input: func() pmetric.ResourceMetrics {
				resourceMetrics := getMetric()
				resource := resourceMetrics.Resource()

				// Set existing attributes that should not be overridden
				resource.Attributes().PutStr(elasticattr.ProcessorEvent, "existing-processor-event")
				resource.Attributes().PutStr(elasticattr.AgentName, "existing-agent-name")
				resource.Attributes().PutStr(elasticattr.AgentVersion, "existing-agent-version")

				return resourceMetrics
			}(),
			config: config.Enabled(),
			expectedAttrs: map[string]any{
				elasticattr.ProcessorEvent: "existing-processor-event",
				elasticattr.AgentName:      "existing-agent-name",
				elasticattr.AgentVersion:   "existing-agent-version",
				elasticattr.MetricsetName:  "app",
			},
		},
		{
			name: "metricset_name_enabled_sets_app_on_resource",
			input: func() pmetric.ResourceMetrics {
				rm := getMetric()
				rm.ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().AppendEmpty().SetDoubleValue(2.0)
				return rm
			}(),
			config: config.Enabled(),
			expectedAttrs: map[string]any{
				elasticattr.AgentName:     "otlp",
				elasticattr.AgentVersion:  "unknown",
				elasticattr.MetricsetName: "app",
			},
		},
		{
			name: "metricset_name_existing_not_overridden",
			input: func() pmetric.ResourceMetrics {
				resourceMetrics := getMetric()
				resourceMetrics.Resource().Attributes().PutStr(elasticattr.MetricsetName, "custom")
				return resourceMetrics
			}(),
			config: config.Enabled(),
			expectedAttrs: map[string]any{
				elasticattr.AgentName:     "otlp",
				elasticattr.AgentVersion:  "unknown",
				elasticattr.MetricsetName: "custom",
			},
		},
		{
			name:  "metricset_name_disabled_not_set",
			input: getMetric(),
			config: func() config.Config {
				cfg := config.Enabled()
				cfg.Metric.MetricsetName.Enabled = false
				return cfg
			}(),
			expectedAttrs: map[string]any{
				elasticattr.AgentName:    "otlp",
				elasticattr.AgentVersion: "unknown",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectedResourceMetrics := pmetric.NewResourceMetrics()
			tc.input.Resource().CopyTo(expectedResourceMetrics.Resource())

			// Merge with the expected attributes
			for k, v := range tc.expectedAttrs {
				_ = expectedResourceMetrics.Resource().Attributes().PutEmpty(k).FromRaw(v)
			}

			// Enrich the metric
			EnrichMetric(tc.input, tc.config)
			EnrichResource(tc.input.Resource(), tc.config.Resource)

			// Verify attributes match expected
			actualAttrs := tc.input.Resource().Attributes().AsRaw()
			expectedAttrs := expectedResourceMetrics.Resource().Attributes().AsRaw()
			assert.Equal(t, expectedAttrs, actualAttrs, "resource attributes should match expected")
		})
	}
}

func TestEnrichMetrics_TranslateUnsupportedAttributes(t *testing.T) {
	cfg := config.Enabled()
	cfg.Metric.TranslateUnsupportedAttributes.Enabled = true

	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	scopeMetrics.Scope().SetName("metrics-instrumentation")
	scopeMetrics.Scope().SetVersion("1.0.0")
	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName("test.metric")
	dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1.0)
	attrs := dp.Attributes()
	attrs.PutStr("data_stream.dataset", "apm.internal")
	attrs.PutStr("data_stream.namespace", "default")
	attrs.PutStr("data_stream.type", "metrics")
	attrs.PutStr("host", "server-01")
	attrs.PutStr("state", "used")
	attrs.PutStr("event.module", "system")
	attrs.PutStr("system.process.cmdline", "/usr/bin/java")
	attrs.PutStr("system.filesystem.mount_point", "/mnt/data")
	attrs.PutStr("user.name", "appuser")

	enricher := NewDefaultMetricEnricher(cfg)
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		enricher.EnrichResourceMetrics(context.Background(), metrics.ResourceMetrics().At(i))
	}

	actualAttrs := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).Attributes()

	value, ok := actualAttrs.Get("data_stream.dataset")
	require.True(t, ok)
	assert.Equal(t, "apm.internal", value.Str())
	value, ok = actualAttrs.Get("data_stream.namespace")
	require.True(t, ok)
	assert.Equal(t, "default", value.Str())
	value, ok = actualAttrs.Get("data_stream.type")
	require.True(t, ok)
	assert.Equal(t, "metrics", value.Str())
	value, ok = actualAttrs.Get("labels.host")
	require.True(t, ok)
	assert.Equal(t, "server-01", value.Str())
	value, ok = actualAttrs.Get("labels.state")
	require.True(t, ok)
	assert.Equal(t, "used", value.Str())
	value, ok = actualAttrs.Get("event.module")
	require.True(t, ok)
	assert.Equal(t, "system", value.Str())
	value, ok = actualAttrs.Get("system.process.cmdline")
	require.True(t, ok)
	assert.Equal(t, "/usr/bin/java", value.Str())
	value, ok = actualAttrs.Get("system.filesystem.mount_point")
	require.True(t, ok)
	assert.Equal(t, "/mnt/data", value.Str())
	value, ok = actualAttrs.Get("user.name")
	require.True(t, ok)
	assert.Equal(t, "appuser", value.Str())
	value, ok = actualAttrs.Get(elasticattr.ServiceFrameworkName)
	require.True(t, ok)
	assert.Equal(t, "metrics-instrumentation", value.Str())
	value, ok = actualAttrs.Get(elasticattr.ServiceFrameworkVersion)
	require.True(t, ok)
	assert.Equal(t, "1.0.0", value.Str())
	_, ok = actualAttrs.Get("host")
	assert.False(t, ok)
	_, ok = actualAttrs.Get("state")
	assert.False(t, ok)
}

func TestEnrichMetrics_TruncatesPreservedMetricSpecialCaseAttributes(t *testing.T) {
	cfg := config.Enabled()
	cfg.Metric.TranslateUnsupportedAttributes.Enabled = true

	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName("test.metric")
	dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1.0)
	attrs := dp.Attributes()
	longValue := strings.Repeat("a", int(sanitize.StandardKeyWordLength)+1)
	attrs.PutStr("system.process.cmdline", longValue)
	attrs.PutStr("system.filesystem.mount_point", longValue)
	attrs.PutStr("user.name", longValue)
	attrs.PutStr("event.module", longValue)
	attrs.PutStr("system.process.state", longValue)

	enricher := NewDefaultMetricEnricher(cfg)
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		enricher.EnrichResourceMetrics(context.Background(), metrics.ResourceMetrics().At(i))
	}

	actualAttrs := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).Attributes()
	expected := strings.Repeat("a", int(sanitize.StandardKeyWordLength))

	value, ok := actualAttrs.Get("system.process.cmdline")
	require.True(t, ok)
	assert.Equal(t, expected, value.Str())
	value, ok = actualAttrs.Get("system.filesystem.mount_point")
	require.True(t, ok)
	assert.Equal(t, expected, value.Str())
	value, ok = actualAttrs.Get("user.name")
	require.True(t, ok)
	assert.Equal(t, expected, value.Str())

	// These preserved attrs are intentionally not truncated in apm-data.
	value, ok = actualAttrs.Get("event.module")
	require.True(t, ok)
	assert.Equal(t, longValue, value.Str())
	value, ok = actualAttrs.Get("system.process.state")
	require.True(t, ok)
	assert.Equal(t, longValue, value.Str())
}

func TestEnrichMetricDataPoints_SkipsAggregatedMetricAttributes(t *testing.T) {
	cfg := config.Enabled()
	cfg.Metric.TranslateUnsupportedAttributes.Enabled = true

	metric := pmetric.NewMetric()
	metric.SetName("service_summary")
	dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1.0)
	attrs := dp.Attributes()
	attrs.PutStr("metricset.name", "service_summary")
	attrs.PutStr("host", "server-01")
	attrs.PutStr("state", "used")

	scope := pcommon.NewInstrumentationScope()
	scope.SetName("github.com/open-telemetry/opentelemetry-collector-contrib/connector/signaltometricsconnector")
	scope.SetVersion("1.0.0")
	scopeAttrs := scope.Attributes()

	EnrichMetricDataPoints(metric, scopeAttrs, cfg)

	actualAttrs := metric.Gauge().DataPoints().At(0).Attributes()
	value, ok := actualAttrs.Get("host")
	require.True(t, ok)
	assert.Equal(t, "server-01", value.Str())
	value, ok = actualAttrs.Get("state")
	require.True(t, ok)
	assert.Equal(t, "used", value.Str())
	_, ok = actualAttrs.Get("labels.host")
	assert.False(t, ok)
	_, ok = actualAttrs.Get("labels.state")
	assert.False(t, ok)
	_, ok = actualAttrs.Get(elasticattr.ServiceFrameworkName)
	assert.False(t, ok)
	_, ok = actualAttrs.Get(elasticattr.ServiceFrameworkVersion)
	assert.False(t, ok)
}

func TestEnrichMetricDataPoints_SkipsMetricsWithMappingHints(t *testing.T) {
	cfg := config.Enabled()
	cfg.Metric.TranslateUnsupportedAttributes.Enabled = true

	metric := pmetric.NewMetric()
	metric.SetName("transaction.duration.histogram")
	dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1.0)
	attrs := dp.Attributes()
	hints := attrs.PutEmptySlice("elasticsearch.mapping.hints")
	hints.AppendEmpty().SetStr("_doc_count")
	attrs.PutStr("host", "server-01")

	scope := pcommon.NewInstrumentationScope()
	scope.SetName("github.com/open-telemetry/opentelemetry-collector-contrib/connector/signaltometricsconnector")
	scope.SetVersion("1.0.0")
	scopeAttrs := scope.Attributes()

	EnrichMetricDataPoints(metric, scopeAttrs, cfg)

	actualAttrs := metric.Gauge().DataPoints().At(0).Attributes()
	value, ok := actualAttrs.Get("host")
	require.True(t, ok)
	assert.Equal(t, "server-01", value.Str())
	_, ok = actualAttrs.Get("labels.host")
	assert.False(t, ok)
	_, ok = actualAttrs.Get(elasticattr.ServiceFrameworkName)
	assert.False(t, ok)
}

func TestEnrichMetricDataPointAttributes_NoOpWhenDisabled(t *testing.T) {
	cfg := config.Enabled()
	cfg.Metric.TranslateUnsupportedAttributes.Enabled = false

	attrs := pcommon.NewMap()
	attrs.PutStr("host", "server-01")

	scope := pcommon.NewInstrumentationScope()
	scope.SetName("metrics-instrumentation")
	scope.SetVersion("1.0.0")
	scopeAttrs := scope.Attributes()

	enrichMetricDataPointAttributes(attrs, scopeAttrs, cfg)

	value, ok := attrs.Get("host")
	require.True(t, ok)
	assert.Equal(t, "server-01", value.Str())
	_, ok = attrs.Get("labels.host")
	assert.False(t, ok)
	_, ok = attrs.Get(elasticattr.ServiceFrameworkName)
	assert.False(t, ok)
}
