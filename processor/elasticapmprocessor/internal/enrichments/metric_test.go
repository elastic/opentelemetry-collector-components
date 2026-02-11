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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
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
		checkResult   func(t *testing.T, rm pmetric.ResourceMetrics)
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
			checkResult: func(t *testing.T, rm pmetric.ResourceMetrics) {
				v, ok := rm.Resource().Attributes().Get(elasticattr.MetricsetName)
				require.True(t, ok, "metricset.name should be set on resource")
				assert.Equal(t, "app", v.Str())
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
			checkResult: func(t *testing.T, rm pmetric.ResourceMetrics) {
				v, ok := rm.Resource().Attributes().Get(elasticattr.MetricsetName)
				require.True(t, ok)
				assert.Equal(t, "custom", v.Str(), "existing metricset.name must not be overridden")
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
			checkResult: func(t *testing.T, rm pmetric.ResourceMetrics) {
				_, ok := rm.Resource().Attributes().Get(elasticattr.MetricsetName)
				assert.False(t, ok, "metricset.name must not be set on resource when disabled")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectedResourceMetrics := pmetric.NewResourceMetrics()
			tc.input.Resource().CopyTo(expectedResourceMetrics.Resource())

			// Merge with the expected attributes. Only run when the test case
			// defines expectedAttrs; cases that use checkResult alone skip this.
			if len(tc.expectedAttrs) > 0 {
				for k, v := range tc.expectedAttrs {
					_ = expectedResourceMetrics.Resource().Attributes().PutEmpty(k).FromRaw(v)
				}
			}

			// Enrich the metric
			EnrichMetric(tc.input, tc.config)
			EnrichResource(tc.input.Resource(), tc.config.Resource)

			// Verify resource attributes match expected (only when test provides expectedAttrs).
			// Ensures enrichment did not override existing attributes and added only what was expected.
			if len(tc.expectedAttrs) > 0 {
				actualAttrs := tc.input.Resource().Attributes().AsRaw()
				expectedAttrs := expectedResourceMetrics.Resource().Attributes().AsRaw()
				assert.Equal(t, expectedAttrs, actualAttrs, "resource attributes should match expected")
			}
			// Optional case-specific checks (e.g. metricset.name present/absent/unchanged).
			if tc.checkResult != nil {
				tc.checkResult(t, tc.input)
			}
		})
	}
}
