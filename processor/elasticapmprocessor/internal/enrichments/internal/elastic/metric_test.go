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

package elastic

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectedResourceMetrics := pmetric.NewResourceMetrics()
			tc.input.Resource().CopyTo(expectedResourceMetrics.Resource())

			// Merge with the expected attributes
			for k, v := range tc.expectedAttrs {
				expectedResourceMetrics.Resource().Attributes().PutEmpty(k).FromRaw(v)
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
