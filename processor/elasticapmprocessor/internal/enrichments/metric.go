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

package enrichments // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/ecs"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/attribute"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
)

const metricSetNameApp = "app"

func EnrichMetric(metric pmetric.ResourceMetrics, cfg config.Config) {
	resAttrs := metric.Resource().Attributes()
	if cfg.Metric.ProcessorEvent.Enabled {
		attribute.PutStr(resAttrs, elasticattr.ProcessorEvent, "metric")
	}
	if cfg.Metric.MetricsetName.Enabled {
		// Add metricset.name to match apm-data logic
		// https://github.com/elastic/apm-data/blob/aa6b909/input/otlp/metrics.go#L171
		attribute.PutStr(resAttrs, elasticattr.MetricsetName, metricSetNameApp)
	}
}

func EnrichMetricDataPoints(metric pmetric.Metric, cfg config.Config) {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dataPoints := metric.Gauge().DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			enrichMetricDataPointAttributes(dataPoints.At(i).Attributes(), cfg)
		}
	case pmetric.MetricTypeSum:
		dataPoints := metric.Sum().DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			enrichMetricDataPointAttributes(dataPoints.At(i).Attributes(), cfg)
		}
	case pmetric.MetricTypeHistogram:
		dataPoints := metric.Histogram().DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			enrichMetricDataPointAttributes(dataPoints.At(i).Attributes(), cfg)
		}
	case pmetric.MetricTypeExponentialHistogram:
		dataPoints := metric.ExponentialHistogram().DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			enrichMetricDataPointAttributes(dataPoints.At(i).Attributes(), cfg)
		}
	case pmetric.MetricTypeSummary:
		dataPoints := metric.Summary().DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			enrichMetricDataPointAttributes(dataPoints.At(i).Attributes(), cfg)
		}
	}
}

// enrichMetricDataPointAttributes applies the raw OTLP metric fallback during
// enrichment, analogous to how EnrichLog delegates log-record fallback to
// EnrichLog. This happens later than apm-data's OTLP-to-APM conversion, so we
// must explicitly avoid relabeling attrs that identify aggregated metrics or
// influence exporter behavior.
func enrichMetricDataPointAttributes(attributes pcommon.Map, cfg config.Config) {
	if !cfg.Metric.TranslateUnsupportedAttributes.Enabled {
		return
	}
	if isAggregatedMetricDataPointAttributes(attributes) {
		return
	}
	ecs.TranslateMetricDataPointAttributes(attributes)
}

// isAggregatedMetricDataPointAttributes preserves aggregated-metric identity and
// exporter-only metadata before the raw OTLP fallback runs.
//
// In the collector we mutate OTel datapoints in place, so we must keep these attrs
// out of label fallback here.
// `elasticsearch.mapping.hints` is collector/exporter-specific metadata and has
// no apm-data equivalent, but it also needs to passthrough untouched.
func isAggregatedMetricDataPointAttributes(attributes pcommon.Map) bool {
	if _, ok := attributes.Get("metricset.name"); ok {
		return true
	}
	if _, ok := attributes.Get("metricset.interval"); ok {
		return true
	}
	if isESMappingHint(attributes) {
		return true
	}
	return false
}

func isESMappingHint(attributes pcommon.Map) bool {
	_, ok := attributes.Get("elasticsearch.mapping.hints")
	return ok
}
