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
	"context"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"
)

// MetricEnricher enriches metric data for a specific mapping mode.
type MetricEnricher interface {
	EnrichMetrics(ctx context.Context, md pmetric.Metrics)
}

// ecsMetricEnricher contains the shared ECS metric enrichment pipeline
// embedded by APMMetricEnricher and OTelMetricEnricher.
type ecsMetricEnricher struct {
	enricher                       *Enricher
	hostIPEnabled                  bool
	serviceNameInDataStreamDataset bool
}

func (e *ecsMetricEnricher) enrichMetrics(ctx context.Context, md pmetric.Metrics) {
	resourceMetrics := md.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		resourceMetric := resourceMetrics.At(i)
		resource := resourceMetric.Resource()
		ecsPreProcessResource(ctx, resource, routing.DataStreamTypeMetrics, e.serviceNameInDataStreamDataset, e.hostIPEnabled)

		// Check if resource has a service name for routing decisions
		hasServiceName := false
		if serviceName, ok := resource.Attributes().Get(routing.ServiceNameAttributeKey); ok && serviceName.Str() != "" {
			hasServiceName = true
		}

		// Route internal metrics to appropriate data streams if needed.
		routeMetricsToDataStream(resourceMetric.ScopeMetrics(), hasServiceName)
	}
	e.enricher.EnrichMetrics(md)
}

// routeMetricsToDataStream routes individual metric data points to their
// appropriate data streams based on metric name and service name presence.
func routeMetricsToDataStream(scopeMetrics pmetric.ScopeMetricsSlice, hasServiceName bool) {
	for j := 0; j < scopeMetrics.Len(); j++ {
		metrics := scopeMetrics.At(j).Metrics()
		for k := 0; k < metrics.Len(); k++ {
			metric := metrics.At(k)
			metricName := metric.Name()

			// Track if any data point is routed to internal metrics
			isInternal := false

			// Route data points based on metric type
			switch metric.Type() {
			case pmetric.MetricTypeGauge:
				dataPoints := metric.Gauge().DataPoints()
				for l := 0; l < dataPoints.Len(); l++ {
					if routing.EncodeDataStreamMetricDataPoint(dataPoints.At(l).Attributes(), metricName, hasServiceName) {
						isInternal = true
					}
				}
			case pmetric.MetricTypeSum:
				dataPoints := metric.Sum().DataPoints()
				for l := 0; l < dataPoints.Len(); l++ {
					if routing.EncodeDataStreamMetricDataPoint(dataPoints.At(l).Attributes(), metricName, hasServiceName) {
						isInternal = true
					}
				}
			case pmetric.MetricTypeHistogram:
				dataPoints := metric.Histogram().DataPoints()
				for l := 0; l < dataPoints.Len(); l++ {
					if routing.EncodeDataStreamMetricDataPoint(dataPoints.At(l).Attributes(), metricName, hasServiceName) {
						isInternal = true
					}
				}
			case pmetric.MetricTypeExponentialHistogram:
				dataPoints := metric.ExponentialHistogram().DataPoints()
				for l := 0; l < dataPoints.Len(); l++ {
					if routing.EncodeDataStreamMetricDataPoint(dataPoints.At(l).Attributes(), metricName, hasServiceName) {
						isInternal = true
					}
				}
			case pmetric.MetricTypeSummary:
				dataPoints := metric.Summary().DataPoints()
				for l := 0; l < dataPoints.Len(); l++ {
					if routing.EncodeDataStreamMetricDataPoint(dataPoints.At(l).Attributes(), metricName, hasServiceName) {
						isInternal = true
					}
				}
			}

			// Internal metrics data stream does not use dynamic mapping,
			// so we must drop unit if specified.
			// This matches the behavior in apm-data:
			// https://github.com/elastic/apm-data/blob/main/model/modelprocessor/datastream.go#L160-L172
			if isInternal {
				metric.SetUnit("")
			}
		}
	}
}

// APMMetricEnricher handles elastic APM intake metric events in ECS mode.
type APMMetricEnricher struct {
	ecsMetricEnricher
}

func (e *APMMetricEnricher) EnrichMetrics(ctx context.Context, md pmetric.Metrics) {
	e.enrichMetrics(ctx, md)
}

// NewAPMMetricEnricher creates a MetricEnricher for elastic APM intake events.
func NewAPMMetricEnricher(baseCfg config.Config, hostIPEnabled bool, serviceNameInDataStreamDataset bool) *APMMetricEnricher {
	cfg := ecsAPMConfig(baseCfg)
	return &APMMetricEnricher{
		ecsMetricEnricher: ecsMetricEnricher{
			enricher:                       NewEnricher(cfg),
			hostIPEnabled:                  hostIPEnabled,
			serviceNameInDataStreamDataset: serviceNameInDataStreamDataset,
		},
	}
}

// OTelMetricEnricher handles elastic OTel metric events in ECS mode.
type OTelMetricEnricher struct {
	ecsMetricEnricher
}

func (e *OTelMetricEnricher) EnrichMetrics(ctx context.Context, md pmetric.Metrics) {
	e.enrichMetrics(ctx, md)
}

// NewOTelMetricEnricher creates a MetricEnricher for elastic OTel events.
func NewOTelMetricEnricher(baseCfg config.Config, hostIPEnabled bool, serviceNameInDataStreamDataset bool) *OTelMetricEnricher {
	cfg := ecsOTelConfig(baseCfg)
	cfg.Metric.TranslateUnsupportedAttributes.Enabled = true
	return &OTelMetricEnricher{
		ecsMetricEnricher: ecsMetricEnricher{
			enricher:                       NewEnricher(cfg),
			hostIPEnabled:                  hostIPEnabled,
			serviceNameInDataStreamDataset: serviceNameInDataStreamDataset,
		},
	}
}

// DefaultMetricEnricher handles non-ECS metric events.
type DefaultMetricEnricher struct {
	enricher *Enricher
}

func (e *DefaultMetricEnricher) EnrichMetrics(_ context.Context, md pmetric.Metrics) {
	e.enricher.EnrichMetrics(md)
}

// NewDefaultMetricEnricher creates a MetricEnricher for non-ECS metric events.
func NewDefaultMetricEnricher(baseCfg config.Config) *DefaultMetricEnricher {
	return &DefaultMetricEnricher{
		enricher: NewEnricher(baseCfg),
	}
}
