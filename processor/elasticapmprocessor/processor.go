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

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/ecs"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"
)

var _ processor.Traces = (*TraceProcessor)(nil)
var _ processor.Metrics = (*MetricProcessor)(nil)
var _ processor.Logs = (*LogProcessor)(nil)

type TraceProcessor struct {
	component.StartFunc
	component.ShutdownFunc

	next        consumer.Traces
	enricher    *enrichments.Enricher
	ecsEnricher *enrichments.Enricher
	// intakeECSEnricher applies ECS enrichment for traces ingested by the
	// elasticapmintakereceiver. It extends ecsEnricher with intake-specific overrides.
	intakeECSEnricher *enrichments.Enricher
	logger            *zap.Logger
	cfg               *Config
}

func NewTraceProcessor(cfg *Config, next consumer.Traces, logger *zap.Logger) *TraceProcessor {
	enricherConfig := cfg.Config
	ecsEnricherConfig := cfg.Config
	ecsEnricherConfig.Resource.DeploymentEnvironment.Enabled = false

	intakeECSEnricherConfig := ecsEnricherConfig
	// The intake receiver already sets transaction.root; skip re-deriving it
	// to avoid overwriting the intake-supplied value or avoid deriving a value
	// when the provided transaction.result is empty to match existing apm-data logic.
	intakeECSEnricherConfig.Transaction.Result.Enabled = false

	return &TraceProcessor{
		next:              next,
		logger:            logger,
		enricher:          enrichments.NewEnricher(enricherConfig),
		ecsEnricher:       enrichments.NewEnricher(ecsEnricherConfig),
		intakeECSEnricher: enrichments.NewEnricher(intakeECSEnricherConfig),
		cfg:               cfg,
	}
}

func (p *TraceProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *TraceProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	enricher := p.enricher
	if isECS(ctx) {
		enricher = p.ecsEnricher
		if isTraceIntakeECS(td) {
			enricher = p.intakeECSEnricher
		}

		resourceSpans := td.ResourceSpans()
		for i := 0; i < resourceSpans.Len(); i++ {
			resourceSpan := resourceSpans.At(i)
			resource := resourceSpan.Resource()
			ecs.TranslateResourceMetadata(resource)
			ecs.ApplyResourceConventions(resource)
			// Traces signal never need to be routed to service-specific datasets
			routing.EncodeDataStream(resource, routing.DataStreamTypeTraces, false)
			if p.cfg.HostIPEnabled {
				ecs.SetHostIP(ctx, resource.Attributes())
			}

			// Iterate through spans to find errors in span events
			scopeSpans := resourceSpan.ScopeSpans()
			for j := 0; j < scopeSpans.Len(); j++ {
				spans := scopeSpans.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					span := spans.At(k)
					events := span.Events()
					for l := 0; l < events.Len(); l++ {
						event := events.At(l)
						if routing.IsErrorEvent(event.Attributes()) {
							// Override the resource-level data stream for error events in spans.
							routing.EncodeErrorDataStream(event.Attributes(), routing.DataStreamTypeTraces)
						}
					}
				}
			}
		}
	}

	enricher.EnrichTraces(td)

	return p.next.ConsumeTraces(ctx, td)
}

func isECS(ctx context.Context) bool {
	clientCtx := client.FromContext(ctx)
	mappingMode := getMetadataValue(clientCtx)
	return mappingMode == "ecs"
}

// isTraceIntakeECS reports whether the traces originated from the elasticapmintakereceiver
// by checking for a pre-existing processor.event on the first span.
//
// The intake receiver unconditionally sets processor.event on every transaction,
// span, and error event it maps.
func isTraceIntakeECS(td ptrace.Traces) bool {
	rs := td.ResourceSpans()
	if rs.Len() == 0 {
		return false
	}
	ss := rs.At(0).ScopeSpans()
	if ss.Len() == 0 {
		return false
	}
	spans := ss.At(0).Spans()
	if spans.Len() == 0 {
		return false
	}
	_, ok := spans.At(0).Attributes().Get(elasticattr.ProcessorEvent)
	return ok
}

func getMetadataValue(info client.Info) string {
	if values := info.Metadata.Get("x-elastic-mapping-mode"); len(values) > 0 {
		return values[0]
	}
	return ""
}

type LogProcessor struct {
	component.StartFunc
	component.ShutdownFunc

	next        consumer.Logs
	enricher    *enrichments.Enricher
	ecsEnricher *enrichments.Enricher
	logger      *zap.Logger
	cfg         *Config
}

func newLogProcessor(cfg *Config, next consumer.Logs, logger *zap.Logger) *LogProcessor {
	enricherConfig := cfg.Config
	ecsEnricherConfig := cfg.Config
	ecsEnricherConfig.Resource.DeploymentEnvironment.Enabled = false
	ecsEnricherConfig.Resource.AgentVersion.Enabled = false
	// disable the transaction result enrichment to avoid deriving a value
	// when the provided result is empty to match existing apm-data logic
	ecsEnricherConfig.Transaction.Result.Enabled = false
	return &LogProcessor{
		next:        next,
		logger:      logger,
		enricher:    enrichments.NewEnricher(enricherConfig),
		ecsEnricher: enrichments.NewEnricher(ecsEnricherConfig),
		cfg:         cfg,
	}
}

func (p *LogProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

type MetricProcessor struct {
	component.StartFunc
	component.ShutdownFunc

	next        consumer.Metrics
	enricher    *enrichments.Enricher
	ecsEnricher *enrichments.Enricher
	logger      *zap.Logger
	cfg         *Config
}

func newMetricProcessor(cfg *Config, next consumer.Metrics, logger *zap.Logger) *MetricProcessor {
	enricherConfig := cfg.Config
	ecsEnricherConfig := cfg.Config
	ecsEnricherConfig.Resource.DeploymentEnvironment.Enabled = false
	return &MetricProcessor{
		next:        next,
		logger:      logger,
		enricher:    enrichments.NewEnricher(enricherConfig),
		ecsEnricher: enrichments.NewEnricher(ecsEnricherConfig),
		cfg:         cfg,
	}
}

func (p *MetricProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *MetricProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	enricher := p.enricher
	ecsMode := isECS(ctx)
	if ecsMode {
		enricher = p.ecsEnricher
		resourceMetrics := md.ResourceMetrics()
		for i := 0; i < resourceMetrics.Len(); i++ {
			resourceMetric := resourceMetrics.At(i)
			resource := resourceMetric.Resource()
			ecs.TranslateResourceMetadata(resource)
			ecs.ApplyResourceConventions(resource)
			routing.EncodeDataStream(resource, routing.DataStreamTypeMetrics, p.cfg.ServiceNameInDataStreamDataset)
			if p.cfg.HostIPEnabled {
				ecs.SetHostIP(ctx, resource.Attributes())
			}

			// Check if resource has a service name for routing decisions
			hasServiceName := false
			if serviceName, ok := resource.Attributes().Get(routing.ServiceNameAttributeKey); ok && serviceName.Str() != "" {
				hasServiceName = true
			}

			// Route internal metrics to appropriate data streams if needed.
			routeMetricsToDataStream(resourceMetric.ScopeMetrics(), hasServiceName)
		}
	}
	// When skipEnrichment is true, only enrich when mapping mode is ecs
	// When skipEnrichment is false (default), always enrich (backwards compatible)
	if !p.cfg.SkipEnrichment || ecsMode {
		enricher.EnrichMetrics(md)
	}
	return p.next.ConsumeMetrics(ctx, md)
}

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

func (p *LogProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	enricher := p.enricher
	ecsMode := isECS(ctx)
	if ecsMode {
		enricher = p.ecsEnricher
		resourceLogs := ld.ResourceLogs()
		for i := 0; i < resourceLogs.Len(); i++ {
			resourceLog := resourceLogs.At(i)
			resource := resourceLog.Resource()
			ecs.TranslateResourceMetadata(resource)
			ecs.ApplyResourceConventions(resource)
			routing.EncodeDataStream(resource, routing.DataStreamTypeLogs, p.cfg.ServiceNameInDataStreamDataset)
			if p.cfg.HostIPEnabled {
				ecs.SetHostIP(ctx, resource.Attributes())
			}

			// Check each log record for error events and route to apm.error dataset
			// This follows the same logic as apm-data to detect error events
			scopeLogs := resourceLog.ScopeLogs()
			for j := 0; j < scopeLogs.Len(); j++ {
				logRecords := scopeLogs.At(j).LogRecords()
				for k := 0; k < logRecords.Len(); k++ {
					logRecord := logRecords.At(k)
					if routing.IsErrorEvent(logRecord.Attributes()) {
						// Override the resource-level data stream for error logs
						routing.EncodeErrorDataStream(logRecord.Attributes(), routing.DataStreamTypeLogs)
					}
				}
			}
		}
	}
	// When skipEnrichment is true, only enrich when mapping mode is ecs
	// When skipEnrichment is false (default), always enrich (backwards compatible)
	if !p.cfg.SkipEnrichment || ecsMode {
		enricher.EnrichLogs(ld)
	}
	return p.next.ConsumeLogs(ctx, ld)
}
