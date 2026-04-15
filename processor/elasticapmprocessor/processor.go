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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments"
)

var _ processor.Traces = (*TraceProcessor)(nil)
var _ processor.Metrics = (*MetricProcessor)(nil)
var _ processor.Logs = (*LogProcessor)(nil)

type TraceProcessor struct {
	component.StartFunc
	component.ShutdownFunc

	next            consumer.Traces
	defaultEnricher enrichments.TraceEnricher
	apmEnricher     enrichments.TraceEnricher
	otelEnricher    enrichments.TraceEnricher
	logger          *zap.Logger
	cfg             *Config
}

func NewTraceProcessor(cfg *Config, next consumer.Traces, logger *zap.Logger) *TraceProcessor {
	return &TraceProcessor{
		next:            next,
		logger:          logger,
		defaultEnricher: enrichments.NewDefaultTraceEnricher(cfg.Config),
		apmEnricher:     enrichments.NewAPMTraceEnricher(cfg.Config, cfg.HostIPEnabled),
		otelEnricher:    enrichments.NewOTelTraceEnricher(cfg.Config, cfg.HostIPEnabled),
		cfg:             cfg,
	}
}

func (p *TraceProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *TraceProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	ecsMode := isECSMappingMode(ctx)
	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		enricher := p.defaultEnricher
		if ecsMode {
			if isElasticAPMAgent(rs.Resource()) {
				enricher = p.apmEnricher
			} else {
				enricher = p.otelEnricher
			}
		}
		enricher.EnrichResourceSpans(ctx, rs)
	}
	return p.next.ConsumeTraces(ctx, td)
}

func isECSMappingMode(ctx context.Context) bool {
	clientCtx := client.FromContext(ctx)
	mappingMode := getMetadataValue(clientCtx)
	return mappingMode == "ecs"
}

// isElasticAPMAgent reports whether the data originated from an Elastic APM
// agent by checking telemetry.sdk.name == "ElasticAPM" on the resource
// attributes. The elasticapmintakereceiver always sets this value; OTLP
// events use their own SDK name.
func isElasticAPMAgent(resource pcommon.Resource) bool {
	if v, ok := resource.Attributes().Get(string(semconv.TelemetrySDKNameKey)); ok {
		return v.Str() == "ElasticAPM"
	}
	return false
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

	next            consumer.Logs
	defaultEnricher enrichments.LogEnricher
	apmEnricher     enrichments.LogEnricher
	otelEnricher    enrichments.LogEnricher
	logger          *zap.Logger
	cfg             *Config
}

func newLogProcessor(cfg *Config, next consumer.Logs, logger *zap.Logger) *LogProcessor {
	return &LogProcessor{
		next:            next,
		logger:          logger,
		defaultEnricher: enrichments.NewDefaultLogEnricher(cfg.Config),
		apmEnricher:     enrichments.NewAPMLogEnricher(cfg.Config, cfg.HostIPEnabled, cfg.ServiceNameInDataStreamDataset),
		otelEnricher:    enrichments.NewOTelLogEnricher(cfg.Config, cfg.HostIPEnabled, cfg.ServiceNameInDataStreamDataset),
		cfg:             cfg,
	}
}

func (p *LogProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

type MetricProcessor struct {
	component.StartFunc
	component.ShutdownFunc

	next            consumer.Metrics
	defaultEnricher enrichments.MetricEnricher
	apmEnricher     enrichments.MetricEnricher
	otelEnricher    enrichments.MetricEnricher
	logger          *zap.Logger
	cfg             *Config
}

func newMetricProcessor(cfg *Config, next consumer.Metrics, logger *zap.Logger) *MetricProcessor {
	return &MetricProcessor{
		next:            next,
		logger:          logger,
		defaultEnricher: enrichments.NewDefaultMetricEnricher(cfg.Config),
		apmEnricher:     enrichments.NewAPMMetricEnricher(cfg.Config, cfg.HostIPEnabled, cfg.ServiceNameInDataStreamDataset),
		otelEnricher:    enrichments.NewOTelMetricEnricher(cfg.Config, cfg.HostIPEnabled, cfg.ServiceNameInDataStreamDataset),
		cfg:             cfg,
	}
}

func (p *MetricProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *MetricProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	ecsMode := isECSMappingMode(ctx)
	if !ecsMode && p.cfg.SkipEnrichment {
		return p.next.ConsumeMetrics(ctx, md)
	}
	resourceMetrics := md.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		rm := resourceMetrics.At(i)
		enricher := p.defaultEnricher
		if ecsMode {
			if isElasticAPMAgent(rm.Resource()) {
				enricher = p.apmEnricher
			} else {
				enricher = p.otelEnricher
			}
		}
		enricher.EnrichResourceMetrics(ctx, rm)
	}
	return p.next.ConsumeMetrics(ctx, md)
}

func (p *LogProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	ecsMode := isECSMappingMode(ctx)
	if !ecsMode && p.cfg.SkipEnrichment {
		return p.next.ConsumeLogs(ctx, ld)
	}
	resourceLogs := ld.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		rl := resourceLogs.At(i)
		enricher := p.defaultEnricher
		if ecsMode {
			if isElasticAPMAgent(rl.Resource()) {
				enricher = p.apmEnricher
			} else {
				enricher = p.otelEnricher
			}
		}
		enricher.EnrichResourceLogs(ctx, rl)
	}
	return p.next.ConsumeLogs(ctx, ld)
}
