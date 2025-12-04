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

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/ecs"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"
	"github.com/elastic/opentelemetry-lib/enrichments"
)

var _ processor.Traces = (*TraceProcessor)(nil)
var _ processor.Metrics = (*MetricProcessor)(nil)
var _ processor.Logs = (*LogProcessor)(nil)

type TraceProcessor struct {
	component.StartFunc
	component.ShutdownFunc

	next     consumer.Traces
	enricher *enrichments.Enricher
	logger   *zap.Logger
	cfg      *Config
}

func NewTraceProcessor(cfg *Config, next consumer.Traces, logger *zap.Logger) *TraceProcessor {
	return &TraceProcessor{
		next:     next,
		logger:   logger,
		enricher: enrichments.NewEnricher(cfg.Config),
		cfg:      cfg,
	}
}

func (p *TraceProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *TraceProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	if isECS(ctx) {
		resourceSpans := td.ResourceSpans()
		for i := 0; i < resourceSpans.Len(); i++ {
			resourceSpan := resourceSpans.At(i)
			resource := resourceSpan.Resource()
			ecs.TranslateResourceMetadata(resource)
			routing.EncodeDataStream(resource, routing.DataStreamTypeTraces, false)
			if p.cfg.HostIPEnabled {
				ecs.SetHostIP(ctx, resource.Attributes())
			}
			// Traces signal never need to be routed to service-specific datasets
			p.enricher.Config.Resource.DeploymentEnvironment.Enabled = false
		}
	}

	p.enricher.EnrichTraces(td)

	return p.next.ConsumeTraces(ctx, td)
}

func isECS(ctx context.Context) bool {
	clientCtx := client.FromContext(ctx)
	mappingMode := getMetadataValue(clientCtx)
	return mappingMode == "ecs"
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

	next     consumer.Logs
	enricher *enrichments.Enricher
	logger   *zap.Logger
	cfg      *Config
}

func newLogProcessor(cfg *Config, next consumer.Logs, logger *zap.Logger) *LogProcessor {
	return &LogProcessor{
		next:     next,
		logger:   logger,
		enricher: enrichments.NewEnricher(cfg.Config),
		cfg:      cfg,
	}
}

func (p *LogProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

type MetricProcessor struct {
	component.StartFunc
	component.ShutdownFunc

	next     consumer.Metrics
	enricher *enrichments.Enricher
	logger   *zap.Logger
	cfg      *Config
}

func newMetricProcessor(cfg *Config, next consumer.Metrics, logger *zap.Logger) *MetricProcessor {
	return &MetricProcessor{
		next:     next,
		logger:   logger,
		enricher: enrichments.NewEnricher(cfg.Config),
		cfg:      cfg,
	}
}

func (p *MetricProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *MetricProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	ecsMode := isECS(ctx)
	if ecsMode {
		resourceMetrics := md.ResourceMetrics()
		for i := 0; i < resourceMetrics.Len(); i++ {
			resourceMetric := resourceMetrics.At(i)
			resource := resourceMetric.Resource()
			ecs.TranslateResourceMetadata(resource)
			if p.cfg.HostIPEnabled {
				ecs.SetHostIP(ctx, resource.Attributes())
			}
			routing.EncodeDataStream(resource, routing.DataStreamTypeMetrics, p.cfg.ServiceNameInDataStreamDataset)
			p.enricher.Config.Resource.DeploymentEnvironment.Enabled = false
		}
	}
	// When skipEnrichment is true, only enrich when mapping mode is ecs
	// When skipEnrichment is false (default), always enrich (backwards compatible)
	if !p.cfg.SkipEnrichment || ecsMode {
		p.enricher.EnrichMetrics(md)
	}
	return p.next.ConsumeMetrics(ctx, md)
}

func (p *LogProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	ecsMode := isECS(ctx)
	if ecsMode {
		resourceLogs := ld.ResourceLogs()
		for i := 0; i < resourceLogs.Len(); i++ {
			resourceLog := resourceLogs.At(i)
			resource := resourceLog.Resource()
			ecs.TranslateResourceMetadata(resource)
			if p.cfg.HostIPEnabled {
				ecs.SetHostIP(ctx, resource.Attributes())
			}
			routing.EncodeDataStream(resource, routing.DataStreamTypeLogs, p.cfg.ServiceNameInDataStreamDataset)
			p.enricher.Config.Resource.AgentVersion.Enabled = false
			p.enricher.Config.Resource.DeploymentEnvironment.Enabled = false
		}
	}
	// When skipEnrichment is true, only enrich when mapping mode is ecs
	// When skipEnrichment is false (default), always enrich (backwards compatible)
	if !p.cfg.SkipEnrichment || ecsMode {
		p.enricher.EnrichLogs(ld)
	}
	return p.next.ConsumeLogs(ctx, ld)
}
