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

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/ecs"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"
	"github.com/elastic/opentelemetry-lib/enrichments"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
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
}

func newTraceProcessor(cfg *Config, next consumer.Traces, logger *zap.Logger) *TraceProcessor {
	return &TraceProcessor{
		next:     next,
		logger:   logger,
		enricher: enrichments.NewEnricher(cfg.Config),
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
			routing.EncodeDataStream(resource, "traces")
			// We expect that the following resource attributes are already present, added by the receiver.
			p.enricher.Config.Resource.AgentName.Enabled = false
			p.enricher.Config.Resource.AgentVersion.Enabled = false
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
}

func newLogProcessor(cfg *Config, next consumer.Logs, logger *zap.Logger) *LogProcessor {
	return &LogProcessor{
		next:     next,
		logger:   logger,
		enricher: enrichments.NewEnricher(cfg.Config),
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
}

func newMetricProcessor(cfg *Config, next consumer.Metrics, logger *zap.Logger) *MetricProcessor {
	return &MetricProcessor{
		next:     next,
		logger:   logger,
		enricher: enrichments.NewEnricher(cfg.Config),
	}
}

func (p *MetricProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *MetricProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	p.enricher.EnrichMetrics(md)
	return p.next.ConsumeMetrics(ctx, md)
}

func (p *LogProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	p.enricher.EnrichLogs(ld)
	return p.next.ConsumeLogs(ctx, ld)
}
