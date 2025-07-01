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

package elastictraceprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/elastictraceprocessor"

import (
	"context"

	"github.com/elastic/opentelemetry-collector-components/processor/elastictraceprocessor/internal/ecs"
	"github.com/elastic/opentelemetry-collector-components/processor/elastictraceprocessor/internal/routing"
	"github.com/elastic/opentelemetry-lib/enrichments/trace"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

var _ processor.Traces = (*Processor)(nil)

type Processor struct {
	component.StartFunc
	component.ShutdownFunc

	next     consumer.Traces
	enricher *trace.Enricher
	logger   *zap.Logger
}

func newProcessor(cfg *Config, next consumer.Traces, logger *zap.Logger) *Processor {
	return &Processor{
		next:     next,
		logger:   logger,
		enricher: trace.NewEnricher(cfg.Config),
	}
}

func (p *Processor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *Processor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
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

	p.enricher.Enrich(td)

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
