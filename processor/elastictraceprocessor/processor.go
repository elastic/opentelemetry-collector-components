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
	"github.com/elastic/opentelemetry-collector-components/processor/elastictraceprocessor/internal/index"
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
	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		resourceSpan := resourceSpans.At(i)
		resource := resourceSpan.Resource()
		ecs.TranslateResourceMetadata(resource)
		index.EncodeDataStream(resource, "traces")
	}
	p.enricher.Enrich(td)
	ecsCtx := client.NewContext(ctx, withMappingMode(client.FromContext(ctx), "ecs"))
	return p.next.ConsumeTraces(ecsCtx, td)
}

func withMappingMode(info client.Info, mode string) client.Info {
	return client.Info{
		Addr:     info.Addr,
		Auth:     info.Auth,
		Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {mode}}),
	}
}
