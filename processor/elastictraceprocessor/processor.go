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

	"github.com/elastic/opentelemetry-lib/enrichments/trace"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

var _ processor.Traces = (*Processor)(nil)

type Processor struct {
	next   consumer.Traces
	logger *zap.Logger
}

func newProcessor(logger *zap.Logger, next consumer.Traces) *Processor {
	return &Processor{
		next:   next,
		logger: logger,
	}
}

func (p *Processor) Start(context.Context, component.Host) error {
	return nil
}

func (p *Processor) Shutdown(context.Context) error {
	return nil
}

func (p *Processor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *Processor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	trace.Enrich(td)
	return p.next.ConsumeTraces(ctx, td)
}
