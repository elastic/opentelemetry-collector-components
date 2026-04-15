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

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"
)

// TraceEnricher enriches a single ResourceSpans for a specific mapping mode.
type TraceEnricher interface {
	EnrichResourceSpans(ctx context.Context, rs ptrace.ResourceSpans)
}

// ecsTraceEnricher contains the shared ECS trace enrichment pipeline
// embedded by APMTraceEnricher and OTelTraceEnricher.
type ecsTraceEnricher struct {
	enricher      *Enricher
	hostIPEnabled bool
}

func (e *ecsTraceEnricher) enrichResourceSpans(ctx context.Context, rs ptrace.ResourceSpans) {
	// Traces signal never need to be routed to service-specific datasets
	ecsPreProcessResource(ctx, rs.Resource(), routing.DataStreamTypeTraces, false, e.hostIPEnabled)
	routeErrorSpanEvents(rs)
	e.enricher.EnrichResourceSpans(rs)
}

// routeErrorSpanEvents iterates through spans to find errors in span
// events and overrides the resource-level data stream for error events.
func routeErrorSpanEvents(rs ptrace.ResourceSpans) {
	scopeSpans := rs.ScopeSpans()
	for j := 0; j < scopeSpans.Len(); j++ {
		spans := scopeSpans.At(j).Spans()
		for k := 0; k < spans.Len(); k++ {
			events := spans.At(k).Events()
			for l := 0; l < events.Len(); l++ {
				event := events.At(l)
				if routing.IsErrorEvent(event.Attributes()) {
					routing.EncodeErrorDataStream(event.Attributes(), routing.DataStreamTypeTraces)
				}
			}
		}
	}
}

// APMTraceEnricher handles elastic APM intake events in ECS mode.
// It applies the shared ECS pre-processing pipeline with intake-specific
// enrichment config (e.g. skips HostOSType, DefaultServiceLanguage,
// TranslateUnsupportedAttributes, and TransactionResult since the
// intake receiver already provides these).
type APMTraceEnricher struct {
	ecsTraceEnricher
}

func (e *APMTraceEnricher) EnrichResourceSpans(ctx context.Context, rs ptrace.ResourceSpans) {
	e.enrichResourceSpans(ctx, rs)
}

// NewAPMTraceEnricher creates a TraceEnricher for elastic APM intake events.
func NewAPMTraceEnricher(baseCfg config.Config, hostIPEnabled bool) *APMTraceEnricher {
	cfg := ecsAPMConfig(baseCfg)
	// The intake receiver already sets transaction.result; skip re-deriving it
	// to avoid overwriting the intake-supplied value or avoid deriving a value
	// when the provided transaction.result is empty to match existing apm-data logic.
	cfg.Transaction.Result.Enabled = false
	return &APMTraceEnricher{
		ecsTraceEnricher: ecsTraceEnricher{
			enricher:      NewEnricher(cfg),
			hostIPEnabled: hostIPEnabled,
		},
	}
}

// OTelTraceEnricher handles elastic OTel events in ECS mode.
// It applies the shared ECS pre-processing pipeline with OTel-specific
// enrichment config (enables HostOSType, DefaultServiceLanguage, and
// TranslateUnsupportedAttributes which are not set by the OTLP receiver).
type OTelTraceEnricher struct {
	ecsTraceEnricher
}

func (e *OTelTraceEnricher) EnrichResourceSpans(ctx context.Context, rs ptrace.ResourceSpans) {
	e.enrichResourceSpans(ctx, rs)
}

// NewOTelTraceEnricher creates a TraceEnricher for elastic OTel events.
func NewOTelTraceEnricher(baseCfg config.Config, hostIPEnabled bool) *OTelTraceEnricher {
	cfg := ecsOTelConfig(baseCfg)
	cfg.Span.TranslateUnsupportedAttributes.Enabled = true
	return &OTelTraceEnricher{
		ecsTraceEnricher: ecsTraceEnricher{
			enricher:      NewEnricher(cfg),
			hostIPEnabled: hostIPEnabled,
		},
	}
}

// DefaultTraceEnricher handles non-ECS trace events.
// It applies base enrichment without ECS translation or data stream routing.
type DefaultTraceEnricher struct {
	enricher *Enricher
}

func (e *DefaultTraceEnricher) EnrichResourceSpans(_ context.Context, rs ptrace.ResourceSpans) {
	e.enricher.EnrichResourceSpans(rs)
}

// NewDefaultTraceEnricher creates a TraceEnricher for non-ECS trace events.
func NewDefaultTraceEnricher(baseCfg config.Config) *DefaultTraceEnricher {
	return &DefaultTraceEnricher{
		enricher: NewEnricher(baseCfg),
	}
}
