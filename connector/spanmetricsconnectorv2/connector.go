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

package spanmetricsconnectorv2 // import "github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2"

import (
	"context"
	"errors"
	"time"

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/aggregator"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/model"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const scopeName = "otelcol/spanmetricsconnectorv2"

type spanMetrics struct {
	component.StartFunc
	component.ShutdownFunc

	next       consumer.Metrics
	metricDefs []model.MetricDef

	ephemeralID string
}

func (sm *spanMetrics) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (sm *spanMetrics) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	var multiError error
	processedMetrics := pmetric.NewMetrics()
	processedMetrics.ResourceMetrics().EnsureCapacity(td.ResourceSpans().Len())
	aggregator := aggregator.NewAggregator(processedMetrics, sm.ephemeralID)
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpan := td.ResourceSpans().At(i)
		resourceAttrs := resourceSpan.Resource().Attributes()
		for j := 0; j < resourceSpan.ScopeSpans().Len(); j++ {
			scopeSpan := resourceSpan.ScopeSpans().At(j)

			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				span := scopeSpan.Spans().At(k)
				var duration time.Duration
				startTime := span.StartTimestamp()
				endTime := span.EndTimestamp()
				if endTime > startTime {
					duration = time.Duration(endTime - startTime)
				}
				spanAttrs := span.Attributes()
				adjustedCount := calculateAdjustedCount(span.TraceState().AsRaw())
				for _, md := range sm.metricDefs {
					multiError = errors.Join(
						multiError,
						aggregator.Add(md, resourceAttrs, spanAttrs, duration, adjustedCount),
					)
				}
			}
		}
	}
	if multiError != nil {
		return multiError
	}
	aggregator.Finalize(sm.metricDefs)
	return sm.next.ConsumeMetrics(ctx, processedMetrics)
}

// calculateAdjustedCount calculates the adjusted count which represents
// the number of spans in the population that are represented by the
// individually sampled span. If the span is not-sampled OR if a non-
// probability sampler is used then adjusted count defaults to 1.
// https://github.com/open-telemetry/oteps/blob/main/text/trace/0235-sampling-threshold-in-trace-state.md
func calculateAdjustedCount(tracestate string) uint64 {
	w3cTraceState, err := sampling.NewW3CTraceState(tracestate)
	if err != nil {
		return 1
	}
	otTraceState := w3cTraceState.OTelValue()
	if otTraceState == nil {
		return 1
	}
	if len(otTraceState.TValue()) == 0 {
		// For non-probabilistic sampler OR always sampling threshold, default to 1
		return 1
	}
	// TODO (lahsivjar): Handle fractional adjusted count. One way to do this
	// would be to scale the values in the histograms for some precision.
	return uint64(otTraceState.AdjustedCount())
}
