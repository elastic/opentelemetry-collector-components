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
	"math"
	"strconv"
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
}

func (sm *spanMetrics) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (sm *spanMetrics) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	var multiError error
	processedMetrics := pmetric.NewMetrics()
	processedMetrics.ResourceMetrics().EnsureCapacity(td.ResourceSpans().Len())
	aggregator := aggregator.NewAggregator()
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		aggregator.Reset()
		resourceSpan := td.ResourceSpans().At(i)
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
					multiError = errors.Join(multiError, aggregator.Add(md, spanAttrs, duration, adjustedCount))
				}
			}
		}

		if aggregator.Empty() {
			continue // don't add an empty resource
		}

		processedResource := processedMetrics.ResourceMetrics().AppendEmpty()
		resourceSpan.Resource().Attributes().CopyTo(processedResource.Resource().Attributes())
		processedScope := processedResource.ScopeMetrics().AppendEmpty()
		processedScope.Scope().SetName(scopeName)
		destMetric := processedScope.Metrics()
		for _, md := range sm.metricDefs {
			aggregator.Move(md, destMetric)
		}
	}
	if multiError != nil {
		return multiError
	}
	return sm.next.ConsumeMetrics(ctx, processedMetrics)
}

// calculateAdjustedCount calculates the adjusted count which represents
// the number of spans in the population that are represented by the
// individually sampled span. If the span is not-sampled OR if a non-
// probability sampler is used then adjusted count defaults to 1.
// https://opentelemetry.io/docs/specs/otel/trace/tracestate-probability-sampling/#adjusted-count
func calculateAdjustedCount(tracestate string) uint64 {
	w3cTraceState, err := sampling.NewW3CTraceState(tracestate)
	if err != nil {
		return 1
	}
	otTraceState := w3cTraceState.OTelValue()
	if otTraceState == nil {
		return 1
	}
	// For probability sampler, calculate the adjusted count based on t-value (`th`).
	if len(otTraceState.TValue()) != 0 {
		// TODO (lahsivjar): Should we handle fractional adjusted count?
		// One way to do this would be to scale the values in the histograms
		// for some precision.
		return uint64(otTraceState.AdjustedCount())
	}
	// For consistent probablity sampler, calculate the adjusted count based on
	// p-value, negative base-2 logarithm of sampling probability
	var p uint64
	for _, kv := range otTraceState.ExtraValues() {
		if kv.Key == "p" {
			// If p-value is present then calculate the adjusted count as per
			// the consistent probability sampling specs.
			if kv.Value != "" {
				// p-value is represented as unsigned decimal integers
				// requiring at most 6 bits of information. We parse to
				// 7 bits as 63 holds a special meaning and thus needs
				// to be distinguished w.r.t. other invalid >63 values.
				p, _ = strconv.ParseUint(kv.Value, 10, 7)
			}
			break
		}
	}
	switch {
	case p == 0:
		return 1
	case p == 63:
		// p-value == 63 represents zero adjusted count
		return 0
	case p > 63:
		// Invalid value, default to 1
		return 1
	default:
		// TODO (lahsivjar): Should we handle fractional adjusted count?
		// One way to do this would be to scale the values in the histograms
		// for some precision.
		return uint64(math.Pow(2, float64(p)))
	}
}
