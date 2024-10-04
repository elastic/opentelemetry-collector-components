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
	"fmt"
	"strconv"
	"time"

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/config"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/aggregator"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/model"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	ephemeralResourceKey = "spanmetricsv2_ephemeral_id"
)

// TODO (lahsivjar): will be removed after full ottl support
// metricUnitToDivider gives a value that could used to divide the
// nano precision duration to the required unit specified in config.
var metricUnitToDivider = map[config.MetricUnit]float64{
	config.MetricUnitNs: float64(time.Nanosecond.Nanoseconds()),
	config.MetricUnitUs: float64(time.Microsecond.Nanoseconds()),
	config.MetricUnitMs: float64(time.Millisecond.Nanoseconds()),
	config.MetricUnitS:  float64(time.Second.Nanoseconds()),
}

type signalToMetrics struct {
	component.StartFunc
	component.ShutdownFunc

	next           consumer.Metrics
	spanMetricDefs []model.MetricDef[ottlspan.TransformContext]
	dpMetricDefs   []model.MetricDef[ottldatapoint.TransformContext]
	logMetricDefs  []model.MetricDef[ottllog.TransformContext]

	// ephemeralID will be removed, see: https://github.com/elastic/opentelemetry-collector-components/issues/159
	ephemeralID string
}

func (sm *signalToMetrics) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (sm *signalToMetrics) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	var multiError, err error
	processedMetrics := pmetric.NewMetrics()
	processedMetrics.ResourceMetrics().EnsureCapacity(td.ResourceSpans().Len())
	aggregator := aggregator.NewAggregator[ottlspan.TransformContext](processedMetrics)

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpan := td.ResourceSpans().At(i)
		resourceAttrs := resourceSpan.Resource().Attributes()
		for j := 0; j < resourceSpan.ScopeSpans().Len(); j++ {
			scopeSpan := resourceSpan.ScopeSpans().At(j)
			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				span := scopeSpan.Spans().At(k)
				spanAttrs := span.Attributes()
				adjustedCount := calculateAdjustedCount(span.TraceState().AsRaw())
				for _, md := range sm.spanMetricDefs {
					filteredSpanAttrs := getFilteredAttributes(spanAttrs, md.Attributes)
					if filteredSpanAttrs.Len() != len(md.Attributes) {
						// If all the configured attributes are not present in
						// source metric then don't count them.
						continue
					}
					var filteredResAttrs pcommon.Map
					if len(md.IncludeResourceAttributes) > 0 {
						filteredResAttrs = getFilteredAttributes(resourceAttrs, md.IncludeResourceAttributes)
					} else {
						// Copy resource attrs to avoid mutating data
						filteredResAttrs = pcommon.NewMap()
						resourceAttrs.CopyTo(filteredResAttrs)
					}
					if md.EphemeralResourceAttribute {
						filteredResAttrs.PutStr(ephemeralResourceKey, sm.ephemeralID)
					}
					var value float64
					count := adjustedCount
					tCtx := ottlspan.NewTransformContext(span, scopeSpan.Scope(), resourceSpan.Resource(), scopeSpan, resourceSpan)
					// Count can be nil, but Value must be non-nil for spans
					if md.ValueCountMetric.CountStatement != nil {
						count, err = getCountFromOTTL(ctx, tCtx, md.ValueCountMetric)
						if err != nil {
							multiError = errors.Join(multiError, err)
							continue
						}
					}
					value, err = getValueFromOTTL(ctx, tCtx, md.ValueCountMetric)
					if err != nil {
						multiError = errors.Join(multiError, err)
						continue
					}
					multiError = errors.Join(
						multiError,
						aggregator.ValueCount(md, filteredResAttrs, filteredSpanAttrs, value, count),
						aggregator.Count(md, filteredResAttrs, filteredSpanAttrs, count),
					)
				}
			}
		}
	}
	if multiError != nil {
		return multiError
	}
	aggregator.Finalize(sm.spanMetricDefs)
	return sm.next.ConsumeMetrics(ctx, processedMetrics)
}

func (sm *signalToMetrics) ConsumeMetrics(ctx context.Context, m pmetric.Metrics) error {
	var multiError error
	processedMetrics := pmetric.NewMetrics()
	processedMetrics.ResourceMetrics().EnsureCapacity(m.ResourceMetrics().Len())
	aggregator := aggregator.NewAggregator[ottldatapoint.TransformContext](processedMetrics)
	for i := 0; i < m.ResourceMetrics().Len(); i++ {
		resourceMetric := m.ResourceMetrics().At(i)
		resourceAttrs := resourceMetric.Resource().Attributes()
		for j := 0; j < resourceMetric.ScopeMetrics().Len(); j++ {
			scopeMetric := resourceMetric.ScopeMetrics().At(j)
			for k := 0; k < scopeMetric.Metrics().Len(); k++ {
				metrics := scopeMetric.Metrics()
				metric := metrics.At(k)
				for _, md := range sm.dpMetricDefs {
					var filteredResAttrs pcommon.Map
					if len(md.IncludeResourceAttributes) > 0 {
						filteredResAttrs = getFilteredAttributes(resourceAttrs, md.IncludeResourceAttributes)
					} else {
						// Copy resource attrs to avoid mutating data
						filteredResAttrs = pcommon.NewMap()
						resourceAttrs.CopyTo(filteredResAttrs)
					}
					if md.EphemeralResourceAttribute {
						filteredResAttrs.PutStr(ephemeralResourceKey, sm.ephemeralID)
					}
					aggregate := func(dp any, dpAttrs pcommon.Map) error {
						tCtx := ottldatapoint.NewTransformContext(dp, metric, metrics, scopeMetric.Scope(), resourceMetric.Resource(), scopeMetric, resourceMetric)
						count := uint64(1)
						if md.ValueCountMetric.CountStatement != nil {
							var err error
							count, err = getCountFromOTTL(ctx, tCtx, md.ValueCountMetric)
							if err != nil {
								return err
							}
						}
						if err := aggregator.Count(md, filteredResAttrs, dpAttrs, count); err != nil {
							return err
						}
						if md.ValueCountMetric.ValueStatement != nil {
							value, err := getValueFromOTTL(ctx, tCtx, md.ValueCountMetric)
							if err != nil {
								return err
							}
							return aggregator.ValueCount(md, filteredResAttrs, dpAttrs, value, count)
						}
						return nil
					}
					//exhaustive:enforce
					switch metric.Type() {
					case pmetric.MetricTypeGauge:
						dps := metric.Gauge().DataPoints()
						for l := 0; l < dps.Len(); l++ {
							dp := dps.At(l)
							filteredDPAttrs := getFilteredAttributes(dp.Attributes(), md.Attributes)
							if filteredDPAttrs.Len() != len(md.Attributes) {
								// If all the configured attributes are not present in
								// source metric then don't count them.
								continue
							}
							multiError = errors.Join(multiError, aggregate(dp, filteredDPAttrs))
						}
					case pmetric.MetricTypeSum:
						dps := metric.Sum().DataPoints()
						for l := 0; l < dps.Len(); l++ {
							dp := dps.At(l)
							filteredDPAttrs := getFilteredAttributes(dp.Attributes(), md.Attributes)
							if filteredDPAttrs.Len() != len(md.Attributes) {
								// If all the configured attributes are not present in
								// source metric then don't count them.
								continue
							}
							multiError = errors.Join(multiError, aggregate(dp, filteredDPAttrs))
						}
					case pmetric.MetricTypeSummary:
						dps := metric.Summary().DataPoints()
						for l := 0; l < dps.Len(); l++ {
							dp := dps.At(l)
							filteredDPAttrs := getFilteredAttributes(dp.Attributes(), md.Attributes)
							if filteredDPAttrs.Len() != len(md.Attributes) {
								// If all the configured attributes are not present in
								// source metric then don't count them.
								continue
							}
							multiError = errors.Join(multiError, aggregate(dp, filteredDPAttrs))
						}
					case pmetric.MetricTypeHistogram:
						dps := metric.Histogram().DataPoints()
						for l := 0; l < dps.Len(); l++ {
							dp := dps.At(l)
							filteredDPAttrs := getFilteredAttributes(dp.Attributes(), md.Attributes)
							if filteredDPAttrs.Len() != len(md.Attributes) {
								// If all the configured attributes are not present in
								// source metric then don't count them.
								continue
							}
							multiError = errors.Join(multiError, aggregate(dp, filteredDPAttrs))
						}
					case pmetric.MetricTypeExponentialHistogram:
						dps := metric.ExponentialHistogram().DataPoints()
						for l := 0; l < dps.Len(); l++ {
							dp := dps.At(l)
							filteredDPAttrs := getFilteredAttributes(dp.Attributes(), md.Attributes)
							if filteredDPAttrs.Len() != len(md.Attributes) {
								// If all the configured attributes are not present in
								// source metric then don't count them.
								continue
							}
							multiError = errors.Join(multiError, aggregate(dp, filteredDPAttrs))
						}
					case pmetric.MetricTypeEmpty:
						multiError = errors.Join(multiError, fmt.Errorf("metric %q: invalid metric type: %v", metric.Name(), metric.Type()))
					}
				}
			}
		}
	}
	if multiError != nil {
		return multiError
	}
	aggregator.Finalize(sm.dpMetricDefs)
	return sm.next.ConsumeMetrics(ctx, processedMetrics)
}

func (sm *signalToMetrics) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	var multiError error
	processedMetrics := pmetric.NewMetrics()
	processedMetrics.ResourceMetrics().EnsureCapacity(logs.ResourceLogs().Len())
	aggregator := aggregator.NewAggregator[ottllog.TransformContext](processedMetrics)
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		resourceLog := logs.ResourceLogs().At(i)
		resourceAttrs := resourceLog.Resource().Attributes()
		for j := 0; j < resourceLog.ScopeLogs().Len(); j++ {
			scopeLog := resourceLog.ScopeLogs().At(j)
			for k := 0; k < scopeLog.LogRecords().Len(); k++ {
				log := scopeLog.LogRecords().At(k)
				logAttrs := log.Attributes()
				for _, md := range sm.logMetricDefs {
					filteredLogAttrs := getFilteredAttributes(logAttrs, md.Attributes)
					if filteredLogAttrs.Len() != len(md.Attributes) {
						// If all the configured attributes are not present in
						// source metric then don't count them.
						continue
					}
					var filteredResAttrs pcommon.Map
					if len(md.IncludeResourceAttributes) > 0 {
						filteredResAttrs = getFilteredAttributes(resourceAttrs, md.IncludeResourceAttributes)
					} else {
						// Copy resource attrs to avoid mutating data
						filteredResAttrs = pcommon.NewMap()
						resourceAttrs.CopyTo(filteredResAttrs)
					}
					if md.EphemeralResourceAttribute {
						filteredResAttrs.PutStr(ephemeralResourceKey, sm.ephemeralID)
					}
					count := uint64(1)
					tCtx := ottllog.NewTransformContext(log, scopeLog.Scope(), resourceLog.Resource(), scopeLog, resourceLog)
					if md.ValueCountMetric.CountStatement != nil {
						var err error
						count, err = getCountFromOTTL(ctx, tCtx, md.ValueCountMetric)
						if err != nil {
							multiError = errors.Join(multiError, err)
							continue
						}
					}
					multiError = errors.Join(multiError, aggregator.Count(md, filteredResAttrs, filteredLogAttrs, count))
					if md.ValueCountMetric.ValueStatement != nil {
						value, err := getValueFromOTTL(ctx, tCtx, md.ValueCountMetric)
						if err != nil {
							multiError = errors.Join(multiError, err)
							continue
						}
						multiError = errors.Join(multiError, aggregator.ValueCount(md, filteredResAttrs, filteredLogAttrs, value, count))
					}
				}
			}
		}
	}
	if multiError != nil {
		return multiError
	}
	aggregator.Finalize(sm.logMetricDefs)
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

func getFilteredAttributes(attrs pcommon.Map, filters []model.AttributeKeyValue) pcommon.Map {
	filteredAttrs := pcommon.NewMap()
	for _, filter := range filters {
		if attr, ok := attrs.Get(filter.Key); ok {
			attr.CopyTo(filteredAttrs.PutEmpty(filter.Key))
			continue
		}
		if filter.DefaultValue.Type() != pcommon.ValueTypeEmpty {
			filter.DefaultValue.CopyTo(filteredAttrs.PutEmpty(filter.Key))
		}
	}
	return filteredAttrs
}

func getCountFromOTTL[K any](
	ctx context.Context,
	tCtx K,
	md model.ValueCountMetric[K],
) (uint64, error) {
	raw, _, err := md.CountStatement.Execute(ctx, tCtx)
	if err != nil {
		return 0, err
	}
	rawInt64, ok := raw.(int64)
	if !ok {
		return 0, fmt.Errorf("failed to parse count OTTL statement value to int")
	}
	return uint64(rawInt64), nil
}

func getValueFromOTTL[K any](
	ctx context.Context,
	tCtx K,
	md model.ValueCountMetric[K],
) (float64, error) {
	raw, _, err := md.ValueStatement.Execute(ctx, tCtx)
	if err != nil {
		return 0, err
	}
	switch raw.(type) {
	case float64:
		return raw.(float64), nil
	case int64:
		return float64(raw.(int64)), nil
	case string:
		v, err := strconv.ParseFloat(raw.(string), 64)
		if err != nil {
			return 0, errors.New("failed to parse count OTTL statement value to float")
		}
		return v, nil
	}
	return 0, errors.New("failed to parse count OTTL statement value to float")
}
