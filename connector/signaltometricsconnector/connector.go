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

package signaltometricsconnector // import "github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector"

import (
	"context"
	"errors"
	"fmt"

	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/aggregator"
	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/model"
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
	"go.uber.org/zap"
)

type signalToMetrics struct {
	component.StartFunc
	component.ShutdownFunc

	logger *zap.Logger

	next           consumer.Metrics
	spanMetricDefs []model.MetricDef[ottlspan.TransformContext]
	dpMetricDefs   []model.MetricDef[ottldatapoint.TransformContext]
	logMetricDefs  []model.MetricDef[ottllog.TransformContext]
}

func (sm *signalToMetrics) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (sm *signalToMetrics) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	if len(sm.spanMetricDefs) == 0 {
		return nil
	}

	var multiError error
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
						// If any of the configured attributes is not present in
						// source metric then don't count them.
						continue
					}

					// The transform context is created from orginal attributes so that the
					// OTTL expressions are also applied on the original attributes.
					tCtx := ottlspan.NewTransformContext(span, scopeSpan.Scope(), resourceSpan.Resource(), scopeSpan, resourceSpan)
					if md.Conditions != nil {
						match, err := md.Conditions.Eval(ctx, tCtx)
						if err != nil {
							multiError = errors.Join(multiError, fmt.Errorf("failed to evaluate conditions, skipping: %w", err))
							continue
						}
						if !match {
							sm.logger.Debug("condition not matched, skipping", zap.String("name", md.Key.Name))
							continue
						}
					}

					var filteredResAttrs pcommon.Map
					if len(md.IncludeResourceAttributes) > 0 {
						filteredResAttrs = getFilteredAttributes(resourceAttrs, md.IncludeResourceAttributes)
					} else {
						// Copy resource attrs to avoid mutating data
						filteredResAttrs = pcommon.NewMap()
						resourceAttrs.CopyTo(filteredResAttrs)
					}
					multiError = errors.Join(multiError, aggregator.Aggregate(ctx, tCtx, md, filteredResAttrs, filteredSpanAttrs, adjustedCount))
				}
			}
		}
	}
	aggregator.Finalize(sm.spanMetricDefs)
	return sm.processNext(ctx, processedMetrics, multiError)
}

func (sm *signalToMetrics) ConsumeMetrics(ctx context.Context, m pmetric.Metrics) error {
	if len(sm.dpMetricDefs) == 0 {
		return nil
	}

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

					aggregate := func(dp any, dpAttrs pcommon.Map) error {
						// The transform context is created from orginal attributes so that the
						// OTTL expressions are also applied on the original attributes.
						tCtx := ottldatapoint.NewTransformContext(dp, metric, metrics, scopeMetric.Scope(), resourceMetric.Resource(), scopeMetric, resourceMetric)
						if md.Conditions != nil {
							match, err := md.Conditions.Eval(ctx, tCtx)
							if err != nil {
								multiError = errors.Join(multiError, fmt.Errorf("failed to evaluate conditions, skipping: %w", err))
								return nil
							}
							if !match {
								sm.logger.Debug("condition not matched, skipping", zap.String("name", md.Key.Name))
								return nil
							}
						}
						return aggregator.Aggregate(ctx, tCtx, md, filteredResAttrs, dpAttrs, 1)
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
	aggregator.Finalize(sm.dpMetricDefs)
	return sm.processNext(ctx, processedMetrics, multiError)
}

func (sm *signalToMetrics) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	if len(sm.logMetricDefs) == 0 {
		return nil
	}

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

					// The transform context is created from orginal attributes so that the
					// OTTL expressions are also applied on the original attributes.
					tCtx := ottllog.NewTransformContext(log, scopeLog.Scope(), resourceLog.Resource(), scopeLog, resourceLog)
					if md.Conditions != nil {
						match, err := md.Conditions.Eval(ctx, tCtx)
						if err != nil {
							multiError = errors.Join(multiError, fmt.Errorf("failed to evaluate conditions, skipping: %w", err))
							continue
						}
						if !match {
							sm.logger.Debug("condition not matched, skipping", zap.String("name", md.Key.Name))
							continue
						}
					}

					var filteredResAttrs pcommon.Map
					if len(md.IncludeResourceAttributes) > 0 {
						filteredResAttrs = getFilteredAttributes(resourceAttrs, md.IncludeResourceAttributes)
					} else {
						// Copy resource attrs to avoid mutating data
						filteredResAttrs = pcommon.NewMap()
						resourceAttrs.CopyTo(filteredResAttrs)
					}
					multiError = errors.Join(multiError, aggregator.Aggregate(ctx, tCtx, md, filteredResAttrs, filteredLogAttrs, 1))
				}
			}
		}
	}
	aggregator.Finalize(sm.logMetricDefs)
	return sm.processNext(ctx, processedMetrics, multiError)
}

// processNext is a helper method for all the Consume* methods to do error handling,
// logging, and sending the processed metrics to the next consumer in the pipeline.
func (sm *signalToMetrics) processNext(ctx context.Context, m pmetric.Metrics, err error) error {
	if err != nil {
		dpCount := m.DataPointCount()
		if dpCount == 0 {
			// No signals were consumed so return an error
			return fmt.Errorf("failed to consume signal: %w", err)
		}
		// At least some signals were partially consumed, so log the error
		// and pass the processed metrics to the next consumer.
		sm.logger.Warn(
			"failed to consume all signals, some signals were partially processed",
			zap.Error(err),
			zap.Int("successful_data_points", dpCount),
		)
	}
	return sm.next.ConsumeMetrics(ctx, m)
}

// calculateAdjustedCount calculates the adjusted count which represents
// the number of spans in the population that are represented by the
// individually sampled span. If the span is not-sampled OR if a non-
// probability sampler is used then adjusted count defaults to 1.
// https://github.com/open-telemetry/oteps/blob/main/text/trace/0235-sampling-threshold-in-trace-state.md
func calculateAdjustedCount(tracestate string) int64 {
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
	return int64(otTraceState.AdjustedCount())
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
