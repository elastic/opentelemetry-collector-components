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

// Package telemetry holds the OpenTelemetry instruments emitted by the
// streamlangprocessor. Names and attributes mirror the Rust streamlang-runtime
// so dashboards work identically across both implementations.
package telemetry // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/telemetry"

import (
	"context"

	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Outcome labels the result of a single per-document processor execution.
type Outcome string

const (
	OutcomeApplied Outcome = "applied"
	OutcomeSkipped Outcome = "skipped"
	OutcomeError   Outcome = "error"
	// OutcomeIgnored is recorded when the processor returned an error but the
	// processor's `ignore_failure` flag swallowed it.
	OutcomeIgnored Outcome = "ignored"
)

// Instruments groups the OTel instruments used by the executor.
type Instruments struct {
	tracer           trace.Tracer
	docsProcessed    metric.Int64Counter
	docsDropped      metric.Int64Counter
	docsRouted       metric.Int64Counter
	processorExec    metric.Int64Counter
	pipelineDuration metric.Float64Histogram
}

// New creates a new Instruments bound to the processor's MeterProvider /
// TracerProvider.
func New(set processor.Settings, scopeName string) (*Instruments, error) {
	meter := set.MeterProvider.Meter(scopeName)
	tracer := set.TracerProvider.Tracer(scopeName)

	docsProcessed, err := meter.Int64Counter(
		"streamlang.documents.processed",
		metric.WithDescription("Number of documents that entered the pipeline"),
	)
	if err != nil {
		return nil, err
	}
	docsDropped, err := meter.Int64Counter(
		"streamlang.documents.dropped",
		metric.WithDescription("Number of documents dropped by the pipeline"),
	)
	if err != nil {
		return nil, err
	}
	docsRouted, err := meter.Int64Counter(
		"streamlang.documents.routed",
		metric.WithDescription("Number of documents routed via stream.name"),
	)
	if err != nil {
		return nil, err
	}
	processorExec, err := meter.Int64Counter(
		"streamlang.processor.executions",
		metric.WithDescription("Number of processor executions, attributed by action and outcome"),
	)
	if err != nil {
		return nil, err
	}
	pipelineDuration, err := meter.Float64Histogram(
		"streamlang.pipeline.duration_ms",
		metric.WithDescription("Wall-clock duration of a pipeline batch execution in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	return &Instruments{
		tracer:           tracer,
		docsProcessed:    docsProcessed,
		docsDropped:      docsDropped,
		docsRouted:       docsRouted,
		processorExec:    processorExec,
		pipelineDuration: pipelineDuration,
	}, nil
}

// Tracer returns the bound Tracer (used by the executor for batch spans).
func (i *Instruments) Tracer() trace.Tracer { return i.tracer }

// RecordDocumentsProcessed records that n documents entered the pipeline.
func (i *Instruments) RecordDocumentsProcessed(ctx context.Context, n int64, tenantID string) {
	i.docsProcessed.Add(ctx, n, metric.WithAttributes(tenantAttr(tenantID)...))
}

// RecordDocumentsDropped records that n documents were dropped from the batch.
func (i *Instruments) RecordDocumentsDropped(ctx context.Context, n int64, tenantID string) {
	i.docsDropped.Add(ctx, n, metric.WithAttributes(tenantAttr(tenantID)...))
}

// RecordPipelineDuration records the wall-clock duration of a batch run.
func (i *Instruments) RecordPipelineDuration(ctx context.Context, ms float64, tenantID string) {
	i.pipelineDuration.Record(ctx, ms, metric.WithAttributes(tenantAttr(tenantID)...))
}

// RecordProcessorOutcome increments the processor.executions counter for one
// (action, outcome) pair by the given count.
func (i *Instruments) RecordProcessorOutcome(ctx context.Context, action string, outcome Outcome, count int64, tenantID string) {
	if count == 0 {
		return
	}
	attrs := tenantAttr(tenantID)
	attrs = append(attrs,
		attribute.String("action", action),
		attribute.String("outcome", string(outcome)),
	)
	i.processorExec.Add(ctx, count, metric.WithAttributes(attrs...))
}

func tenantAttr(tenantID string) []attribute.KeyValue {
	if tenantID == "" {
		return nil
	}
	return []attribute.KeyValue{attribute.String("tenant.id", tenantID)}
}
