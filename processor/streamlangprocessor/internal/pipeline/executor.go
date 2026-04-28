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

package pipeline // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/pipeline"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/condition"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/processors"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/telemetry"
)

// FailureMode mirrors streamlangprocessor.FailureMode without an import cycle.
type FailureMode int

const (
	// FailureModeDrop drops the failing document and continues with the rest.
	FailureModeDrop FailureMode = iota
	// FailureModePropagate returns the first per-document error to the caller.
	FailureModePropagate
)

// BatchStats summarises the outcome of an ExecuteBatch call.
type BatchStats struct {
	Input   int
	Dropped int
}

// ExecuteBatch runs the compiled pipeline against each document in docs.
//
// Per-document error isolation matches Elasticsearch ingest pipelines:
//   - A processor failure on a doc terminates that doc only (it's marked
//     dropped via Document.Drop()); other docs continue.
//   - processors.SkipError indicates the processor opted out for that doc and
//     the next processor proceeds normally.
//   - A processor with `ignore_failure: true` swallows real errors; the doc
//     keeps flowing through subsequent processors.
//
// The executor never aborts a whole batch on one doc's error.
func (c *Compiled) ExecuteBatch(
	ctx context.Context,
	docs []document.Document,
	instr *telemetry.Instruments,
	mode FailureMode,
	tenantID string,
) (BatchStats, error) {
	stats := BatchStats{Input: len(docs)}
	if len(docs) == 0 || len(c.steps) == 0 {
		return stats, nil
	}
	start := time.Now()

	// Per-batch span — gives APM a single waterfall row per Consume call
	// with per-action applied/skipped/error/ignored counts attached as
	// attributes. Querying for `streamlang.batch.<action>.applied > 0` is
	// the cheap "did this action actually run" signal.
	var span trace.Span
	if instr != nil {
		ctx, span = instr.Tracer().Start(ctx, "streamlang.pipeline.execute_batch",
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithAttributes(
				attribute.String("tenant.id", tenantID),
				attribute.Int("streamlang.batch.input_count", len(docs)),
				attribute.Int("streamlang.pipeline.steps", len(c.steps)),
			),
		)
	}

	defer func() {
		if instr != nil {
			ms := float64(time.Since(start).Microseconds()) / 1000.0
			instr.RecordPipelineDuration(ctx, ms, tenantID)
		}
		if span != nil {
			span.SetAttributes(
				attribute.Int("streamlang.batch.dropped_count", stats.Dropped),
				attribute.Float64("streamlang.batch.duration_ms",
					float64(time.Since(start).Microseconds())/1000.0),
			)
			span.End()
		}
	}()

	if instr != nil {
		instr.RecordDocumentsProcessed(ctx, int64(len(docs)), tenantID)
	}

	// Per-action aggregate counters (one batch span equivalent).
	type acc struct{ applied, skipped, errored, ignored int64 }
	totals := make([]acc, len(c.steps))

	var firstErr error
	for _, doc := range docs {
		if doc.IsDropped() {
			continue
		}
		for i, step := range c.steps {
			if doc.IsDropped() {
				break
			}
			if step.where != nil && !step.where(doc) {
				totals[i].skipped++
				continue
			}
			err := step.proc.Execute(doc)
			switch {
			case err == nil:
				totals[i].applied++
			case processors.IsSkip(err):
				totals[i].skipped++
			case step.proc.IgnoreFailure():
				totals[i].ignored++
			default:
				totals[i].errored++
				if mode == FailureModePropagate && firstErr == nil {
					firstErr = err
				}
				doc.Drop()
			}
		}
	}

	for i, step := range c.steps {
		action := step.proc.Action()
		if instr != nil {
			instr.RecordProcessorOutcome(ctx, action, telemetry.OutcomeApplied, totals[i].applied, tenantID)
			instr.RecordProcessorOutcome(ctx, action, telemetry.OutcomeSkipped, totals[i].skipped, tenantID)
			instr.RecordProcessorOutcome(ctx, action, telemetry.OutcomeError, totals[i].errored, tenantID)
			instr.RecordProcessorOutcome(ctx, action, telemetry.OutcomeIgnored, totals[i].ignored, tenantID)
		}
		if span != nil {
			// Per-action counts: only tag the batch span when the action
			// actually fired (applied or errored). Pure-skip steps would
			// otherwise dominate every span's attribute set with zeros.
			if totals[i].applied > 0 {
				span.SetAttributes(attribute.Int(
					fmt.Sprintf("streamlang.batch.%s.applied", action),
					int(totals[i].applied)))
			}
			if totals[i].skipped > 0 {
				span.SetAttributes(attribute.Int(
					fmt.Sprintf("streamlang.batch.%s.skipped", action),
					int(totals[i].skipped)))
			}
			if totals[i].errored > 0 {
				span.SetAttributes(attribute.Int(
					fmt.Sprintf("streamlang.batch.%s.errored", action),
					int(totals[i].errored)))
			}
			if totals[i].ignored > 0 {
				span.SetAttributes(attribute.Int(
					fmt.Sprintf("streamlang.batch.%s.ignored", action),
					int(totals[i].ignored)))
			}
		}
	}

	for _, doc := range docs {
		if doc.IsDropped() {
			stats.Dropped++
		}
	}
	if instr != nil && stats.Dropped > 0 {
		instr.RecordDocumentsDropped(ctx, int64(stats.Dropped), tenantID)
	}

	return stats, firstErr
}

// document.Document satisfies condition.Source structurally via the Field
// method; no adapter struct is required, so the executor passes the
// Document directly to compiled conditions on the hot path.
var _ condition.Source = (document.Document)(nil)
