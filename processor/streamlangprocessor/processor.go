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

package streamlangprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor"

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"gopkg.in/yaml.v3"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/pipeline"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/telemetry"
)

// tenantHeader is the metadata key the proxy / ingest collector uses to
// identify which tenant a request belongs to (see CLAUDE.md).
const tenantHeader = "x-elastic-project-id"

type baseProcessor struct {
	cfg      *Config
	set      processor.Settings
	compiled *pipeline.Compiled
	instr    *telemetry.Instruments
	failMode pipeline.FailureMode
}

func newBase(set processor.Settings, cfg *Config) (baseProcessor, error) {
	d, err := loadDSL(cfg)
	if err != nil {
		return baseProcessor{}, fmt.Errorf("streamlangprocessor: load DSL: %w", err)
	}
	compiled, err := pipeline.Compile(d)
	if err != nil {
		return baseProcessor{}, fmt.Errorf("streamlangprocessor: compile pipeline: %w", err)
	}
	instr, err := telemetry.New(set, metadata.ScopeName)
	if err != nil {
		return baseProcessor{}, fmt.Errorf("streamlangprocessor: build instruments: %w", err)
	}
	mode := pipeline.FailureModeDrop
	if cfg.FailureMode == FailureModePropagate {
		mode = pipeline.FailureModePropagate
	}
	return baseProcessor{
		cfg:      cfg,
		set:      set,
		compiled: compiled,
		instr:    instr,
		failMode: mode,
	}, nil
}

func loadDSL(cfg *Config) (dsl.DSL, error) {
	if cfg.Path != "" {
		b, err := os.ReadFile(cfg.Path)
		if err != nil {
			return dsl.DSL{}, fmt.Errorf("read %q: %w", cfg.Path, err)
		}
		return dsl.ParseYAML(b)
	}
	// Inline steps: feed through the YAML round-trip so ParseYAML's
	// map[any]any normalisation handles any values mapstructure left as
	// non-string-keyed.
	wrapped := map[string]any{"steps": cfg.Steps}
	b, err := yaml.Marshal(wrapped)
	if err != nil {
		return dsl.DSL{}, fmt.Errorf("re-marshal steps: %w", err)
	}
	return dsl.ParseYAML(b)
}

// tenantIDFromContext returns the tenant ID from client metadata, or "".
func tenantIDFromContext(ctx context.Context) string {
	info := client.FromContext(ctx)
	if vs := info.Metadata.Get(tenantHeader); len(vs) > 0 {
		return vs[0]
	}
	return ""
}

// --- Logs ----------------------------------------------------------------

type logsProcessor struct {
	baseProcessor
	next consumer.Logs
}

func newLogsProcessor(set processor.Settings, cfg *Config, next consumer.Logs) (*logsProcessor, error) {
	b, err := newBase(set, cfg)
	if err != nil {
		return nil, err
	}
	return &logsProcessor{baseProcessor: b, next: next}, nil
}

func (p *logsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *logsProcessor) Start(_ context.Context, _ component.Host) error { return nil }
func (p *logsProcessor) Shutdown(_ context.Context) error                { return nil }

func (p *logsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	if !p.cfg.Logs.IsEnabled() || p.compiled.Steps() == 0 {
		return p.next.ConsumeLogs(ctx, ld)
	}

	// Build one Document per LogRecord. We index back into this slice during
	// pruning, in lockstep with the iteration order.
	rls := ld.ResourceLogs()
	docs := make([]document.Document, 0, ld.LogRecordCount())
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			lrs := sl.LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				docs = append(docs, document.NewLogDocument(rl, sl, lrs.At(k)))
			}
		}
	}

	tenantID := tenantIDFromContext(ctx)
	if _, err := p.compiled.ExecuteBatch(ctx, docs, p.instr, p.failMode, tenantID); err != nil {
		return err
	}

	// Prune any dropped records. Iterate parents and use RemoveIf so dropped
	// records vanish from the wire.
	idx := 0
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			lrs := sl.LogRecords()
			lrs.RemoveIf(func(_ plog.LogRecord) bool {
				dropped := docs[idx].IsDropped()
				idx++
				return dropped
			})
		}
		// Drop empty Scope/Resource containers to keep the output tidy.
		sls.RemoveIf(func(s plog.ScopeLogs) bool { return s.LogRecords().Len() == 0 })
	}
	rls.RemoveIf(func(r plog.ResourceLogs) bool { return r.ScopeLogs().Len() == 0 })

	if ld.LogRecordCount() == 0 {
		return nil
	}
	return p.next.ConsumeLogs(ctx, ld)
}

// --- Metrics (Phase 1: pass-through) -------------------------------------

type metricsProcessor struct {
	baseProcessor
	next consumer.Metrics
}

func newMetricsProcessor(set processor.Settings, cfg *Config, next consumer.Metrics) (*metricsProcessor, error) {
	b, err := newBase(set, cfg)
	if err != nil {
		return nil, err
	}
	return &metricsProcessor{baseProcessor: b, next: next}, nil
}

func (p *metricsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *metricsProcessor) Start(_ context.Context, _ component.Host) error { return nil }
func (p *metricsProcessor) Shutdown(_ context.Context) error                { return nil }

func (p *metricsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if !p.cfg.Metrics.IsEnabled() || p.compiled.Steps() == 0 {
		return p.next.ConsumeMetrics(ctx, md)
	}

	// Build one Document per data point. We index back into this slice during
	// pruning, in lockstep with the iteration order.
	rms := md.ResourceMetrics()
	docs := make([]document.Document, 0, md.DataPointCount())
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					n := dps.Len()
					for d := 0; d < n; d++ {
						docs = append(docs, document.NewNumberMetricDocument(rm, sm, m, dps.At(d), n))
					}
				case pmetric.MetricTypeSum:
					dps := m.Sum().DataPoints()
					n := dps.Len()
					for d := 0; d < n; d++ {
						docs = append(docs, document.NewNumberMetricDocument(rm, sm, m, dps.At(d), n))
					}
				case pmetric.MetricTypeHistogram:
					dps := m.Histogram().DataPoints()
					n := dps.Len()
					for d := 0; d < n; d++ {
						docs = append(docs, document.NewHistogramMetricDocument(rm, sm, m, dps.At(d), n))
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := m.ExponentialHistogram().DataPoints()
					n := dps.Len()
					for d := 0; d < n; d++ {
						docs = append(docs, document.NewExponentialHistogramMetricDocument(rm, sm, m, dps.At(d), n))
					}
				case pmetric.MetricTypeSummary:
					dps := m.Summary().DataPoints()
					n := dps.Len()
					for d := 0; d < n; d++ {
						docs = append(docs, document.NewSummaryMetricDocument(rm, sm, m, dps.At(d), n))
					}
				}
			}
		}
	}

	tenantID := tenantIDFromContext(ctx)
	if _, err := p.compiled.ExecuteBatch(ctx, docs, p.instr, p.failMode, tenantID); err != nil {
		return err
	}

	// Prune dropped data points. The idx counter advances in lockstep with
	// the original iteration order (Resource → Scope → Metric → typed-DPs).
	idx := 0
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					m.Gauge().DataPoints().RemoveIf(func(_ pmetric.NumberDataPoint) bool {
						dropped := docs[idx].IsDropped()
						idx++
						return dropped
					})
				case pmetric.MetricTypeSum:
					m.Sum().DataPoints().RemoveIf(func(_ pmetric.NumberDataPoint) bool {
						dropped := docs[idx].IsDropped()
						idx++
						return dropped
					})
				case pmetric.MetricTypeHistogram:
					m.Histogram().DataPoints().RemoveIf(func(_ pmetric.HistogramDataPoint) bool {
						dropped := docs[idx].IsDropped()
						idx++
						return dropped
					})
				case pmetric.MetricTypeExponentialHistogram:
					m.ExponentialHistogram().DataPoints().RemoveIf(func(_ pmetric.ExponentialHistogramDataPoint) bool {
						dropped := docs[idx].IsDropped()
						idx++
						return dropped
					})
				case pmetric.MetricTypeSummary:
					m.Summary().DataPoints().RemoveIf(func(_ pmetric.SummaryDataPoint) bool {
						dropped := docs[idx].IsDropped()
						idx++
						return dropped
					})
				}
			}
			// Drop metrics whose DP slice is empty after pruning.
			ms.RemoveIf(metricHasNoDataPoints)
		}
		sms.RemoveIf(func(s pmetric.ScopeMetrics) bool { return s.Metrics().Len() == 0 })
	}
	rms.RemoveIf(func(r pmetric.ResourceMetrics) bool { return r.ScopeMetrics().Len() == 0 })

	if md.DataPointCount() == 0 {
		return nil
	}
	return p.next.ConsumeMetrics(ctx, md)
}

func metricHasNoDataPoints(m pmetric.Metric) bool {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints().Len() == 0
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints().Len() == 0
	case pmetric.MetricTypeHistogram:
		return m.Histogram().DataPoints().Len() == 0
	case pmetric.MetricTypeExponentialHistogram:
		return m.ExponentialHistogram().DataPoints().Len() == 0
	case pmetric.MetricTypeSummary:
		return m.Summary().DataPoints().Len() == 0
	}
	return true
}

// --- Traces (Phase 1: pass-through) --------------------------------------

type tracesProcessor struct {
	baseProcessor
	next consumer.Traces
}

func newTracesProcessor(set processor.Settings, cfg *Config, next consumer.Traces) (*tracesProcessor, error) {
	b, err := newBase(set, cfg)
	if err != nil {
		return nil, err
	}
	return &tracesProcessor{baseProcessor: b, next: next}, nil
}

func (p *tracesProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *tracesProcessor) Start(_ context.Context, _ component.Host) error { return nil }
func (p *tracesProcessor) Shutdown(_ context.Context) error                { return nil }

func (p *tracesProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// Phase 1: pass-through. Phase 4 wires per-span Documents.
	return p.next.ConsumeTraces(ctx, td)
}
