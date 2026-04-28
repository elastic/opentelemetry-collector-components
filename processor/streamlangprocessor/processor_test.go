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

package streamlangprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/metadata"
)

func newTestLog(severity, message string) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetSeverityText(severity)
	lr.Body().SetStr(message)
	return ld
}

func TestLogsProcessor_AppliesPipeline(t *testing.T) {
	cfg := &Config{
		Steps: []map[string]any{
			{"action": "set", "to": "attributes.processed_by", "value": "streamlang"},
			{"action": "uppercase", "from": "severity_text"},
		},
		FailureMode: FailureModeDrop,
	}
	require.NoError(t, cfg.Validate())

	sink := &consumertest.LogsSink{}
	p, err := newLogsProcessor(processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, p.Start(context.Background(), nil))

	require.NoError(t, p.ConsumeLogs(context.Background(), newTestLog("error", "hello")))

	require.Len(t, sink.AllLogs(), 1)
	out := sink.AllLogs()[0]
	require.Equal(t, 1, out.LogRecordCount())
	lr := out.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "ERROR", lr.SeverityText())
	pb, ok := lr.Attributes().Get("processed_by")
	require.True(t, ok)
	assert.Equal(t, "streamlang", pb.Str())

	require.NoError(t, p.Shutdown(context.Background()))
}

func TestLogsProcessor_DropDocumentRemovesRecord(t *testing.T) {
	cfg := &Config{
		Steps: []map[string]any{
			{
				"action": "drop_document",
				"where":  map[string]any{"field": "severity_text", "eq": "DEBUG"},
			},
		},
		FailureMode: FailureModeDrop,
	}
	require.NoError(t, cfg.Validate())

	sink := &consumertest.LogsSink{}
	p, err := newLogsProcessor(processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)

	// Mix DEBUG (dropped) and INFO (kept) records.
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	for _, sev := range []string{"DEBUG", "INFO", "DEBUG", "WARN"} {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetSeverityText(sev)
	}
	require.NoError(t, p.ConsumeLogs(context.Background(), ld))

	require.Len(t, sink.AllLogs(), 1)
	out := sink.AllLogs()[0]
	assert.Equal(t, 2, out.LogRecordCount())
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"empty", Config{}, true},
		{"both", Config{Steps: []map[string]any{{"action": "drop_document"}}, Path: "/x"}, true},
		{"steps_ok", Config{Steps: []map[string]any{{"action": "set", "to": "a", "value": "b"}}}, false},
		{"path_ok", Config{Path: "/x"}, false},
		{"bad_failure_mode", Config{Path: "/x", FailureMode: "weird"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func newGaugeMetric(name string, dpAttrs map[string]string, value float64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetDoubleValue(value)
	dp.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	for k, v := range dpAttrs {
		dp.Attributes().PutStr(k, v)
	}
	return md
}

func TestMetricsProcessor_AppliesPipeline(t *testing.T) {
	cfg := &Config{
		Steps: []map[string]any{
			{"action": "set", "to": "attributes.processed_by", "value": "streamlang"},
			{"action": "set", "to": "data_point.value", "value": 99.5},
		},
		FailureMode: FailureModeDrop,
	}
	require.NoError(t, cfg.Validate())

	sink := &consumertest.MetricsSink{}
	p, err := newMetricsProcessor(processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)

	require.NoError(t, p.ConsumeMetrics(context.Background(), newGaugeMetric("cpu", map[string]string{"host": "h1"}, 42.0)))

	require.Len(t, sink.AllMetrics(), 1)
	out := sink.AllMetrics()[0]
	dp := out.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
	pb, ok := dp.Attributes().Get("processed_by")
	require.True(t, ok)
	assert.Equal(t, "streamlang", pb.Str())
	assert.Equal(t, 99.5, dp.DoubleValue())
}

func TestMetricsProcessor_DropDocumentRemovesDP(t *testing.T) {
	cfg := &Config{
		Steps: []map[string]any{
			{
				"action": "drop_document",
				"where":  map[string]any{"field": "attributes.host", "eq": "h2"},
			},
		},
		FailureMode: FailureModeDrop,
	}
	require.NoError(t, cfg.Validate())

	// Two data points on the same gauge metric. h2 should be dropped, h1 kept.
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("cpu")
	g := m.SetEmptyGauge()
	for _, host := range []string{"h1", "h2", "h3"} {
		dp := g.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("host", host)
		dp.SetDoubleValue(1.0)
	}

	sink := &consumertest.MetricsSink{}
	p, err := newMetricsProcessor(processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)

	require.NoError(t, p.ConsumeMetrics(context.Background(), md))

	require.Len(t, sink.AllMetrics(), 1)
	dps := sink.AllMetrics()[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints()
	require.Equal(t, 2, dps.Len())
	hosts := []string{
		mustStr(dps.At(0).Attributes().Get("host")),
		mustStr(dps.At(1).Attributes().Get("host")),
	}
	assert.NotContains(t, hosts, "h2")
}

func TestMetricsProcessor_HistogramSumWrite(t *testing.T) {
	cfg := &Config{
		Steps: []map[string]any{
			{"action": "set", "to": "data_point.sum", "value": 123.5},
		},
		FailureMode: FailureModeDrop,
	}
	require.NoError(t, cfg.Validate())

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("latency")
	h := m.SetEmptyHistogram()
	dp := h.DataPoints().AppendEmpty()
	dp.SetCount(10)
	dp.SetSum(50.0)

	sink := &consumertest.MetricsSink{}
	p, err := newMetricsProcessor(processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)

	require.NoError(t, p.ConsumeMetrics(context.Background(), md))
	out := sink.AllMetrics()[0]
	got := out.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Histogram().DataPoints().At(0)
	assert.Equal(t, 123.5, got.Sum())
}

func mustStr(v pcommon.Value, ok bool) string {
	if !ok {
		return ""
	}
	return v.Str()
}

func TestFactoryLifecycle(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Steps = []map[string]any{{"action": "set", "to": "attributes.x", "value": "y"}}
	require.NoError(t, cfg.Validate())

	logs, err := f.CreateLogs(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NoError(t, logs.Start(context.Background(), nil))
	require.NoError(t, logs.Shutdown(context.Background()))

	traces, err := f.CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NoError(t, traces.Start(context.Background(), nil))
	require.NoError(t, traces.Shutdown(context.Background()))

	metrics, err := f.CreateMetrics(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NoError(t, metrics.Start(context.Background(), nil))
	require.NoError(t, metrics.Shutdown(context.Background()))
}
