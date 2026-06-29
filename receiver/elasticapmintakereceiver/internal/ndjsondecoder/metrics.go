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

package ndjsondecoder

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
)

// DecodeMetricset reads the next NDJSON line and decodes it as a metricset event.
func DecodeMetricset(dec *NDJSONStreamDecoder) (*metricset, error) {
	var root metricsetRoot
	if err := dec.Decode(&root); err != nil {
		return nil, err
	}
	return &root.Metricset, nil
}

// MetricsetName returns the metricset name: "span_breakdown" when the metricset
// carries span+transaction correlation fields, "app" otherwise.
func MetricsetName(ms *metricset) string {
	if (ms.Span.Type.IsSet() || ms.Span.Subtype.IsSet()) &&
		(ms.Transaction.Name.IsSet() || ms.Transaction.Type.IsSet()) {
		return "span_breakdown"
	}
	return "app"
}

// MetricsetContextService returns a per-event service override when the
// metricset carries service correlation fields, or nil otherwise.
func MetricsetContextService(ms *metricset) *contextService {
	if !ms.Service.Name.IsSet() && !ms.Service.Version.IsSet() {
		return nil
	}
	return &contextService{
		Name:    ms.Service.Name,
		Version: ms.Service.Version,
	}
}

// MetricsetTags returns the raw per-metricset label map (may be nil).
// The stream handler uses this to build additional resource attributes and
// to route shadow batches when metricset tags shadow global labels.
func MetricsetTags(ms *metricset) map[string]any {
	return ms.Tags
}

// WriteFAASResourceAttrs writes FaaS resource attributes from ms into attrs.
// It is a no-op when ms carries no FaaS fields.
func WriteFAASResourceAttrs(attrs pcommon.Map, ms *metricset) {
	f := ms.FAAS
	if f.ID.IsSet() {
		attrs.PutStr(string(semconv.FaaSInstanceKey), f.ID.Val)
	}
	if f.Name.IsSet() {
		attrs.PutStr(string(semconv.FaaSNameKey), f.Name.Val)
	}
	if f.Version.IsSet() {
		attrs.PutStr(string(semconv.FaaSVersionKey), f.Version.Val)
	}
	if f.Trigger.Type.IsSet() {
		attrs.PutStr(string(semconv.FaaSTriggerKey), f.Trigger.Type.Val)
	}
	if f.Coldstart.IsSet() {
		attrs.PutBool(string(semconv.FaaSColdstartKey), f.Coldstart.Val)
	}
	if f.Trigger.RequestID.IsSet() {
		attrs.PutStr(elasticattr.FaaSTriggerRequestID, f.Trigger.RequestID.Val)
	}
	if f.Execution.IsSet() {
		attrs.PutStr(elasticattr.FaaSExecution, f.Execution.Val)
	}
}

// AppendMetricset appends metrics from ms into sm.
// Resource-level fields (service, FaaS, tags, metricset.name) are NOT written
// here; the stream handler owns them.
func AppendMetricset(sm pmetric.ScopeMetrics, ms *metricset) {
	ts := pcommon.Timestamp(ms.Timestamp.Val.UnixNano())
	if MetricsetName(ms) == "span_breakdown" {
		appendSpanBreakdownMetrics(sm, ms, ts)
		return
	}
	for name, sample := range ms.Samples {
		m := sm.Metrics().AppendEmpty()
		m.SetName(name)
		if sample.Unit.IsSet() {
			m.SetUnit(sample.Unit.Val)
		}
		switch sample.Type.Val {
		case "histogram":
			dp := m.SetEmptyHistogram().DataPoints().AppendEmpty()
			dp.SetTimestamp(ts)
			dp.Attributes().PutStr(elasticattr.ProcessorEvent, "metric")
			populateHistogramDP(dp, sample)
		case "counter":
			dp := m.SetEmptySum().DataPoints().AppendEmpty()
			dp.SetTimestamp(ts)
			dp.SetDoubleValue(sample.Value.Val)
			dp.Attributes().PutStr(elasticattr.ProcessorEvent, "metric")
		default: // "gauge" or unspecified
			dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(ts)
			dp.SetDoubleValue(sample.Value.Val)
			dp.Attributes().PutStr(elasticattr.ProcessorEvent, "metric")
		}
	}
}

func appendSpanBreakdownMetrics(sm pmetric.ScopeMetrics, ms *metricset, ts pcommon.Timestamp) {
	if sumSample, ok := ms.Samples["span.self_time.sum.us"]; ok {
		m := sm.Metrics().AppendEmpty()
		m.SetName("span.self_time.sum.us")
		m.SetUnit("us")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(ts)
		dp.SetIntValue(int64(sumSample.Value.Val))
		setBreakdownDPAttrs(dp.Attributes(), ms)
	}
	if countSample, ok := ms.Samples["span.self_time.count"]; ok {
		m := sm.Metrics().AppendEmpty()
		m.SetName("span.self_time.count")
		m.SetUnit("{span}")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(ts)
		dp.SetDoubleValue(countSample.Value.Val)
		setBreakdownDPAttrs(dp.Attributes(), ms)
	}
}

func setBreakdownDPAttrs(attrs pcommon.Map, ms *metricset) {
	if ms.Transaction.Name.IsSet() {
		attrs.PutStr(elasticattr.TransactionName, ms.Transaction.Name.Val)
	}
	if ms.Transaction.Type.IsSet() {
		attrs.PutStr(elasticattr.TransactionType, ms.Transaction.Type.Val)
	}
	if ms.Span.Type.IsSet() {
		attrs.PutStr(elasticattr.SpanType, ms.Span.Type.Val)
	}
	if ms.Span.Subtype.IsSet() {
		attrs.PutStr(elasticattr.SpanSubtype, ms.Span.Subtype.Val)
	}
	attrs.PutStr(elasticattr.ProcessorEvent, "metric")
}

func populateHistogramDP(dp pmetric.HistogramDataPoint, sample metricsetSampleValue) {
	values := sample.Values
	counts := sample.Counts
	if len(values) == 0 || len(counts) == 0 || len(values) != len(counts) {
		return
	}
	sum := 0.0
	for i, v := range values {
		sum += v * float64(counts[i])
	}
	dp.SetSum(sum)
	var count uint64
	for _, c := range counts {
		count += c
	}
	dp.SetCount(count)
	dp.BucketCounts().FromRaw(append(counts, 0))
	dp.ExplicitBounds().FromRaw(values)
	dp.Attributes().PutEmptySlice("elasticsearch.mapping.hints").AppendEmpty().SetStr("histogram:raw")
}
