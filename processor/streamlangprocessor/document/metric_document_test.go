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

package document

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func newGaugeDoc(t *testing.T) (*MetricDocument, pmetric.NumberDataPoint) {
	t.Helper()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("cpu.utilization")
	m.SetUnit("percent")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetDoubleValue(42.5)
	dp.SetTimestamp(pcommon.Timestamp(1700000000000000000))
	return NewNumberMetricDocument(rm, sm, m, dp, 1), dp
}

func newSumDoc(t *testing.T, dpCount int) (*MetricDocument, pmetric.NumberDataPoint) {
	t.Helper()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("requests")
	s := m.SetEmptySum()
	var dp pmetric.NumberDataPoint
	for i := 0; i < dpCount; i++ {
		next := s.DataPoints().AppendEmpty()
		next.SetIntValue(int64(i + 1))
		if i == 0 {
			dp = next
		}
	}
	return NewNumberMetricDocument(rm, sm, m, dp, dpCount), dp
}

func newHistogramDoc(t *testing.T) (*MetricDocument, pmetric.HistogramDataPoint) {
	t.Helper()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("latency")
	h := m.SetEmptyHistogram()
	dp := h.DataPoints().AppendEmpty()
	dp.SetCount(100)
	dp.SetSum(50.0)
	dp.SetMin(0.5)
	dp.SetMax(99.9)
	return NewHistogramMetricDocument(rm, sm, m, dp, 1), dp
}

func newSummaryDoc(t *testing.T) (*MetricDocument, pmetric.SummaryDataPoint) {
	t.Helper()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("response_size")
	s := m.SetEmptySummary()
	dp := s.DataPoints().AppendEmpty()
	dp.SetCount(7)
	dp.SetSum(123.0)
	return NewSummaryMetricDocument(rm, sm, m, dp, 1), dp
}

func TestMetricDocument_GetParentMetric(t *testing.T) {
	d, _ := newGaugeDoc(t)
	v, ok := d.Get("name")
	require.True(t, ok)
	assert.Equal(t, "cpu.utilization", v.Str())
	v, ok = d.Get("unit")
	require.True(t, ok)
	assert.Equal(t, "percent", v.Str())
}

func TestMetricDocument_SetParentMetric_SingleDP(t *testing.T) {
	d, _ := newGaugeDoc(t)
	require.NoError(t, d.Set("name", StringValue("cpu.usage")))
	v, _ := d.Get("name")
	assert.Equal(t, "cpu.usage", v.Str())
	require.NoError(t, d.Set("unit", StringValue("%")))
	v, _ = d.Get("unit")
	assert.Equal(t, "%", v.Str())
}

func TestMetricDocument_SetParentMetric_SharedDPs_Forbidden(t *testing.T) {
	d, _ := newSumDoc(t, 3)
	err := d.Set("name", StringValue("renamed"))
	require.ErrorIs(t, err, ErrUnsupportedTarget)
	err = d.Set("description", StringValue("x"))
	require.ErrorIs(t, err, ErrUnsupportedTarget)
	err = d.Set("unit", StringValue("y"))
	require.ErrorIs(t, err, ErrUnsupportedTarget)
}

func TestMetricDocument_DataPointAttributes(t *testing.T) {
	d, _ := newGaugeDoc(t)
	require.NoError(t, d.Set("attributes.host", StringValue("h1")))
	v, ok := d.Get("attributes.host")
	require.True(t, ok)
	assert.Equal(t, "h1", v.Str())
	v, ok = d.Get("data_point.attributes.host")
	require.True(t, ok)
	assert.Equal(t, "h1", v.Str())
}

func TestMetricDocument_BareKeyFallback(t *testing.T) {
	d, _ := newGaugeDoc(t)
	require.NoError(t, d.Set("region", StringValue("eu-west")))
	v, ok := d.Get("region")
	require.True(t, ok)
	assert.Equal(t, "eu-west", v.Str())
	v, ok = d.Get("attributes.region")
	require.True(t, ok)
	assert.Equal(t, "eu-west", v.Str())
}

func TestMetricDocument_NumberValue_Read(t *testing.T) {
	d, _ := newGaugeDoc(t)
	v, ok := d.Get("data_point.value")
	require.True(t, ok)
	assert.Equal(t, ValueTypeDouble, v.Type())
	assert.Equal(t, 42.5, v.Double())
}

func TestMetricDocument_NumberValue_Write(t *testing.T) {
	d, _ := newGaugeDoc(t)
	require.NoError(t, d.Set("data_point.value", DoubleValue(99.0)))
	v, _ := d.Get("data_point.value")
	assert.Equal(t, 99.0, v.Double())

	// Switch to int.
	require.NoError(t, d.Set("data_point.value", IntValue(7)))
	v, _ = d.Get("data_point.value")
	assert.Equal(t, ValueTypeInt, v.Type())
	assert.Equal(t, int64(7), v.Int())
}

func TestMetricDocument_NumberValue_OnHistogram_Unsupported(t *testing.T) {
	d, _ := newHistogramDoc(t)
	_, ok := d.Get("data_point.value")
	assert.False(t, ok)
	err := d.Set("data_point.value", IntValue(1))
	require.ErrorIs(t, err, ErrUnsupportedTarget)
}

func TestMetricDocument_HistogramSumCountMinMax(t *testing.T) {
	d, _ := newHistogramDoc(t)
	v, ok := d.Get("data_point.count")
	require.True(t, ok)
	assert.Equal(t, int64(100), v.Int())
	v, ok = d.Get("data_point.sum")
	require.True(t, ok)
	assert.Equal(t, 50.0, v.Double())
	v, ok = d.Get("data_point.min")
	require.True(t, ok)
	assert.Equal(t, 0.5, v.Double())
	v, ok = d.Get("data_point.max")
	require.True(t, ok)
	assert.Equal(t, 99.9, v.Double())

	require.NoError(t, d.Set("data_point.sum", DoubleValue(60.0)))
	v, _ = d.Get("data_point.sum")
	assert.Equal(t, 60.0, v.Double())

	require.NoError(t, d.Set("data_point.count", IntValue(150)))
	v, _ = d.Get("data_point.count")
	assert.Equal(t, int64(150), v.Int())
}

func TestMetricDocument_SummaryCountSum(t *testing.T) {
	d, _ := newSummaryDoc(t)
	v, _ := d.Get("data_point.count")
	assert.Equal(t, int64(7), v.Int())
	v, _ = d.Get("data_point.sum")
	assert.Equal(t, 123.0, v.Double())
	require.NoError(t, d.Set("data_point.count", IntValue(8)))
	v, _ = d.Get("data_point.count")
	assert.Equal(t, int64(8), v.Int())
}

func TestMetricDocument_Timestamps(t *testing.T) {
	d, _ := newGaugeDoc(t)
	v, _ := d.Get("data_point.time_unix_nano")
	assert.Equal(t, int64(1700000000000000000), v.Int())

	require.NoError(t, d.Set("data_point.time_unix_nano", IntValue(1800000000000000000)))
	v, _ = d.Get("data_point.time_unix_nano")
	assert.Equal(t, int64(1800000000000000000), v.Int())
}

func TestMetricDocument_ResourceAndScope(t *testing.T) {
	d, _ := newGaugeDoc(t)
	require.NoError(t, d.Set("resource.attributes.host.name", StringValue("server-1")))
	v, _ := d.Get("resource.attributes.host.name")
	assert.Equal(t, "server-1", v.Str())

	require.NoError(t, d.Set("scope.name", StringValue("io.opentelemetry.runtime")))
	v, _ = d.Get("scope.name")
	assert.Equal(t, "io.opentelemetry.runtime", v.Str())
}

func TestMetricDocument_Drop(t *testing.T) {
	d, _ := newGaugeDoc(t)
	assert.False(t, d.IsDropped())
	d.Drop()
	assert.True(t, d.IsDropped())
}

func TestMetricDocument_RemoveAttribute(t *testing.T) {
	d, _ := newGaugeDoc(t)
	require.NoError(t, d.Set("attributes.foo", StringValue("bar")))
	require.True(t, d.Remove("attributes.foo"))
	_, ok := d.Get("attributes.foo")
	assert.False(t, ok)
}

func TestMetricDocument_RemoveByPrefix(t *testing.T) {
	d, _ := newGaugeDoc(t)
	require.NoError(t, d.Set("attributes.user.id", StringValue("42")))
	require.NoError(t, d.Set("attributes.user.name", StringValue("Ada")))
	d.RemoveByPrefix("attributes.user")
	_, ok := d.Get("attributes.user.id")
	assert.False(t, ok)
	_, ok = d.Get("attributes.user.name")
	assert.False(t, ok)
}

func TestMetricDocument_Has(t *testing.T) {
	d, _ := newGaugeDoc(t)
	assert.True(t, d.Has("name"))
	assert.False(t, d.Has("nonexistent"))
	assert.True(t, d.Has("data_point.value"))
}
