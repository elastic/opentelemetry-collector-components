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

package document // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// DataPointKind identifies which OTLP metric data-point variant a Document
// is wrapping. The variant determines which paths under data_point.* are
// addressable.
type DataPointKind int

const (
	DataPointKindNumber DataPointKind = iota
	DataPointKindHistogram
	DataPointKindExponentialHistogram
	DataPointKindSummary
)

// MetricDocument is a Document over a single metric data point plus its
// enclosing ResourceMetrics / ScopeMetrics / Metric.
//
// The parent Metric is shared across data points within one Metric envelope,
// so writes to name / description / unit are only allowed when this is the
// only data point ("dpCountInParent == 1"). Otherwise Set returns
// ErrUnsupportedTarget — surfaceable as a per-document failure that the
// executor can swallow with ignore_failure.
type MetricDocument struct {
	rm pmetric.ResourceMetrics
	sm pmetric.ScopeMetrics
	m  pmetric.Metric

	kind            DataPointKind
	number          pmetric.NumberDataPoint
	histogram       pmetric.HistogramDataPoint
	expHistogram    pmetric.ExponentialHistogramDataPoint
	summary         pmetric.SummaryDataPoint
	dpCountInParent int

	dropped bool
}

// NewNumberMetricDocument returns a Document wrapping a NumberDataPoint.
func NewNumberMetricDocument(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, m pmetric.Metric, dp pmetric.NumberDataPoint, dpCount int) *MetricDocument {
	return &MetricDocument{
		rm: rm, sm: sm, m: m,
		kind: DataPointKindNumber, number: dp, dpCountInParent: dpCount,
	}
}

// NewHistogramMetricDocument returns a Document wrapping a HistogramDataPoint.
func NewHistogramMetricDocument(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, m pmetric.Metric, dp pmetric.HistogramDataPoint, dpCount int) *MetricDocument {
	return &MetricDocument{
		rm: rm, sm: sm, m: m,
		kind: DataPointKindHistogram, histogram: dp, dpCountInParent: dpCount,
	}
}

// NewExponentialHistogramMetricDocument returns a Document wrapping an
// ExponentialHistogramDataPoint.
func NewExponentialHistogramMetricDocument(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, m pmetric.Metric, dp pmetric.ExponentialHistogramDataPoint, dpCount int) *MetricDocument {
	return &MetricDocument{
		rm: rm, sm: sm, m: m,
		kind: DataPointKindExponentialHistogram, expHistogram: dp, dpCountInParent: dpCount,
	}
}

// NewSummaryMetricDocument returns a Document wrapping a SummaryDataPoint.
func NewSummaryMetricDocument(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, m pmetric.Metric, dp pmetric.SummaryDataPoint, dpCount int) *MetricDocument {
	return &MetricDocument{
		rm: rm, sm: sm, m: m,
		kind: DataPointKindSummary, summary: dp, dpCountInParent: dpCount,
	}
}

// Signal implements Document.
func (d *MetricDocument) Signal() SignalKind { return SignalMetrics }

// Field implements Document. Equivalent to Get(path).AsAny() but skips the
// Value struct allocation on the hot path.
func (d *MetricDocument) Field(path string) (any, bool) {
	v, ok := d.Get(path)
	if !ok {
		return nil, false
	}
	return v.AsAny(), true
}

// Drop marks this data point as dropped — the processor prunes it from the
// parent slice during ConsumeMetrics.
func (d *MetricDocument) Drop()           { d.dropped = true }
func (d *MetricDocument) IsDropped() bool { return d.dropped }

// Kind returns which DataPoint variant this Document wraps.
func (d *MetricDocument) Kind() DataPointKind { return d.kind }

// dpAttributes returns the data point's attributes regardless of variant.
func (d *MetricDocument) dpAttributes() pcommon.Map {
	switch d.kind {
	case DataPointKindNumber:
		return d.number.Attributes()
	case DataPointKindHistogram:
		return d.histogram.Attributes()
	case DataPointKindExponentialHistogram:
		return d.expHistogram.Attributes()
	case DataPointKindSummary:
		return d.summary.Attributes()
	}
	return pcommon.NewMap()
}

// dpTimestamp returns the data point's primary timestamp regardless of variant.
func (d *MetricDocument) dpTimestamp() pcommon.Timestamp {
	switch d.kind {
	case DataPointKindNumber:
		return d.number.Timestamp()
	case DataPointKindHistogram:
		return d.histogram.Timestamp()
	case DataPointKindExponentialHistogram:
		return d.expHistogram.Timestamp()
	case DataPointKindSummary:
		return d.summary.Timestamp()
	}
	return 0
}

// dpStartTimestamp mirrors dpTimestamp for StartTimestamp.
func (d *MetricDocument) dpStartTimestamp() pcommon.Timestamp {
	switch d.kind {
	case DataPointKindNumber:
		return d.number.StartTimestamp()
	case DataPointKindHistogram:
		return d.histogram.StartTimestamp()
	case DataPointKindExponentialHistogram:
		return d.expHistogram.StartTimestamp()
	case DataPointKindSummary:
		return d.summary.StartTimestamp()
	}
	return 0
}

func (d *MetricDocument) setDPTimestamp(t pcommon.Timestamp) {
	switch d.kind {
	case DataPointKindNumber:
		d.number.SetTimestamp(t)
	case DataPointKindHistogram:
		d.histogram.SetTimestamp(t)
	case DataPointKindExponentialHistogram:
		d.expHistogram.SetTimestamp(t)
	case DataPointKindSummary:
		d.summary.SetTimestamp(t)
	}
}

func (d *MetricDocument) setDPStartTimestamp(t pcommon.Timestamp) {
	switch d.kind {
	case DataPointKindNumber:
		d.number.SetStartTimestamp(t)
	case DataPointKindHistogram:
		d.histogram.SetStartTimestamp(t)
	case DataPointKindExponentialHistogram:
		d.expHistogram.SetStartTimestamp(t)
	case DataPointKindSummary:
		d.summary.SetStartTimestamp(t)
	}
}

// Get implements Document. See README path-conventions table for the metric
// signal mapping.
func (d *MetricDocument) Get(path string) (Value, bool) {
	prefix, rest := SplitPath(path)
	switch prefix {
	case "resource":
		return d.getResource(rest)
	case "scope":
		return d.getScope(rest)
	case "name":
		if rest != "" {
			return Value{}, false
		}
		return StringValue(d.m.Name()), true
	case "description":
		if rest != "" {
			return Value{}, false
		}
		return StringValue(d.m.Description()), true
	case "unit":
		if rest != "" {
			return Value{}, false
		}
		return StringValue(d.m.Unit()), true
	case "metadata":
		if rest == "" {
			return FromPcommon(mapAsValue(d.m.Metadata())), true
		}
		v, ok := mapGetNested(d.m.Metadata(), rest)
		if !ok {
			return Value{}, false
		}
		return FromPcommon(v), true
	case "attributes":
		// Alias for data_point.attributes. Matches log/span ergonomics.
		if rest == "" {
			return FromPcommon(mapAsValue(d.dpAttributes())), true
		}
		v, ok := mapGetNested(d.dpAttributes(), rest)
		if !ok {
			return Value{}, false
		}
		return FromPcommon(v), true
	case "data_point":
		return d.getDataPoint(rest)
	}
	// Bare-key fallback → data point attributes (ES ingest compat).
	if rest == "" {
		v, ok := mapGetNested(d.dpAttributes(), prefix)
		if !ok {
			return Value{}, false
		}
		return FromPcommon(v), true
	}
	v, ok := mapGetNested(d.dpAttributes(), path)
	if !ok {
		return Value{}, false
	}
	return FromPcommon(v), true
}

// Set implements Document.
func (d *MetricDocument) Set(path string, v Value) error {
	prefix, rest := SplitPath(path)
	switch prefix {
	case "resource":
		return d.setResource(rest, v)
	case "scope":
		return d.setScope(rest, v)
	case "name":
		return d.setMetricName(rest, v)
	case "description":
		return d.setMetricDescription(rest, v)
	case "unit":
		return d.setMetricUnit(rest, v)
	case "metadata":
		if rest == "" {
			return ErrUnsupportedTarget
		}
		mapSetNested(d.m.Metadata(), rest, v)
		return nil
	case "attributes":
		if rest == "" {
			return ErrUnsupportedTarget
		}
		mapSetNested(d.dpAttributes(), rest, v)
		return nil
	case "data_point":
		return d.setDataPoint(rest, v)
	}
	// Bare-key fallback → data point attributes.
	mapSetNested(d.dpAttributes(), path, v)
	return nil
}

// Remove implements Document.
func (d *MetricDocument) Remove(path string) bool {
	prefix, rest := SplitPath(path)
	switch prefix {
	case "resource":
		if rest == "" {
			return false
		}
		if subPrefix, subRest := SplitPath(rest); subPrefix == "attributes" {
			return mapRemoveNested(d.rm.Resource().Attributes(), subRest)
		}
		return false
	case "scope":
		if rest == "" {
			return false
		}
		if subPrefix, subRest := SplitPath(rest); subPrefix == "attributes" {
			return mapRemoveNested(d.sm.Scope().Attributes(), subRest)
		}
		return false
	case "metadata":
		if rest == "" {
			return false
		}
		return mapRemoveNested(d.m.Metadata(), rest)
	case "attributes":
		if rest == "" {
			return false
		}
		return mapRemoveNested(d.dpAttributes(), rest)
	case "data_point":
		// Only attribute-removal is supported under data_point.
		if subPrefix, subRest := SplitPath(rest); subPrefix == "attributes" && subRest != "" {
			return mapRemoveNested(d.dpAttributes(), subRest)
		}
		return false
	}
	return mapRemoveNested(d.dpAttributes(), path)
}

// RemoveByPrefix implements Document.
func (d *MetricDocument) RemoveByPrefix(prefix string) {
	p, rest := SplitPath(prefix)
	switch p {
	case "resource":
		if subPrefix, subRest := SplitPath(rest); subPrefix == "attributes" {
			mapRemoveByPrefix(d.rm.Resource().Attributes(), subRest)
		}
		return
	case "scope":
		if subPrefix, subRest := SplitPath(rest); subPrefix == "attributes" {
			mapRemoveByPrefix(d.sm.Scope().Attributes(), subRest)
		}
		return
	case "metadata":
		mapRemoveByPrefix(d.m.Metadata(), rest)
		return
	case "attributes":
		mapRemoveByPrefix(d.dpAttributes(), rest)
		return
	case "data_point":
		if subPrefix, subRest := SplitPath(rest); subPrefix == "attributes" {
			mapRemoveByPrefix(d.dpAttributes(), subRest)
		}
		return
	}
	mapRemoveByPrefix(d.dpAttributes(), prefix)
}

// Has implements Document.
func (d *MetricDocument) Has(path string) bool {
	_, ok := d.Get(path)
	return ok
}

// --- helpers below: parent-scope getters/setters ------------------------

func (d *MetricDocument) getResource(rest string) (Value, bool) {
	if rest == "" {
		return Value{}, false
	}
	subPrefix, subRest := SplitPath(rest)
	if subPrefix != "attributes" {
		return Value{}, false
	}
	if subRest == "" {
		return FromPcommon(mapAsValue(d.rm.Resource().Attributes())), true
	}
	v, ok := mapGetNested(d.rm.Resource().Attributes(), subRest)
	if !ok {
		return Value{}, false
	}
	return FromPcommon(v), true
}

func (d *MetricDocument) setResource(rest string, v Value) error {
	if rest == "" {
		return ErrUnsupportedTarget
	}
	subPrefix, subRest := SplitPath(rest)
	if subPrefix != "attributes" || subRest == "" {
		return ErrUnsupportedTarget
	}
	mapSetNested(d.rm.Resource().Attributes(), subRest, v)
	return nil
}

func (d *MetricDocument) getScope(rest string) (Value, bool) {
	if rest == "" {
		return Value{}, false
	}
	subPrefix, subRest := SplitPath(rest)
	switch subPrefix {
	case "name":
		if subRest != "" {
			return Value{}, false
		}
		return StringValue(d.sm.Scope().Name()), true
	case "version":
		if subRest != "" {
			return Value{}, false
		}
		return StringValue(d.sm.Scope().Version()), true
	case "attributes":
		if subRest == "" {
			return FromPcommon(mapAsValue(d.sm.Scope().Attributes())), true
		}
		v, ok := mapGetNested(d.sm.Scope().Attributes(), subRest)
		if !ok {
			return Value{}, false
		}
		return FromPcommon(v), true
	}
	return Value{}, false
}

func (d *MetricDocument) setScope(rest string, v Value) error {
	if rest == "" {
		return ErrUnsupportedTarget
	}
	subPrefix, subRest := SplitPath(rest)
	switch subPrefix {
	case "name":
		if subRest != "" || v.Type() != ValueTypeStr {
			return ErrUnsupportedTarget
		}
		d.sm.Scope().SetName(v.Str())
		return nil
	case "version":
		if subRest != "" || v.Type() != ValueTypeStr {
			return ErrUnsupportedTarget
		}
		d.sm.Scope().SetVersion(v.Str())
		return nil
	case "attributes":
		if subRest == "" {
			return ErrUnsupportedTarget
		}
		mapSetNested(d.sm.Scope().Attributes(), subRest, v)
		return nil
	}
	return ErrUnsupportedTarget
}

func (d *MetricDocument) setMetricName(rest string, v Value) error {
	if rest != "" || v.Type() != ValueTypeStr {
		return ErrUnsupportedTarget
	}
	if d.dpCountInParent > 1 {
		return ErrUnsupportedTarget
	}
	d.m.SetName(v.Str())
	return nil
}

func (d *MetricDocument) setMetricDescription(rest string, v Value) error {
	if rest != "" || v.Type() != ValueTypeStr {
		return ErrUnsupportedTarget
	}
	if d.dpCountInParent > 1 {
		return ErrUnsupportedTarget
	}
	d.m.SetDescription(v.Str())
	return nil
}

func (d *MetricDocument) setMetricUnit(rest string, v Value) error {
	if rest != "" || v.Type() != ValueTypeStr {
		return ErrUnsupportedTarget
	}
	if d.dpCountInParent > 1 {
		return ErrUnsupportedTarget
	}
	d.m.SetUnit(v.Str())
	return nil
}

// --- data_point.* paths -------------------------------------------------

func (d *MetricDocument) getDataPoint(rest string) (Value, bool) {
	if rest == "" {
		return Value{}, false
	}
	subPrefix, subRest := SplitPath(rest)
	switch subPrefix {
	case "attributes":
		if subRest == "" {
			return FromPcommon(mapAsValue(d.dpAttributes())), true
		}
		v, ok := mapGetNested(d.dpAttributes(), subRest)
		if !ok {
			return Value{}, false
		}
		return FromPcommon(v), true
	case "time_unix_nano":
		if subRest != "" {
			return Value{}, false
		}
		return IntValue(int64(d.dpTimestamp())), true
	case "start_time_unix_nano":
		if subRest != "" {
			return Value{}, false
		}
		return IntValue(int64(d.dpStartTimestamp())), true
	case "flags":
		if subRest != "" {
			return Value{}, false
		}
		return d.getDataPointFlags()
	case "value":
		if subRest != "" || d.kind != DataPointKindNumber {
			return Value{}, false
		}
		switch d.number.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			return IntValue(d.number.IntValue()), true
		case pmetric.NumberDataPointValueTypeDouble:
			return DoubleValue(d.number.DoubleValue()), true
		}
		return Value{}, false
	case "count":
		if subRest != "" {
			return Value{}, false
		}
		return d.getDataPointCount()
	case "sum":
		if subRest != "" {
			return Value{}, false
		}
		return d.getDataPointSum()
	case "min":
		if subRest != "" {
			return Value{}, false
		}
		return d.getDataPointMin()
	case "max":
		if subRest != "" {
			return Value{}, false
		}
		return d.getDataPointMax()
	}
	return Value{}, false
}

func (d *MetricDocument) setDataPoint(rest string, v Value) error {
	if rest == "" {
		return ErrUnsupportedTarget
	}
	subPrefix, subRest := SplitPath(rest)
	switch subPrefix {
	case "attributes":
		if subRest == "" {
			return ErrUnsupportedTarget
		}
		mapSetNested(d.dpAttributes(), subRest, v)
		return nil
	case "time_unix_nano":
		if subRest != "" {
			return ErrUnsupportedTarget
		}
		ts, ok := valueToTimestamp(v)
		if !ok {
			return ErrUnsupportedTarget
		}
		d.setDPTimestamp(ts)
		return nil
	case "start_time_unix_nano":
		if subRest != "" {
			return ErrUnsupportedTarget
		}
		ts, ok := valueToTimestamp(v)
		if !ok {
			return ErrUnsupportedTarget
		}
		d.setDPStartTimestamp(ts)
		return nil
	case "value":
		if subRest != "" || d.kind != DataPointKindNumber {
			return ErrUnsupportedTarget
		}
		switch v.Type() {
		case ValueTypeInt:
			d.number.SetIntValue(v.Int())
			return nil
		case ValueTypeDouble:
			d.number.SetDoubleValue(v.Double())
			return nil
		}
		return ErrUnsupportedTarget
	case "count":
		if subRest != "" {
			return ErrUnsupportedTarget
		}
		return d.setDataPointCount(v)
	case "sum":
		if subRest != "" {
			return ErrUnsupportedTarget
		}
		return d.setDataPointSum(v)
	case "min":
		if subRest != "" {
			return ErrUnsupportedTarget
		}
		return d.setDataPointMin(v)
	case "max":
		if subRest != "" {
			return ErrUnsupportedTarget
		}
		return d.setDataPointMax(v)
	}
	return ErrUnsupportedTarget
}

// --- per-variant numeric accessors -------------------------------------

func (d *MetricDocument) getDataPointFlags() (Value, bool) {
	switch d.kind {
	case DataPointKindNumber:
		return IntValue(int64(d.number.Flags())), true
	case DataPointKindHistogram:
		return IntValue(int64(d.histogram.Flags())), true
	case DataPointKindExponentialHistogram:
		return IntValue(int64(d.expHistogram.Flags())), true
	case DataPointKindSummary:
		return IntValue(int64(d.summary.Flags())), true
	}
	return Value{}, false
}

func (d *MetricDocument) getDataPointCount() (Value, bool) {
	switch d.kind {
	case DataPointKindHistogram:
		return IntValue(int64(d.histogram.Count())), true
	case DataPointKindExponentialHistogram:
		return IntValue(int64(d.expHistogram.Count())), true
	case DataPointKindSummary:
		return IntValue(int64(d.summary.Count())), true
	}
	return Value{}, false
}

func (d *MetricDocument) setDataPointCount(v Value) error {
	c, ok := valueToUint64(v)
	if !ok {
		return ErrUnsupportedTarget
	}
	switch d.kind {
	case DataPointKindHistogram:
		d.histogram.SetCount(c)
		return nil
	case DataPointKindExponentialHistogram:
		d.expHistogram.SetCount(c)
		return nil
	case DataPointKindSummary:
		d.summary.SetCount(c)
		return nil
	}
	return ErrUnsupportedTarget
}

func (d *MetricDocument) getDataPointSum() (Value, bool) {
	switch d.kind {
	case DataPointKindHistogram:
		if !d.histogram.HasSum() {
			return Value{}, false
		}
		return DoubleValue(d.histogram.Sum()), true
	case DataPointKindExponentialHistogram:
		if !d.expHistogram.HasSum() {
			return Value{}, false
		}
		return DoubleValue(d.expHistogram.Sum()), true
	case DataPointKindSummary:
		return DoubleValue(d.summary.Sum()), true
	}
	return Value{}, false
}

func (d *MetricDocument) setDataPointSum(v Value) error {
	f, ok := valueToFloat64(v)
	if !ok {
		return ErrUnsupportedTarget
	}
	switch d.kind {
	case DataPointKindHistogram:
		d.histogram.SetSum(f)
		return nil
	case DataPointKindExponentialHistogram:
		d.expHistogram.SetSum(f)
		return nil
	case DataPointKindSummary:
		d.summary.SetSum(f)
		return nil
	}
	return ErrUnsupportedTarget
}

func (d *MetricDocument) getDataPointMin() (Value, bool) {
	switch d.kind {
	case DataPointKindHistogram:
		if !d.histogram.HasMin() {
			return Value{}, false
		}
		return DoubleValue(d.histogram.Min()), true
	case DataPointKindExponentialHistogram:
		if !d.expHistogram.HasMin() {
			return Value{}, false
		}
		return DoubleValue(d.expHistogram.Min()), true
	}
	return Value{}, false
}

func (d *MetricDocument) setDataPointMin(v Value) error {
	f, ok := valueToFloat64(v)
	if !ok {
		return ErrUnsupportedTarget
	}
	switch d.kind {
	case DataPointKindHistogram:
		d.histogram.SetMin(f)
		return nil
	case DataPointKindExponentialHistogram:
		d.expHistogram.SetMin(f)
		return nil
	}
	return ErrUnsupportedTarget
}

func (d *MetricDocument) getDataPointMax() (Value, bool) {
	switch d.kind {
	case DataPointKindHistogram:
		if !d.histogram.HasMax() {
			return Value{}, false
		}
		return DoubleValue(d.histogram.Max()), true
	case DataPointKindExponentialHistogram:
		if !d.expHistogram.HasMax() {
			return Value{}, false
		}
		return DoubleValue(d.expHistogram.Max()), true
	}
	return Value{}, false
}

func (d *MetricDocument) setDataPointMax(v Value) error {
	f, ok := valueToFloat64(v)
	if !ok {
		return ErrUnsupportedTarget
	}
	switch d.kind {
	case DataPointKindHistogram:
		d.histogram.SetMax(f)
		return nil
	case DataPointKindExponentialHistogram:
		d.expHistogram.SetMax(f)
		return nil
	}
	return ErrUnsupportedTarget
}

// --- value coercion helpers used by metric setters --------------------

func valueToTimestamp(v Value) (pcommon.Timestamp, bool) {
	switch v.Type() {
	case ValueTypeInt:
		return pcommon.Timestamp(v.Int()), true
	case ValueTypeDouble:
		return pcommon.Timestamp(int64(v.Double())), true
	}
	return 0, false
}

func valueToUint64(v Value) (uint64, bool) {
	switch v.Type() {
	case ValueTypeInt:
		i := v.Int()
		if i < 0 {
			return 0, false
		}
		return uint64(i), true
	case ValueTypeDouble:
		f := v.Double()
		if f < 0 {
			return 0, false
		}
		return uint64(f), true
	}
	return 0, false
}

func valueToFloat64(v Value) (float64, bool) {
	switch v.Type() {
	case ValueTypeInt:
		return float64(v.Int()), true
	case ValueTypeDouble:
		return v.Double(), true
	}
	return 0, false
}
