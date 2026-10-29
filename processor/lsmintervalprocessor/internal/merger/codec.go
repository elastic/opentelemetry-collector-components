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

package merger // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/xpdata/entity"
)

// This file holds the binary encoding used by the v2 value format for
// attributes and datapoints. Datapoints are encoded without their
// attributes, which are stored once per stream next to the stream identity.
//
// Every datapoint payload starts with the start timestamp and timestamp as
// fixed 8 byte fields, so the timestamp can be read without decoding the
// rest of the payload.

var errShortBuffer = errors.New("failed to decode value: unexpected end of data")

// decoder reads the primitives used by the v2 format. The first error is
// sticky: later reads return zero values and err stays set.
type decoder struct {
	b   []byte
	err error
}

func (d *decoder) fail(err error) {
	if d.err == nil {
		d.err = err
	}
	d.b = nil
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail(errShortBuffer)
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) varint() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.fail(errShortBuffer)
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) fixed64() uint64 {
	if d.err != nil {
		return 0
	}
	if len(d.b) < 8 {
		d.fail(errShortBuffer)
		return 0
	}
	v := binary.BigEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}

func (d *decoder) float64() float64 { return math.Float64frombits(d.fixed64()) }

func (d *decoder) byte() byte {
	if d.err != nil {
		return 0
	}
	if len(d.b) < 1 {
		d.fail(errShortBuffer)
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

// bytes returns the next length-prefixed byte slice. The slice aliases the
// decoder's buffer.
func (d *decoder) bytes() []byte {
	n := d.uvarint()
	if d.err != nil {
		return nil
	}
	if uint64(len(d.b)) < n {
		d.fail(errShortBuffer)
		return nil
	}
	v := d.b[:n:n]
	d.b = d.b[n:]
	return v
}

func (d *decoder) string() string { return string(d.bytes()) }

func (d *decoder) hash16() [16]byte {
	var h [16]byte
	if d.err != nil {
		return h
	}
	if len(d.b) < 16 {
		d.fail(errShortBuffer)
		return h
	}
	copy(h[:], d.b[:16])
	d.b = d.b[16:]
	return h
}

func appendBytes(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

func appendString(b []byte, v string) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

func appendFixed64(b []byte, v uint64) []byte { return binary.BigEndian.AppendUint64(b, v) }

func appendFloat64(b []byte, v float64) []byte { return appendFixed64(b, math.Float64bits(v)) }

// Attribute encoding.

const (
	anyEmpty byte = iota
	anyStr
	anyBool
	anyInt
	anyDouble
	anyBytes
	anySlice
	anyMap
)

func appendMap(b []byte, m pcommon.Map) []byte {
	b = binary.AppendUvarint(b, uint64(m.Len()))
	m.Range(func(k string, v pcommon.Value) bool {
		b = appendString(b, k)
		b = appendAnyValue(b, v)
		return true
	})
	return b
}

func appendAnyValue(b []byte, v pcommon.Value) []byte {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		b = append(b, anyStr)
		b = appendString(b, v.Str())
	case pcommon.ValueTypeBool:
		b = append(b, anyBool)
		if v.Bool() {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
	case pcommon.ValueTypeInt:
		b = append(b, anyInt)
		b = binary.AppendVarint(b, v.Int())
	case pcommon.ValueTypeDouble:
		b = append(b, anyDouble)
		b = appendFloat64(b, v.Double())
	case pcommon.ValueTypeBytes:
		b = append(b, anyBytes)
		b = appendBytes(b, v.Bytes().AsRaw())
	case pcommon.ValueTypeSlice:
		b = append(b, anySlice)
		s := v.Slice()
		b = binary.AppendUvarint(b, uint64(s.Len()))
		for i := 0; i < s.Len(); i++ {
			b = appendAnyValue(b, s.At(i))
		}
	case pcommon.ValueTypeMap:
		b = append(b, anyMap)
		b = appendMap(b, v.Map())
	default:
		b = append(b, anyEmpty)
	}
	return b
}

func (d *decoder) mapInto(m pcommon.Map) {
	n := d.uvarint()
	if d.err != nil {
		return
	}
	m.EnsureCapacity(int(min(n, uint64(len(d.b)))))
	for i := uint64(0); i < n && d.err == nil; i++ {
		k := d.string()
		d.anyValueInto(m.PutEmpty(k))
	}
}

func (d *decoder) anyValueInto(v pcommon.Value) {
	switch t := d.byte(); t {
	case anyEmpty:
	case anyStr:
		v.SetStr(d.string())
	case anyBool:
		v.SetBool(d.byte() == 1)
	case anyInt:
		v.SetInt(d.varint())
	case anyDouble:
		v.SetDouble(d.float64())
	case anyBytes:
		v.SetEmptyBytes().FromRaw(append([]byte(nil), d.bytes()...))
	case anySlice:
		s := v.SetEmptySlice()
		n := d.uvarint()
		for i := uint64(0); i < n && d.err == nil; i++ {
			d.anyValueInto(s.AppendEmpty())
		}
	case anyMap:
		d.mapInto(v.SetEmptyMap())
	default:
		d.fail(fmt.Errorf("failed to decode value: unknown attribute type %d", t))
	}
}

// encodeResource encodes a resource: its attributes, dropped attribute
// count, and entity references.
func encodeResource(res pcommon.Resource) []byte {
	b := encodeAttrs(res.Attributes(), res.DroppedAttributesCount())
	refs := entity.ResourceEntityRefs(res)
	b = binary.AppendUvarint(b, uint64(refs.Len()))
	for i := 0; i < refs.Len(); i++ {
		ref := refs.At(i)
		b = appendString(b, ref.SchemaUrl())
		b = appendString(b, ref.Type())
		b = appendStrings(b, ref.IdKeys())
		b = appendStrings(b, ref.DescriptionKeys())
	}
	return b
}

func decodeResourceInto(raw []byte, res pcommon.Resource) error {
	d := decoder{b: raw}
	res.SetDroppedAttributesCount(uint32(d.uvarint()))
	d.mapInto(res.Attributes())
	n := d.uvarint()
	refs := entity.ResourceEntityRefs(res)
	for i := uint64(0); i < n && d.err == nil; i++ {
		ref := refs.AppendEmpty()
		ref.SetSchemaUrl(d.string())
		ref.SetType(d.string())
		d.stringsInto(ref.IdKeys())
		d.stringsInto(ref.DescriptionKeys())
	}
	return d.err
}

func appendStrings(b []byte, s pcommon.StringSlice) []byte {
	b = binary.AppendUvarint(b, uint64(s.Len()))
	for i := 0; i < s.Len(); i++ {
		b = appendString(b, s.At(i))
	}
	return b
}

func (d *decoder) stringsInto(s pcommon.StringSlice) {
	n := d.uvarint()
	for i := uint64(0); i < n && d.err == nil; i++ {
		s.Append(d.string())
	}
}

// encodeAttrs encodes attributes and a dropped attribute count, as stored
// for scopes and streams.
func encodeAttrs(m pcommon.Map, dropped uint32) []byte {
	b := binary.AppendUvarint(nil, uint64(dropped))
	return appendMap(b, m)
}

func decodeAttrsInto(raw []byte, m pcommon.Map) (dropped uint32, err error) {
	d := decoder{b: raw}
	dropped = uint32(d.uvarint())
	d.mapInto(m)
	return dropped, d.err
}

// Datapoint payloads.

const (
	hasSum byte = 1 << iota
	hasMin
	hasMax
)

func appendExemplars(b []byte, es pmetric.ExemplarSlice) []byte {
	b = binary.AppendUvarint(b, uint64(es.Len()))
	for i := 0; i < es.Len(); i++ {
		e := es.At(i)
		b = appendFixed64(b, uint64(e.Timestamp()))
		switch e.ValueType() {
		case pmetric.ExemplarValueTypeInt:
			b = append(b, byte(pmetric.ExemplarValueTypeInt))
			b = binary.AppendVarint(b, e.IntValue())
		case pmetric.ExemplarValueTypeDouble:
			b = append(b, byte(pmetric.ExemplarValueTypeDouble))
			b = appendFloat64(b, e.DoubleValue())
		default:
			b = append(b, byte(pmetric.ExemplarValueTypeEmpty))
		}
		tid := e.TraceID()
		b = append(b, tid[:]...)
		sid := e.SpanID()
		b = append(b, sid[:]...)
		b = appendMap(b, e.FilteredAttributes())
	}
	return b
}

func (d *decoder) exemplarsInto(es pmetric.ExemplarSlice) {
	n := d.uvarint()
	for i := uint64(0); i < n && d.err == nil; i++ {
		e := es.AppendEmpty()
		e.SetTimestamp(pcommon.Timestamp(d.fixed64()))
		switch pmetric.ExemplarValueType(d.byte()) {
		case pmetric.ExemplarValueTypeInt:
			e.SetIntValue(d.varint())
		case pmetric.ExemplarValueTypeDouble:
			e.SetDoubleValue(d.float64())
		}
		var tid pcommon.TraceID
		var sid pcommon.SpanID
		if len(d.b) < len(tid)+len(sid) {
			d.fail(errShortBuffer)
			return
		}
		copy(tid[:], d.b)
		copy(sid[:], d.b[len(tid):])
		d.b = d.b[len(tid)+len(sid):]
		e.SetTraceID(tid)
		e.SetSpanID(sid)
		d.mapInto(e.FilteredAttributes())
	}
}

func appendDPHeader(b []byte, start, ts pcommon.Timestamp, flags pmetric.DataPointFlags) []byte {
	b = appendFixed64(b, uint64(start))
	b = appendFixed64(b, uint64(ts))
	return binary.AppendUvarint(b, uint64(flags))
}

func (d *decoder) dpHeader() (start, ts pcommon.Timestamp, flags pmetric.DataPointFlags) {
	start = pcommon.Timestamp(d.fixed64())
	ts = pcommon.Timestamp(d.fixed64())
	flags = pmetric.DataPointFlags(d.uvarint())
	return start, ts, flags
}

// payloadTimestamp returns the timestamp of an encoded datapoint without
// decoding it.
func payloadTimestamp(payload []byte) (pcommon.Timestamp, error) {
	if len(payload) < 16 {
		return 0, errShortBuffer
	}
	return pcommon.Timestamp(binary.BigEndian.Uint64(payload[8:16])), nil
}

func appendOptionalStats(b []byte, has func() (bool, bool, bool), sum, minV, maxV float64) []byte {
	s, mn, mx := has()
	var mask byte
	if s {
		mask |= hasSum
	}
	if mn {
		mask |= hasMin
	}
	if mx {
		mask |= hasMax
	}
	b = append(b, mask)
	if s {
		b = appendFloat64(b, sum)
	}
	if mn {
		b = appendFloat64(b, minV)
	}
	if mx {
		b = appendFloat64(b, maxV)
	}
	return b
}

func appendNumberDP(b []byte, dp pmetric.NumberDataPoint) []byte {
	b = appendDPHeader(b, dp.StartTimestamp(), dp.Timestamp(), dp.Flags())
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		b = append(b, byte(pmetric.NumberDataPointValueTypeInt))
		b = binary.AppendVarint(b, dp.IntValue())
	case pmetric.NumberDataPointValueTypeDouble:
		b = append(b, byte(pmetric.NumberDataPointValueTypeDouble))
		b = appendFloat64(b, dp.DoubleValue())
	default:
		b = append(b, byte(pmetric.NumberDataPointValueTypeEmpty))
	}
	return appendExemplars(b, dp.Exemplars())
}

func decodeNumberDP(payload []byte, dp pmetric.NumberDataPoint) error {
	d := decoder{b: payload}
	start, ts, flags := d.dpHeader()
	dp.SetStartTimestamp(start)
	dp.SetTimestamp(ts)
	dp.SetFlags(flags)
	switch pmetric.NumberDataPointValueType(d.byte()) {
	case pmetric.NumberDataPointValueTypeInt:
		dp.SetIntValue(d.varint())
	case pmetric.NumberDataPointValueTypeDouble:
		dp.SetDoubleValue(d.float64())
	}
	d.exemplarsInto(dp.Exemplars())
	return d.err
}

func appendHistogramDP(b []byte, dp pmetric.HistogramDataPoint) []byte {
	b = appendDPHeader(b, dp.StartTimestamp(), dp.Timestamp(), dp.Flags())
	b = binary.AppendUvarint(b, dp.Count())
	b = appendOptionalStats(b, func() (bool, bool, bool) { return dp.HasSum(), dp.HasMin(), dp.HasMax() },
		dp.Sum(), dp.Min(), dp.Max())
	bounds := dp.ExplicitBounds()
	b = binary.AppendUvarint(b, uint64(bounds.Len()))
	for i := 0; i < bounds.Len(); i++ {
		b = appendFloat64(b, bounds.At(i))
	}
	counts := dp.BucketCounts()
	b = binary.AppendUvarint(b, uint64(counts.Len()))
	for i := 0; i < counts.Len(); i++ {
		b = binary.AppendUvarint(b, counts.At(i))
	}
	return appendExemplars(b, dp.Exemplars())
}

func decodeHistogramDP(payload []byte, dp pmetric.HistogramDataPoint) error {
	d := decoder{b: payload}
	start, ts, flags := d.dpHeader()
	dp.SetStartTimestamp(start)
	dp.SetTimestamp(ts)
	dp.SetFlags(flags)
	dp.SetCount(d.uvarint())
	mask := d.byte()
	if mask&hasSum != 0 {
		dp.SetSum(d.float64())
	}
	if mask&hasMin != 0 {
		dp.SetMin(d.float64())
	}
	if mask&hasMax != 0 {
		dp.SetMax(d.float64())
	}
	n := d.uvarint()
	bounds := dp.ExplicitBounds()
	bounds.EnsureCapacity(int(min(n, uint64(len(d.b)/8))))
	for i := uint64(0); i < n && d.err == nil; i++ {
		bounds.Append(d.float64())
	}
	n = d.uvarint()
	counts := dp.BucketCounts()
	counts.EnsureCapacity(int(min(n, uint64(len(d.b)))))
	for i := uint64(0); i < n && d.err == nil; i++ {
		counts.Append(d.uvarint())
	}
	d.exemplarsInto(dp.Exemplars())
	return d.err
}

func appendBuckets(b []byte, bs pmetric.ExponentialHistogramDataPointBuckets) []byte {
	b = binary.AppendVarint(b, int64(bs.Offset()))
	counts := bs.BucketCounts()
	b = binary.AppendUvarint(b, uint64(counts.Len()))
	for i := 0; i < counts.Len(); i++ {
		b = binary.AppendUvarint(b, counts.At(i))
	}
	return b
}

func (d *decoder) bucketsInto(bs pmetric.ExponentialHistogramDataPointBuckets) {
	bs.SetOffset(int32(d.varint()))
	n := d.uvarint()
	counts := bs.BucketCounts()
	counts.EnsureCapacity(int(min(n, uint64(len(d.b)))))
	for i := uint64(0); i < n && d.err == nil; i++ {
		counts.Append(d.uvarint())
	}
}

func appendExpHistogramDP(b []byte, dp pmetric.ExponentialHistogramDataPoint) []byte {
	b = appendDPHeader(b, dp.StartTimestamp(), dp.Timestamp(), dp.Flags())
	b = binary.AppendUvarint(b, dp.Count())
	b = appendOptionalStats(b, func() (bool, bool, bool) { return dp.HasSum(), dp.HasMin(), dp.HasMax() },
		dp.Sum(), dp.Min(), dp.Max())
	b = binary.AppendVarint(b, int64(dp.Scale()))
	b = binary.AppendUvarint(b, dp.ZeroCount())
	b = appendFloat64(b, dp.ZeroThreshold())
	b = appendBuckets(b, dp.Positive())
	b = appendBuckets(b, dp.Negative())
	return appendExemplars(b, dp.Exemplars())
}

func decodeExpHistogramDP(payload []byte, dp pmetric.ExponentialHistogramDataPoint) error {
	d := decoder{b: payload}
	start, ts, flags := d.dpHeader()
	dp.SetStartTimestamp(start)
	dp.SetTimestamp(ts)
	dp.SetFlags(flags)
	dp.SetCount(d.uvarint())
	mask := d.byte()
	if mask&hasSum != 0 {
		dp.SetSum(d.float64())
	}
	if mask&hasMin != 0 {
		dp.SetMin(d.float64())
	}
	if mask&hasMax != 0 {
		dp.SetMax(d.float64())
	}
	dp.SetScale(int32(d.varint()))
	dp.SetZeroCount(d.uvarint())
	dp.SetZeroThreshold(d.float64())
	d.bucketsInto(dp.Positive())
	d.bucketsInto(dp.Negative())
	d.exemplarsInto(dp.Exemplars())
	return d.err
}

func appendSummaryDP(b []byte, dp pmetric.SummaryDataPoint) []byte {
	b = appendDPHeader(b, dp.StartTimestamp(), dp.Timestamp(), dp.Flags())
	b = binary.AppendUvarint(b, dp.Count())
	b = appendFloat64(b, dp.Sum())
	qs := dp.QuantileValues()
	b = binary.AppendUvarint(b, uint64(qs.Len()))
	for i := 0; i < qs.Len(); i++ {
		b = appendFloat64(b, qs.At(i).Quantile())
		b = appendFloat64(b, qs.At(i).Value())
	}
	return b
}

func decodeSummaryDP(payload []byte, dp pmetric.SummaryDataPoint) error {
	d := decoder{b: payload}
	start, ts, flags := d.dpHeader()
	dp.SetStartTimestamp(start)
	dp.SetTimestamp(ts)
	dp.SetFlags(flags)
	dp.SetCount(d.uvarint())
	dp.SetSum(d.float64())
	n := d.uvarint()
	qs := dp.QuantileValues()
	for i := uint64(0); i < n && d.err == nil; i++ {
		q := qs.AppendEmpty()
		q.SetQuantile(d.float64())
		q.SetValue(d.float64())
	}
	return d.err
}
