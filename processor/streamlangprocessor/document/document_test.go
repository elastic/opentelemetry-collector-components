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

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// newTestLog creates an empty log record wrapped in fresh ResourceLogs/ScopeLogs.
func newTestLog() *LogDocument {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	return NewLogDocument(rl, sl, lr)
}

func TestSplitPath(t *testing.T) {
	cases := []struct {
		in           string
		prefix, rest string
	}{
		{"foo", "foo", ""},
		{"foo.bar", "foo", "bar"},
		{"a.b.c.d", "a", "b.c.d"},
		{"", "", ""},
	}
	for _, c := range cases {
		p, r := SplitPath(c.in)
		require.Equal(t, c.prefix, p, c.in)
		require.Equal(t, c.rest, r, c.in)
	}
}

func TestSignal(t *testing.T) {
	d := newTestLog()
	require.Equal(t, SignalLogs, d.Signal())
}

// --- attributes ----------------------------------------------------------

func TestSetGetSimpleAttribute(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("attributes.foo", StringValue("bar")))
	v, ok := d.Get("attributes.foo")
	require.True(t, ok)
	require.Equal(t, ValueTypeStr, v.Type())
	require.Equal(t, "bar", v.Str())
}

func TestSetGetNestedAttribute(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("attributes.user.id", IntValue(42)))
	v, ok := d.Get("attributes.user.id")
	require.True(t, ok)
	require.Equal(t, ValueTypeInt, v.Type())
	require.EqualValues(t, 42, v.Int())

	// Underlying storage must be nested map (not flat key).
	mv, ok := d.lr.Attributes().Get("user")
	require.True(t, ok)
	require.Equal(t, pcommon.ValueTypeMap, mv.Type())
	inner, ok := mv.Map().Get("id")
	require.True(t, ok)
	require.EqualValues(t, 42, inner.Int())
}

func TestRoundTripIntValue(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("attributes.x", IntValue(7)))
	v, ok := d.Get("attributes.x")
	require.True(t, ok)
	require.EqualValues(t, 7, v.Int())
}

func TestWireFlatKeyFallthrough(t *testing.T) {
	// Wire OTLP normally stores attribute keys as flat strings — populate
	// directly and confirm dotted lookup still finds them.
	d := newTestLog()
	d.lr.Attributes().PutStr("http.method", "GET")
	v, ok := d.Get("attributes.http.method")
	require.True(t, ok)
	require.Equal(t, "GET", v.Str())
}

func TestRemoveSimpleAttribute(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("attributes.foo", StringValue("bar")))
	require.True(t, d.Remove("attributes.foo"))
	_, ok := d.Get("attributes.foo")
	require.False(t, ok)
	// Second remove returns false.
	require.False(t, d.Remove("attributes.foo"))
}

func TestRemoveByPrefixAttributes(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("attributes.user.id", IntValue(1)))
	require.NoError(t, d.Set("attributes.user.name", StringValue("alice")))
	require.NoError(t, d.Set("attributes.other", StringValue("keep")))
	// Pre-populate a flat dotted key as well to exercise both paths.
	d.lr.Attributes().PutStr("user.role", "admin")

	d.RemoveByPrefix("attributes.user")
	_, ok := d.Get("attributes.user.id")
	require.False(t, ok)
	_, ok = d.Get("attributes.user.name")
	require.False(t, ok)
	_, ok = d.Get("attributes.user.role")
	require.False(t, ok)
	v, ok := d.Get("attributes.other")
	require.True(t, ok)
	require.Equal(t, "keep", v.Str())
}

func TestBareKeyWritesIntoAttributes(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("client.ip", StringValue("1.2.3.4")))
	// Lookup via attributes prefix must find it.
	v, ok := d.Get("attributes.client.ip")
	require.True(t, ok)
	require.Equal(t, "1.2.3.4", v.Str())
	// Lookup via bare key also works.
	v, ok = d.Get("client.ip")
	require.True(t, ok)
	require.Equal(t, "1.2.3.4", v.Str())

	// Confirm nested storage shape.
	clientV, ok := d.lr.Attributes().Get("client")
	require.True(t, ok)
	require.Equal(t, pcommon.ValueTypeMap, clientV.Type())
}

// --- body ---------------------------------------------------------------

func TestBodyScalarSetGet(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("body", StringValue("hello world")))
	v, ok := d.Get("body")
	require.True(t, ok)
	require.Equal(t, ValueTypeStr, v.Type())
	require.Equal(t, "hello world", v.Str())
}

func TestBodyMapAutoPromote(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("body.foo.bar", StringValue("baz")))
	v, ok := d.Get("body.foo.bar")
	require.True(t, ok)
	require.Equal(t, "baz", v.Str())
	require.Equal(t, pcommon.ValueTypeMap, d.lr.Body().Type())
}

// --- top-level fields ---------------------------------------------------

func TestSeverityTextRoundTrip(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("severity_text", StringValue("ERROR")))
	v, ok := d.Get("severity_text")
	require.True(t, ok)
	require.Equal(t, "ERROR", v.Str())
}

func TestSeverityNumberRoundTrip(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("severity_number", IntValue(9)))
	v, ok := d.Get("severity_number")
	require.True(t, ok)
	require.EqualValues(t, 9, v.Int())
}

func TestSeverityNumberWrongTypeFails(t *testing.T) {
	d := newTestLog()
	err := d.Set("severity_number", StringValue("nope"))
	require.ErrorIs(t, err, ErrUnsupportedTarget)
}

func TestTimeUnixNanoRoundTrip(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("time_unix_nano", IntValue(1234567890)))
	v, ok := d.Get("time_unix_nano")
	require.True(t, ok)
	require.EqualValues(t, 1234567890, v.Int())
}

func TestObservedTimeUnixNanoRoundTrip(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("observed_time_unix_nano", IntValue(42)))
	v, ok := d.Get("observed_time_unix_nano")
	require.True(t, ok)
	require.EqualValues(t, 42, v.Int())
}

func TestEventNameRoundTrip(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("event_name", StringValue("login")))
	v, ok := d.Get("event_name")
	require.True(t, ok)
	require.Equal(t, "login", v.Str())
}

func TestFlagsRoundTrip(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("flags", IntValue(1)))
	v, ok := d.Get("flags")
	require.True(t, ok)
	require.EqualValues(t, 1, v.Int())
}

func TestTraceIDReadOnly(t *testing.T) {
	d := newTestLog()
	err := d.Set("trace_id", StringValue("00000000000000000000000000000001"))
	require.ErrorIs(t, err, ErrUnsupportedTarget)

	// Pre-populate via direct API and confirm Get works.
	d.lr.SetTraceID(pcommon.TraceID([16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}))
	v, ok := d.Get("trace_id")
	require.True(t, ok)
	require.Equal(t, "00000000000000000000000000000001", v.Str())
}

func TestSpanIDReadOnly(t *testing.T) {
	d := newTestLog()
	err := d.Set("span_id", StringValue("0000000000000001"))
	require.ErrorIs(t, err, ErrUnsupportedTarget)
}

// --- resource / scope ---------------------------------------------------

func TestResourceAttributeRoundTrip(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("resource.attributes.service.name", StringValue("svc")))
	v, ok := d.Get("resource.attributes.service.name")
	require.True(t, ok)
	require.Equal(t, "svc", v.Str())
}

func TestScopeNameVersion(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("scope.name", StringValue("lib")))
	require.NoError(t, d.Set("scope.version", StringValue("1.2.3")))

	v, ok := d.Get("scope.name")
	require.True(t, ok)
	require.Equal(t, "lib", v.Str())
	v, ok = d.Get("scope.version")
	require.True(t, ok)
	require.Equal(t, "1.2.3", v.Str())
}

func TestScopeAttributeRoundTrip(t *testing.T) {
	d := newTestLog()
	require.NoError(t, d.Set("scope.attributes.k", IntValue(7)))
	v, ok := d.Get("scope.attributes.k")
	require.True(t, ok)
	require.EqualValues(t, 7, v.Int())
}

// --- drop ---------------------------------------------------------------

func TestDropAndIsDropped(t *testing.T) {
	d := newTestLog()
	require.False(t, d.IsDropped())
	d.Drop()
	require.True(t, d.IsDropped())
}

// --- has -----------------------------------------------------------------

func TestHasSemantics(t *testing.T) {
	d := newTestLog()
	require.False(t, d.Has("attributes.missing"))
	require.NoError(t, d.Set("attributes.present", StringValue("yes")))
	require.True(t, d.Has("attributes.present"))

	// Null value: the key is present even if its value is empty, matching
	// the Rust runtime where `has` is `get().is_some()` and Null is Some.
	require.NoError(t, d.Set("attributes.nullish", NilValue()))
	require.True(t, d.Has("attributes.nullish"))
}

// --- value constructors --------------------------------------------------

func TestFromAnyPrimitives(t *testing.T) {
	cases := map[string]struct {
		in   any
		kind ValueType
	}{
		"string": {"hi", ValueTypeStr},
		"bool":   {true, ValueTypeBool},
		"int":    {7, ValueTypeInt},
		"int64":  {int64(7), ValueTypeInt},
		"f64":    {3.14, ValueTypeDouble},
		"slice":  {[]any{"a", 1}, ValueTypeSlice},
		"map":    {map[string]any{"k": "v"}, ValueTypeMap},
		"nil":    {nil, ValueTypeEmpty},
	}
	for name, c := range cases {
		v, err := FromAny(c.in)
		require.NoError(t, err, name)
		require.Equal(t, c.kind, v.Type(), name)
	}
}

func TestFromAnyRejectsUnknown(t *testing.T) {
	type weird struct{}
	_, err := FromAny(weird{})
	require.Error(t, err)
}

func TestSliceMapAccess(t *testing.T) {
	v := SliceValue([]Value{StringValue("a"), IntValue(2)})
	got := v.Slice()
	require.Len(t, got, 2)
	require.Equal(t, "a", got[0].Str())
	require.EqualValues(t, 2, got[1].Int())

	mv := MapValue(map[string]Value{"k": StringValue("v")})
	gotM := mv.Map()
	require.Equal(t, "v", gotM["k"].Str())
}

func TestAsAnyRoundTrip(t *testing.T) {
	v := MapValue(map[string]Value{
		"s": StringValue("x"),
		"i": IntValue(1),
	})
	out := v.AsAny().(map[string]any)
	require.Equal(t, "x", out["s"])
	require.EqualValues(t, 1, out["i"])
}
