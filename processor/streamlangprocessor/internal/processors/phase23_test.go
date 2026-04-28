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

package processors

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

// --- split ---------------------------------------------------------------

func TestSplit_Basic(t *testing.T) {
	c, err := compileSplit(&dsl.SplitProcessor{From: "attributes.csv", Separator: ","})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.csv", document.StringValue("a,b,c")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.csv")
	require.Equal(t, document.ValueTypeSlice, v.Type())
	assert.Len(t, v.Slice(), 3)
}

func TestSplit_TrimsTrailingByDefault(t *testing.T) {
	c, err := compileSplit(&dsl.SplitProcessor{From: "attributes.csv", Separator: ","})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.csv", document.StringValue("a,b,c,")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.csv")
	assert.Len(t, v.Slice(), 3)
}

func TestSplit_PreserveTrailing(t *testing.T) {
	c, err := compileSplit(&dsl.SplitProcessor{From: "attributes.csv", Separator: ",", PreserveTrailing: ptrBool(true)})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.csv", document.StringValue("a,b,c,")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.csv")
	assert.Len(t, v.Slice(), 4)
}

func TestSplit_RegexSeparator(t *testing.T) {
	c, err := compileSplit(&dsl.SplitProcessor{From: "attributes.x", Separator: `\s+`})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.x", document.StringValue("a   b\tc")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.x")
	assert.Len(t, v.Slice(), 3)
}

func TestSplit_MissingFieldRules(t *testing.T) {
	c, _ := compileSplit(&dsl.SplitProcessor{From: "attributes.x", Separator: ","})
	require.Error(t, c.Execute(newTestDoc(t)))

	c2, _ := compileSplit(&dsl.SplitProcessor{From: "attributes.x", Separator: ",", IgnoreMissing: ptrBool(true)})
	err := c2.Execute(newTestDoc(t))
	require.True(t, IsSkip(err))
}

func TestSplit_BadSeparator(t *testing.T) {
	_, err := compileSplit(&dsl.SplitProcessor{From: "x", Separator: "[unclosed"})
	require.Error(t, err)
}

// --- sort ----------------------------------------------------------------

func TestSort_Numeric(t *testing.T) {
	c, err := compileSort(&dsl.SortProcessor{From: "attributes.xs"})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.xs", document.SliceValue([]document.Value{
		document.IntValue(3), document.IntValue(1), document.IntValue(2),
	})))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.xs")
	got := v.Slice()
	assert.Equal(t, int64(1), got[0].Int())
	assert.Equal(t, int64(2), got[1].Int())
	assert.Equal(t, int64(3), got[2].Int())
}

func TestSort_Desc(t *testing.T) {
	c, err := compileSort(&dsl.SortProcessor{From: "attributes.xs", Order: "desc"})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.xs", document.SliceValue([]document.Value{
		document.StringValue("a"), document.StringValue("c"), document.StringValue("b"),
	})))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.xs")
	got := v.Slice()
	assert.Equal(t, "c", got[0].Str())
	assert.Equal(t, "b", got[1].Str())
	assert.Equal(t, "a", got[2].Str())
}

func TestSort_NotSlice(t *testing.T) {
	c, _ := compileSort(&dsl.SortProcessor{From: "attributes.x"})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.x", document.StringValue("not array")))
	require.Error(t, c.Execute(d))
}

func TestSort_BadOrder(t *testing.T) {
	_, err := compileSort(&dsl.SortProcessor{From: "x", Order: "weird"})
	require.Error(t, err)
}

// --- join ----------------------------------------------------------------

func TestJoin_Scalars(t *testing.T) {
	c, err := compileJoin(&dsl.JoinProcessor{From: []string{"attributes.a", "attributes.b", "attributes.c"}, Delimiter: "-", To: "attributes.out"})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.a", document.StringValue("x")))
	require.NoError(t, d.Set("attributes.b", document.IntValue(2)))
	require.NoError(t, d.Set("attributes.c", document.StringValue("y")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.out")
	assert.Equal(t, "x-2-y", v.Str())
}

func TestJoin_NestedSlice(t *testing.T) {
	c, _ := compileJoin(&dsl.JoinProcessor{From: []string{"attributes.a"}, Delimiter: ",", To: "attributes.out"})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.a", document.SliceValue([]document.Value{
		document.StringValue("p"), document.StringValue("q"),
	})))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.out")
	assert.Equal(t, "p,q", v.Str())
}

func TestJoin_MissingIgnore(t *testing.T) {
	c, _ := compileJoin(&dsl.JoinProcessor{From: []string{"attributes.a", "attributes.b"}, Delimiter: "-", To: "attributes.out", IgnoreMissing: ptrBool(true)})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.a", document.StringValue("x")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.out")
	assert.Equal(t, "x-", v.Str())
}

func TestJoin_MissingError(t *testing.T) {
	c, _ := compileJoin(&dsl.JoinProcessor{From: []string{"attributes.a"}, Delimiter: "-", To: "attributes.out"})
	require.Error(t, c.Execute(newTestDoc(t)))
}

// --- concat --------------------------------------------------------------

func TestConcat_Mix(t *testing.T) {
	c, err := compileConcat(&dsl.ConcatProcessor{
		From: []dsl.ConcatPart{
			{Type: "literal", Value: "user-"},
			{Type: "field", Value: "attributes.id"},
			{Type: "literal", Value: "@"},
			{Type: "field", Value: "attributes.host"},
		},
		To: "attributes.out",
	})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.id", document.IntValue(42)))
	require.NoError(t, d.Set("attributes.host", document.StringValue("example.com")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.out")
	assert.Equal(t, "user-42@example.com", v.Str())
}

func TestConcat_MissingError(t *testing.T) {
	c, _ := compileConcat(&dsl.ConcatProcessor{
		From: []dsl.ConcatPart{{Type: "field", Value: "attributes.x"}},
		To:   "attributes.out",
	})
	require.Error(t, c.Execute(newTestDoc(t)))
}

func TestConcat_MissingIgnore(t *testing.T) {
	c, _ := compileConcat(&dsl.ConcatProcessor{
		From: []dsl.ConcatPart{
			{Type: "literal", Value: "a"},
			{Type: "field", Value: "attributes.missing"},
			{Type: "literal", Value: "b"},
		},
		To:            "attributes.out",
		IgnoreMissing: ptrBool(true),
	})
	require.NoError(t, c.Execute(newTestDoc(t)))
}

// --- math ----------------------------------------------------------------

func TestMath_Basic(t *testing.T) {
	c, err := compileMath(&dsl.MathProcessor{Expression: "(attributes.a + attributes.b) * 2", To: "attributes.out"})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.a", document.DoubleValue(3)))
	require.NoError(t, d.Set("attributes.b", document.DoubleValue(4)))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.out")
	assert.Equal(t, int64(14), v.Int())
}

func TestMath_UnaryAndPrecedence(t *testing.T) {
	c, _ := compileMath(&dsl.MathProcessor{Expression: "-2 + 3 * 4", To: "attributes.out"})
	d := newTestDoc(t)
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.out")
	assert.Equal(t, int64(10), v.Int())
}

func TestMath_DivByZero(t *testing.T) {
	c, _ := compileMath(&dsl.MathProcessor{Expression: "1 / 0", To: "attributes.out"})
	require.Error(t, c.Execute(newTestDoc(t)))
}

func TestMath_MissingField(t *testing.T) {
	c, _ := compileMath(&dsl.MathProcessor{Expression: "attributes.x + 1", To: "attributes.out"})
	require.Error(t, c.Execute(newTestDoc(t)))

	c2, _ := compileMath(&dsl.MathProcessor{Expression: "attributes.x + 1", To: "attributes.out", IgnoreMissing: ptrBool(true)})
	err := c2.Execute(newTestDoc(t))
	require.True(t, IsSkip(err))
}

func TestMath_BadExpression(t *testing.T) {
	_, err := compileMath(&dsl.MathProcessor{Expression: "1 + (2", To: "x"})
	require.Error(t, err)
}

// --- json_extract --------------------------------------------------------

func TestJSONExtract_Basic(t *testing.T) {
	c, err := compileJSONExtract(&dsl.JSONExtractProcessor{
		Field: "attributes.body",
		Extractions: []dsl.JSONExtraction{
			{Selector: "user.id", TargetField: "attributes.uid", Type: "integer"},
			{Selector: "user.name", TargetField: "attributes.name"},
		},
	})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.body", document.StringValue(`{"user":{"id":42,"name":"Ada"}}`)))
	require.NoError(t, c.Execute(d))
	uid, _ := d.Get("attributes.uid")
	assert.Equal(t, int64(42), uid.Int())
	name, _ := d.Get("attributes.name")
	assert.Equal(t, "Ada", name.Str())
}

func TestJSONExtract_ArrayIndex(t *testing.T) {
	c, _ := compileJSONExtract(&dsl.JSONExtractProcessor{
		Field: "attributes.body",
		Extractions: []dsl.JSONExtraction{
			{Selector: "items[1].name", TargetField: "attributes.second"},
		},
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.body", document.StringValue(`{"items":[{"name":"a"},{"name":"b"}]}`)))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.second")
	assert.Equal(t, "b", v.Str())
}

func TestJSONExtract_MissingPathSilent(t *testing.T) {
	c, _ := compileJSONExtract(&dsl.JSONExtractProcessor{
		Field: "attributes.body",
		Extractions: []dsl.JSONExtraction{
			{Selector: "missing.path", TargetField: "attributes.out"},
		},
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.body", document.StringValue(`{"a":1}`)))
	require.NoError(t, c.Execute(d))
	_, ok := d.Get("attributes.out")
	assert.False(t, ok)
}

func TestJSONExtract_BadJSON(t *testing.T) {
	c, _ := compileJSONExtract(&dsl.JSONExtractProcessor{
		Field:       "attributes.body",
		Extractions: []dsl.JSONExtraction{{Selector: "x", TargetField: "attributes.x"}},
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.body", document.StringValue(`not json`)))
	require.Error(t, c.Execute(d))
}

// --- network_direction ---------------------------------------------------

func TestNetworkDirection_Inbound(t *testing.T) {
	c, _ := compileNetworkDirection(&dsl.NetworkDirectionProcessor{
		SourceIP:         "attributes.src",
		DestinationIP:    "attributes.dst",
		InternalNetworks: []string{"10.0.0.0/8"},
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.src", document.StringValue("8.8.8.8")))
	require.NoError(t, d.Set("attributes.dst", document.StringValue("10.1.2.3")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("network.direction")
	assert.Equal(t, "inbound", v.Str())
}

func TestNetworkDirection_Outbound(t *testing.T) {
	c, _ := compileNetworkDirection(&dsl.NetworkDirectionProcessor{
		SourceIP: "attributes.src", DestinationIP: "attributes.dst",
		InternalNetworks: []string{"10.0.0.0/8"},
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.src", document.StringValue("10.0.0.1")))
	require.NoError(t, d.Set("attributes.dst", document.StringValue("8.8.8.8")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("network.direction")
	assert.Equal(t, "outbound", v.Str())
}

func TestNetworkDirection_NamedGroupPrivate(t *testing.T) {
	c, _ := compileNetworkDirection(&dsl.NetworkDirectionProcessor{
		SourceIP: "attributes.src", DestinationIP: "attributes.dst",
		InternalNetworks: []string{"private"},
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.src", document.StringValue("192.168.1.1")))
	require.NoError(t, d.Set("attributes.dst", document.StringValue("192.168.1.2")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("network.direction")
	assert.Equal(t, "internal", v.Str())
}

func TestNetworkDirection_DynamicField(t *testing.T) {
	c, _ := compileNetworkDirection(&dsl.NetworkDirectionProcessor{
		SourceIP: "attributes.src", DestinationIP: "attributes.dst",
		InternalNetworksField: "attributes.internal",
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.internal", document.StringValue("10.0.0.0/8")))
	require.NoError(t, d.Set("attributes.src", document.StringValue("10.0.0.1")))
	require.NoError(t, d.Set("attributes.dst", document.StringValue("8.8.8.8")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("network.direction")
	assert.Equal(t, "outbound", v.Str())
}

func TestNetworkDirection_BadIP(t *testing.T) {
	c, _ := compileNetworkDirection(&dsl.NetworkDirectionProcessor{
		SourceIP: "attributes.src", DestinationIP: "attributes.dst",
		InternalNetworks: []string{"10.0.0.0/8"},
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.src", document.StringValue("not-an-ip")))
	require.NoError(t, d.Set("attributes.dst", document.StringValue("8.8.8.8")))
	require.Error(t, c.Execute(d))
}

// --- grok ----------------------------------------------------------------

func TestGrok_Basic(t *testing.T) {
	c, err := compileGrok(&dsl.GrokProcessor{
		From:     "attributes.msg",
		Patterns: []string{"%{IP:client} %{WORD:method} %{URIPATHPARAM:path}"},
	})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.msg", document.StringValue("192.168.1.1 GET /api/health")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("client")
	assert.Equal(t, "192.168.1.1", v.Str())
	v, _ = d.Get("method")
	assert.Equal(t, "GET", v.Str())
	v, _ = d.Get("path")
	assert.Equal(t, "/api/health", v.Str())
}

func TestGrok_TypedCapture(t *testing.T) {
	c, err := compileGrok(&dsl.GrokProcessor{
		From:     "attributes.msg",
		Patterns: []string{"%{NUMBER:port:int}"},
	})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.msg", document.StringValue("8080")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("port")
	assert.Equal(t, int64(8080), v.Int())
}

func TestGrok_NoMatch(t *testing.T) {
	c, _ := compileGrok(&dsl.GrokProcessor{
		From:     "attributes.msg",
		Patterns: []string{"%{IP:client}"},
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.msg", document.StringValue("not an ip")))
	require.Error(t, c.Execute(d))
}

func TestGrok_FallthroughPatterns(t *testing.T) {
	c, _ := compileGrok(&dsl.GrokProcessor{
		From:     "attributes.msg",
		Patterns: []string{"%{IP:client}", "%{WORD:word}"},
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.msg", document.StringValue("hello")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("word")
	assert.Equal(t, "hello", v.Str())
}

// --- redact --------------------------------------------------------------

func TestRedact_IP(t *testing.T) {
	c, err := compileRedact(&dsl.RedactProcessor{
		From:     "attributes.msg",
		Patterns: []string{"%{IP:client}"},
	})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.msg", document.StringValue("from 192.168.1.1 to 10.0.0.1")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.msg")
	assert.NotContains(t, v.Str(), "192.168.1.1")
	assert.Contains(t, v.Str(), "<client>")
}

func TestRedact_CustomPrefixSuffix(t *testing.T) {
	c, _ := compileRedact(&dsl.RedactProcessor{
		From:     "attributes.msg",
		Patterns: []string{"%{IP:ip}"},
		Prefix:   "[",
		Suffix:   "]",
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.msg", document.StringValue("ip=10.1.2.3")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.msg")
	assert.Contains(t, v.Str(), "[ip]")
}

func TestRedact_MissingFieldSilent(t *testing.T) {
	c, _ := compileRedact(&dsl.RedactProcessor{
		From:     "attributes.msg",
		Patterns: []string{"%{IP:ip}"},
	})
	require.NoError(t, c.Execute(newTestDoc(t)))
}

// --- dissect -------------------------------------------------------------

func TestDissect_Basic(t *testing.T) {
	c, err := compileDissect(&dsl.DissectProcessor{
		From:    "attributes.msg",
		Pattern: "%{level} - %{message}",
	})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.msg", document.StringValue("INFO - hello world")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("level")
	assert.Equal(t, "INFO", v.Str())
	v, _ = d.Get("message")
	assert.Equal(t, "hello world", v.Str())
}

func TestDissect_Skip(t *testing.T) {
	c, _ := compileDissect(&dsl.DissectProcessor{
		From:    "attributes.msg",
		Pattern: "%{?ignore} %{level}",
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.msg", document.StringValue("FOO INFO")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("level")
	assert.Equal(t, "INFO", v.Str())
	_, ok := d.Get("ignore")
	assert.False(t, ok)
}

func TestDissect_AppendKey(t *testing.T) {
	c, _ := compileDissect(&dsl.DissectProcessor{
		From:            "attributes.msg",
		Pattern:         "%{+name} %{+name}",
		AppendSeparator: " ",
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.msg", document.StringValue("Ada Lovelace")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("name")
	assert.Equal(t, "Ada Lovelace", v.Str())
}

func TestDissect_NoMatch(t *testing.T) {
	c, _ := compileDissect(&dsl.DissectProcessor{
		From:    "attributes.msg",
		Pattern: "%{a} - %{b}",
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.msg", document.StringValue("no_separator_here")))
	require.Error(t, c.Execute(d))
}

// --- date ----------------------------------------------------------------

func TestDate_ISO8601(t *testing.T) {
	c, err := compileDate(&dsl.DateProcessor{
		From:    "attributes.ts",
		Formats: []string{"ISO8601"},
	})
	require.NoError(t, err)
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.ts", document.StringValue("2026-04-27T12:34:56Z")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("@timestamp")
	require.Equal(t, document.ValueTypeStr, v.Type())
	assert.True(t, strings.HasPrefix(v.Str(), "2026-04-27T12:34:56"))
}

func TestDate_EpochMillis(t *testing.T) {
	c, _ := compileDate(&dsl.DateProcessor{
		From:    "attributes.ts",
		Formats: []string{"epoch_millis"},
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.ts", document.StringValue("1700000000000")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("@timestamp")
	require.Equal(t, document.ValueTypeStr, v.Type())
	assert.Contains(t, v.Str(), "2023")
}

func TestDate_JodaPattern(t *testing.T) {
	c, _ := compileDate(&dsl.DateProcessor{
		From:    "attributes.ts",
		Formats: []string{"yyyy-MM-dd HH:mm:ss"},
		To:      "attributes.parsed",
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.ts", document.StringValue("2026-04-27 12:34:56")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.parsed")
	assert.True(t, strings.HasPrefix(v.Str(), "2026-04-27T12:34:56"))
}

func TestDate_OutputEpochMillis(t *testing.T) {
	c, _ := compileDate(&dsl.DateProcessor{
		From:         "attributes.ts",
		Formats:      []string{"ISO8601"},
		OutputFormat: "epoch_millis",
		To:           "attributes.ms",
	})
	d := newTestDoc(t)
	require.NoError(t, d.Set("attributes.ts", document.StringValue("2026-04-27T12:34:56Z")))
	require.NoError(t, c.Execute(d))
	v, _ := d.Get("attributes.ms")
	require.Equal(t, document.ValueTypeInt, v.Type())
	assert.Greater(t, v.Int(), int64(1700000000000))
}

func TestDate_BadTimezone(t *testing.T) {
	_, err := compileDate(&dsl.DateProcessor{
		From: "x", Formats: []string{"ISO8601"}, Timezone: "Atlantis/MiddleEarth",
	})
	require.Error(t, err)
}
