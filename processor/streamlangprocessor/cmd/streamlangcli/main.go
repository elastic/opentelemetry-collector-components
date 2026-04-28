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

// streamlangcli is a tiny CLI used by the cross-target consistency harness.
// It reads a JSON test case on stdin, executes the pipeline using the same
// internal packages the processor uses, and writes the resulting flat record
// (or {dropped: true}) to stdout.
//
// Input shape:
//
//	{"streamlang": {"steps": [...]}, "input": {...flat record}, "signal_type": "logs"}
//
// signal_type defaults to "logs". For Phase-6 the harness only exercises
// logs; the binary accepts the field for future expansion.
//
// Output shape:
//
//	{"output": {...flat record} | null, "dropped": false}
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/pipeline"
)

type cliInput struct {
	Streamlang map[string]any `json:"streamlang"`
	Input      map[string]any `json:"input"`
	SignalType string         `json:"signal_type,omitempty"`
}

type cliOutput struct {
	Output  map[string]any `json:"output"`
	Dropped bool           `json:"dropped"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "streamlangcli: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("read stdin: %w", err)
	}
	var in cliInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return fmt.Errorf("parse input JSON: %w", err)
	}
	if in.SignalType == "" {
		in.SignalType = "logs"
	}
	if in.SignalType != "logs" {
		return fmt.Errorf("signal_type %q not supported in this binary yet", in.SignalType)
	}

	d, err := dsl.Parse(in.Streamlang)
	if err != nil {
		return fmt.Errorf("parse DSL: %w", err)
	}
	compiled, err := pipeline.Compile(d)
	if err != nil {
		return fmt.Errorf("compile pipeline: %w", err)
	}

	// Build a single LogRecord with each input key as a top-level attribute.
	// This matches the OTTL target adapter's behaviour in
	// streamlang-consistency/src/targets/ottl.ts (buildOtlpPayload).
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	for k, v := range in.Input {
		setPcommonValue(lr.Attributes().PutEmpty(k), v)
	}

	doc := document.NewLogDocument(rl, sl, lr)
	if _, err := compiled.ExecuteBatch(context.Background(), []document.Document{doc}, nil, pipeline.FailureModeDrop, ""); err != nil {
		return fmt.Errorf("execute pipeline: %w", err)
	}

	out := cliOutput{Dropped: doc.IsDropped()}
	if !out.Dropped {
		out.Output = flattenAttributes(lr.Attributes())
	}

	enc := json.NewEncoder(os.Stdout)
	if err := enc.Encode(&out); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

// setPcommonValue assigns a Go any to a pcommon.Value, matching the wire
// representation the OTTL harness uses.
func setPcommonValue(dest pcommon.Value, v any) {
	switch x := v.(type) {
	case nil:
		// leave as Empty
	case string:
		dest.SetStr(x)
	case bool:
		dest.SetBool(x)
	case float64:
		// JSON numbers come through as float64. If integer-valued, store as Int.
		if x == float64(int64(x)) {
			dest.SetInt(int64(x))
		} else {
			dest.SetDouble(x)
		}
	case int:
		dest.SetInt(int64(x))
	case int64:
		dest.SetInt(x)
	case []any:
		s := dest.SetEmptySlice()
		for _, it := range x {
			setPcommonValue(s.AppendEmpty(), it)
		}
	case map[string]any:
		m := dest.SetEmptyMap()
		for k, v := range x {
			setPcommonValue(m.PutEmpty(k), v)
		}
	default:
		// Fallback: stringify.
		dest.SetStr(fmt.Sprint(x))
	}
}

// flattenAttributes converts a pcommon.Map into a flat Go map for JSON
// output. Nested KVList values are flattened too — the consistency harness
// compares flat records, and dotted-key children at the wire level become
// pcommon.Map values when written through Document.Set.
func flattenAttributes(m pcommon.Map) map[string]any {
	out := make(map[string]any, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		switch v.Type() {
		case pcommon.ValueTypeMap:
			child := flattenAttributes(v.Map())
			for ck, cv := range child {
				out[k+"."+ck] = cv
			}
			// Also keep the parent key present if the inner map had no entries
			if len(child) == 0 {
				out[k] = map[string]any{}
			}
		default:
			out[k] = pcommonValueAsAny(v)
		}
		return true
	})
	return out
}

func pcommonValueAsAny(v pcommon.Value) any {
	switch v.Type() {
	case pcommon.ValueTypeEmpty:
		return nil
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeBool:
		return v.Bool()
	case pcommon.ValueTypeInt:
		return v.Int()
	case pcommon.ValueTypeDouble:
		return v.Double()
	case pcommon.ValueTypeBytes:
		return v.Bytes().AsRaw()
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		out := make([]any, s.Len())
		for i := 0; i < s.Len(); i++ {
			out[i] = pcommonValueAsAny(s.At(i))
		}
		return out
	case pcommon.ValueTypeMap:
		return flattenAttributes(v.Map())
	}
	return nil
}
