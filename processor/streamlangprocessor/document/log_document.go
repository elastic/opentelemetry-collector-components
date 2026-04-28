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
	"encoding/hex"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// LogDocument is a Document view over a single LogRecord plus its enclosing
// ResourceLogs / ScopeLogs. The struct holds value handles into pdata, so
// mutations propagate back to the original plog.Logs.
type LogDocument struct {
	rl      plog.ResourceLogs
	sl      plog.ScopeLogs
	lr      plog.LogRecord
	dropped bool
}

// NewLogDocument returns a Document wrapping the given log record. The
// caller is responsible for passing the matching ResourceLogs and ScopeLogs
// so resource / scope path resolution works.
func NewLogDocument(rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) *LogDocument {
	return &LogDocument{rl: rl, sl: sl, lr: lr}
}

// Signal implements Document.
func (d *LogDocument) Signal() SignalKind { return SignalLogs }

// Field implements Document. Equivalent to Get(path).AsAny() but skips the
// Value struct allocation on the hot path so the executor can pass the
// LogDocument directly to condition closures with no adapter alloc.
func (d *LogDocument) Field(path string) (any, bool) {
	v, ok := d.Get(path)
	if !ok {
		return nil, false
	}
	return v.AsAny(), true
}

// Drop marks this document as dropped. The executor short-circuits on
// IsDropped — Document itself never enforces the drop.
func (d *LogDocument) Drop() { d.dropped = true }

// IsDropped reports whether Drop has been called.
func (d *LogDocument) IsDropped() bool { return d.dropped }

// Get implements Document. See package doc for path conventions.
func (d *LogDocument) Get(path string) (Value, bool) {
	prefix, rest := SplitPath(path)

	switch prefix {
	case "resource":
		return d.getResource(rest)
	case "scope":
		return d.getScope(rest)
	case "attributes":
		// Attributes path: rest is the dotted key into LogRecord.Attributes.
		if rest == "" {
			return FromPcommon(mapAsValue(d.lr.Attributes())), true
		}
		v, ok := mapGetNested(d.lr.Attributes(), rest)
		if !ok {
			return Value{}, false
		}
		return FromPcommon(v), true
	case "body":
		return d.getBody(rest)
	case "severity_text":
		if rest != "" {
			return Value{}, false
		}
		return StringValue(d.lr.SeverityText()), true
	case "severity_number":
		if rest != "" {
			return Value{}, false
		}
		return IntValue(int64(d.lr.SeverityNumber())), true
	case "time_unix_nano":
		if rest != "" {
			return Value{}, false
		}
		return IntValue(int64(d.lr.Timestamp())), true
	case "observed_time_unix_nano":
		if rest != "" {
			return Value{}, false
		}
		return IntValue(int64(d.lr.ObservedTimestamp())), true
	case "trace_id":
		if rest != "" {
			return Value{}, false
		}
		tid := d.lr.TraceID()
		if tid.IsEmpty() {
			return StringValue(""), true
		}
		return StringValue(hex.EncodeToString(tid[:])), true
	case "span_id":
		if rest != "" {
			return Value{}, false
		}
		sid := d.lr.SpanID()
		if sid.IsEmpty() {
			return StringValue(""), true
		}
		return StringValue(hex.EncodeToString(sid[:])), true
	case "flags":
		if rest != "" {
			return Value{}, false
		}
		return IntValue(int64(d.lr.Flags())), true
	case "event_name":
		if rest != "" {
			return Value{}, false
		}
		return StringValue(d.lr.EventName()), true
	}
	// Bare key — fall through to LogRecord attributes.
	v, ok := mapGetNested(d.lr.Attributes(), path)
	if !ok {
		return Value{}, false
	}
	return FromPcommon(v), true
}

// Has implements Document. Mirrors Rust runtime: returns true whenever the
// path resolves, even if the underlying value is null/empty.
func (d *LogDocument) Has(path string) bool {
	_, ok := d.Get(path)
	return ok
}

// Set implements Document.
func (d *LogDocument) Set(path string, v Value) error {
	prefix, rest := SplitPath(path)
	switch prefix {
	case "resource":
		return d.setResource(rest, v)
	case "scope":
		return d.setScope(rest, v)
	case "attributes":
		if rest == "" {
			return ErrUnsupportedTarget
		}
		mapSetNested(d.lr.Attributes(), rest, v)
		return nil
	case "body":
		return d.setBody(rest, v)
	case "severity_text":
		if rest != "" || v.Type() != ValueTypeStr {
			return ErrUnsupportedTarget
		}
		d.lr.SetSeverityText(v.Str())
		return nil
	case "severity_number":
		if rest != "" {
			return ErrUnsupportedTarget
		}
		if v.Type() != ValueTypeInt && v.Type() != ValueTypeDouble {
			return ErrUnsupportedTarget
		}
		d.lr.SetSeverityNumber(plog.SeverityNumber(v.Int()))
		return nil
	case "time_unix_nano":
		if rest != "" {
			return ErrUnsupportedTarget
		}
		if v.Type() != ValueTypeInt && v.Type() != ValueTypeDouble {
			return ErrUnsupportedTarget
		}
		d.lr.SetTimestamp(pcommon.Timestamp(v.Int()))
		return nil
	case "observed_time_unix_nano":
		if rest != "" {
			return ErrUnsupportedTarget
		}
		if v.Type() != ValueTypeInt && v.Type() != ValueTypeDouble {
			return ErrUnsupportedTarget
		}
		d.lr.SetObservedTimestamp(pcommon.Timestamp(v.Int()))
		return nil
	case "trace_id", "span_id":
		// Read-only — re-deriving the binary id from a hex string that the
		// pipeline may have rewritten is risky and unsupported by the Rust
		// runtime today. Return ErrUnsupportedTarget per spec.
		return ErrUnsupportedTarget
	case "flags":
		if rest != "" {
			return ErrUnsupportedTarget
		}
		if v.Type() != ValueTypeInt && v.Type() != ValueTypeDouble {
			return ErrUnsupportedTarget
		}
		d.lr.SetFlags(plog.LogRecordFlags(uint32(v.Int())))
		return nil
	case "event_name":
		if rest != "" || v.Type() != ValueTypeStr {
			return ErrUnsupportedTarget
		}
		d.lr.SetEventName(v.Str())
		return nil
	}
	// Bare key writes to LogRecord attributes (ES ingest compat). Use the
	// full path so dotted keys like `client.ip` get expanded into nested
	// attribute maps — same as Rust `set_nested` driven by `attributes_to_json`.
	mapSetNested(d.lr.Attributes(), path, v)
	return nil
}

// Remove implements Document.
func (d *LogDocument) Remove(path string) bool {
	prefix, rest := SplitPath(path)
	switch prefix {
	case "resource":
		return d.removeResource(rest)
	case "scope":
		return d.removeScope(rest)
	case "attributes":
		if rest == "" {
			cleared := d.lr.Attributes().Len() > 0
			d.lr.Attributes().Clear()
			return cleared
		}
		return mapRemoveNested(d.lr.Attributes(), rest)
	case "body":
		return d.removeBody(rest)
	case "severity_text":
		if rest != "" {
			return false
		}
		had := d.lr.SeverityText() != ""
		d.lr.SetSeverityText("")
		return had
	case "severity_number":
		if rest != "" {
			return false
		}
		had := d.lr.SeverityNumber() != 0
		d.lr.SetSeverityNumber(0)
		return had
	case "time_unix_nano":
		if rest != "" {
			return false
		}
		had := d.lr.Timestamp() != 0
		d.lr.SetTimestamp(0)
		return had
	case "observed_time_unix_nano":
		if rest != "" {
			return false
		}
		had := d.lr.ObservedTimestamp() != 0
		d.lr.SetObservedTimestamp(0)
		return had
	case "flags":
		if rest != "" {
			return false
		}
		had := d.lr.Flags() != 0
		d.lr.SetFlags(0)
		return had
	case "event_name":
		if rest != "" {
			return false
		}
		had := d.lr.EventName() != ""
		d.lr.SetEventName("")
		return had
	case "trace_id", "span_id":
		// No-op: ids are immutable from a Streamlang perspective.
		return false
	}
	// Bare key: try LogRecord attributes.
	return mapRemoveNested(d.lr.Attributes(), path)
}

// RemoveByPrefix implements Document.
func (d *LogDocument) RemoveByPrefix(prefix string) {
	if prefix == "" {
		return
	}
	first, rest := SplitPath(prefix)
	switch first {
	case "resource":
		if rest == "" {
			d.rl.Resource().Attributes().Clear()
			return
		}
		// resource.attributes.* etc.
		sub, subRest := SplitPath(rest)
		if sub == "attributes" {
			if subRest == "" {
				d.rl.Resource().Attributes().Clear()
				return
			}
			mapRemoveByPrefix(d.rl.Resource().Attributes(), subRest)
		}
		return
	case "scope":
		if rest == "" {
			return
		}
		sub, subRest := SplitPath(rest)
		switch sub {
		case "attributes":
			if subRest == "" {
				d.sl.Scope().Attributes().Clear()
				return
			}
			mapRemoveByPrefix(d.sl.Scope().Attributes(), subRest)
		}
		return
	case "attributes":
		if rest == "" {
			d.lr.Attributes().Clear()
			return
		}
		mapRemoveByPrefix(d.lr.Attributes(), rest)
		return
	case "body":
		// "body" or "body.x.y" — clear body if exact, otherwise remove subkey.
		if rest == "" {
			d.lr.Body().SetEmptyMap()
			d.lr.Body().Map().Clear()
			return
		}
		if d.lr.Body().Type() == pcommon.ValueTypeMap {
			mapRemoveByPrefix(d.lr.Body().Map(), rest)
		}
		return
	}
	// Bare key prefix: attributes.
	mapRemoveByPrefix(d.lr.Attributes(), prefix)
}

// --- helpers for resource / scope / body ---------------------------------

func (d *LogDocument) getResource(rest string) (Value, bool) {
	if rest == "" {
		return Value{}, false
	}
	sub, subRest := SplitPath(rest)
	if sub != "attributes" {
		return Value{}, false
	}
	if subRest == "" {
		return FromPcommon(mapAsValue(d.rl.Resource().Attributes())), true
	}
	v, ok := mapGetNested(d.rl.Resource().Attributes(), subRest)
	if !ok {
		return Value{}, false
	}
	return FromPcommon(v), true
}

func (d *LogDocument) setResource(rest string, v Value) error {
	if rest == "" {
		return ErrUnsupportedTarget
	}
	sub, subRest := SplitPath(rest)
	if sub != "attributes" || subRest == "" {
		return ErrUnsupportedTarget
	}
	mapSetNested(d.rl.Resource().Attributes(), subRest, v)
	return nil
}

func (d *LogDocument) removeResource(rest string) bool {
	if rest == "" {
		return false
	}
	sub, subRest := SplitPath(rest)
	if sub != "attributes" {
		return false
	}
	if subRest == "" {
		had := d.rl.Resource().Attributes().Len() > 0
		d.rl.Resource().Attributes().Clear()
		return had
	}
	return mapRemoveNested(d.rl.Resource().Attributes(), subRest)
}

func (d *LogDocument) getScope(rest string) (Value, bool) {
	if rest == "" {
		return Value{}, false
	}
	sub, subRest := SplitPath(rest)
	switch sub {
	case "name":
		if subRest != "" {
			return Value{}, false
		}
		return StringValue(d.sl.Scope().Name()), true
	case "version":
		if subRest != "" {
			return Value{}, false
		}
		return StringValue(d.sl.Scope().Version()), true
	case "attributes":
		if subRest == "" {
			return FromPcommon(mapAsValue(d.sl.Scope().Attributes())), true
		}
		v, ok := mapGetNested(d.sl.Scope().Attributes(), subRest)
		if !ok {
			return Value{}, false
		}
		return FromPcommon(v), true
	}
	return Value{}, false
}

func (d *LogDocument) setScope(rest string, v Value) error {
	if rest == "" {
		return ErrUnsupportedTarget
	}
	sub, subRest := SplitPath(rest)
	switch sub {
	case "name":
		if subRest != "" || v.Type() != ValueTypeStr {
			return ErrUnsupportedTarget
		}
		d.sl.Scope().SetName(v.Str())
		return nil
	case "version":
		if subRest != "" || v.Type() != ValueTypeStr {
			return ErrUnsupportedTarget
		}
		d.sl.Scope().SetVersion(v.Str())
		return nil
	case "attributes":
		if subRest == "" {
			return ErrUnsupportedTarget
		}
		mapSetNested(d.sl.Scope().Attributes(), subRest, v)
		return nil
	}
	return ErrUnsupportedTarget
}

func (d *LogDocument) removeScope(rest string) bool {
	if rest == "" {
		return false
	}
	sub, subRest := SplitPath(rest)
	switch sub {
	case "name":
		if subRest != "" {
			return false
		}
		had := d.sl.Scope().Name() != ""
		d.sl.Scope().SetName("")
		return had
	case "version":
		if subRest != "" {
			return false
		}
		had := d.sl.Scope().Version() != ""
		d.sl.Scope().SetVersion("")
		return had
	case "attributes":
		if subRest == "" {
			had := d.sl.Scope().Attributes().Len() > 0
			d.sl.Scope().Attributes().Clear()
			return had
		}
		return mapRemoveNested(d.sl.Scope().Attributes(), subRest)
	}
	return false
}

func (d *LogDocument) getBody(rest string) (Value, bool) {
	body := d.lr.Body()
	if rest == "" {
		// Whole body. For scalar bodies, expose as their primitive form.
		if body.Type() == pcommon.ValueTypeEmpty {
			return Value{}, false
		}
		return FromPcommon(body), true
	}
	if body.Type() != pcommon.ValueTypeMap {
		return Value{}, false
	}
	v, ok := mapGetNested(body.Map(), rest)
	if !ok {
		return Value{}, false
	}
	return FromPcommon(v), true
}

func (d *LogDocument) setBody(rest string, v Value) error {
	body := d.lr.Body()
	if rest == "" {
		// Replace whole body. Re-init based on incoming type.
		v.copyTo(body)
		return nil
	}
	// Auto-promote body to a Map when writing a sub-key.
	if body.Type() != pcommon.ValueTypeMap {
		body.SetEmptyMap()
	}
	mapSetNested(body.Map(), rest, v)
	return nil
}

func (d *LogDocument) removeBody(rest string) bool {
	body := d.lr.Body()
	if rest == "" {
		had := body.Type() != pcommon.ValueTypeEmpty
		body.SetEmptyMap()
		body.Map().Clear()
		// Reset to empty value: pdata Value has no SetEmpty; cleared map is
		// the closest representation. Callers checking Type() on body after
		// remove will see Map (empty).
		return had
	}
	if body.Type() != pcommon.ValueTypeMap {
		return false
	}
	return mapRemoveNested(body.Map(), rest)
}

// mapAsValue wraps a pcommon.Map as a pcommon.Value for return through Get.
// pdata doesn't expose a direct Map → Value conversion that shares storage,
// so we copy the keys into a fresh Value. This is only used on Get of a
// whole map (rare); per-key Get goes through mapGetNested which avoids the
// copy.
func mapAsValue(m pcommon.Map) pcommon.Value {
	out := pcommon.NewValueEmpty()
	dst := out.SetEmptyMap()
	m.CopyTo(dst)
	return out
}
