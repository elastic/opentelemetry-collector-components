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
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// Document abstracts field access over a log record or a plain map.
// In pdata mode, fields map to log record attributes or body map.
// In map mode, fields map to a plain map[string]any (for testing/standalone use).
type Document struct {
	// pdata mode fields
	record   plog.LogRecord
	resource pcommon.Resource
	pfields  pcommon.Map

	// map mode fields
	mapFields map[string]any

	mapMode       bool
	transportMode bool
	dropped       bool
}

// NewDocument creates a Document in pdata mode backed by log record attributes.
func NewDocument(args ...interface{}) *Document {
	switch len(args) {
	case 1:
		// Map mode: NewDocument(map[string]any{...})
		if m, ok := args[0].(map[string]any); ok {
			return &Document{mapFields: m, mapMode: true}
		}
	case 2:
		// Pdata mode: NewDocument(record, resource)
		if record, ok := args[0].(plog.LogRecord); ok {
			if resource, ok := args[1].(pcommon.Resource); ok {
				return &Document{
					record:   record,
					resource: resource,
					pfields:  record.Attributes(),
				}
			}
		}
	}
	// Fallback: empty map mode
	return &Document{mapFields: make(map[string]any), mapMode: true}
}

// NewPdataDocument creates a Document backed by pdata log record attributes.
func NewPdataDocument(record plog.LogRecord, resource pcommon.Resource) *Document {
	return &Document{
		record:   record,
		resource: resource,
		pfields:  record.Attributes(),
	}
}

// NewTransportDocument creates a Document in transport mode (body map) for pdata.
func NewTransportDocument(record plog.LogRecord, resource pcommon.Resource) *Document {
	body := record.Body()
	if body.Type() != pcommon.ValueTypeMap {
		body.SetEmptyMap()
	}
	return &Document{
		record:        record,
		resource:      resource,
		pfields:       body.Map(),
		transportMode: true,
	}
}

// Get retrieves a field value by dot-path as a Go interface{}.
func (d *Document) Get(path string) (interface{}, bool) {
	if d.mapMode {
		return getMapValue(d.mapFields, path)
	}
	if d.transportMode {
		v, ok := getNestedPValue(d.pfields, path)
		if !ok {
			return nil, false
		}
		return valueToAny(v), true
	}
	return d.getNamespaced(path)
}

// GetAny is an alias for Get.
func (d *Document) GetAny(path string) (interface{}, bool) {
	return d.Get(path)
}

// GetPValue retrieves a field value as a pcommon.Value (pdata mode only).
func (d *Document) GetPValue(path string) (pcommon.Value, bool) {
	if d.mapMode {
		return pcommon.Value{}, false
	}
	return getNestedPValue(d.pfields, path)
}

// Set sets a field value by dot-path, creating intermediate maps as needed.
func (d *Document) Set(path string, val interface{}) {
	if d.mapMode {
		setMapValue(d.mapFields, path, val)
		return
	}
	if d.transportMode {
		setNestedValue(d.pfields, path, val)
		return
	}
	d.setNamespaced(path, val)
}

// SetValue sets a field to a pcommon.Value by dot-path (pdata mode only).
func (d *Document) SetValue(path string, val pcommon.Value) {
	if d.mapMode {
		return
	}
	setNestedPValue(d.pfields, path, val)
}

// Remove removes a field by dot-path. Returns true if the field existed.
func (d *Document) Remove(path string) bool {
	if d.mapMode {
		return removeMapValue(d.mapFields, path)
	}
	if d.transportMode {
		return removeNestedPValue(d.pfields, path)
	}
	return d.removeNamespaced(path)
}

// RemoveByPrefix removes a field and all fields with that prefix.
func (d *Document) RemoveByPrefix(prefix string) {
	if d.mapMode {
		removeMapByPrefix(d.mapFields, prefix)
		return
	}
	removeByPrefix(d.pfields, prefix)
}

// Exists checks if a field exists at the given dot-path.
func (d *Document) Exists(path string) bool {
	_, ok := d.Get(path)
	return ok
}

// Has is an alias for Exists.
func (d *Document) Has(path string) bool {
	return d.Exists(path)
}

// --- namespace-aware accessors (regular pdata mode only) ---

// resolveNamespace splits a path into the target pcommon.Map and remaining sub-path,
// or returns a special namespace string for scalar fields like @timestamp, severity.
func (d *Document) resolveNamespace(path string) (ns string, subpath string) {
	switch path {
	case "@timestamp":
		return "@timestamp", ""
	case "severity", "severity_text":
		return "severity_text", ""
	case "severity_number":
		return "severity_number", ""
	case "body":
		return "body", ""
	}

	parts := strings.SplitN(path, ".", 2)
	switch parts[0] {
	case "resource":
		if len(parts) == 2 {
			return "resource", parts[1]
		}
		return "resource", ""
	case "body":
		if len(parts) == 2 {
			return "body", parts[1]
		}
		return "body", ""
	case "attributes":
		if len(parts) == 2 {
			return "attributes", parts[1]
		}
		return "attributes", ""
	default:
		// No known prefix → default to attributes namespace
		return "attributes", path
	}
}

func (d *Document) getNamespaced(path string) (interface{}, bool) {
	ns, sub := d.resolveNamespace(path)
	switch ns {
	case "@timestamp":
		ts := d.record.Timestamp()
		if ts == 0 {
			return nil, false
		}
		return ts.AsTime().UTC().Format(time.RFC3339Nano), true
	case "severity_text":
		s := d.record.SeverityText()
		if s == "" {
			return nil, false
		}
		return s, true
	case "severity_number":
		n := d.record.SeverityNumber()
		if n == plog.SeverityNumberUnspecified {
			return nil, false
		}
		return int64(n), true
	case "body":
		body := d.record.Body()
		if sub == "" {
			switch body.Type() {
			case pcommon.ValueTypeStr:
				return body.Str(), true
			case pcommon.ValueTypeMap:
				return body.Map().AsRaw(), true
			case pcommon.ValueTypeEmpty:
				return nil, false
			default:
				return valueToAny(body), true
			}
		}
		if body.Type() != pcommon.ValueTypeMap {
			return nil, false
		}
		v, ok := getNestedPValue(body.Map(), sub)
		if !ok {
			return nil, false
		}
		return valueToAny(v), true
	case "resource":
		if sub == "" {
			return d.resource.Attributes().AsRaw(), true
		}
		v, ok := getNestedPValue(d.resource.Attributes(), sub)
		if !ok {
			return nil, false
		}
		return valueToAny(v), true
	default: // "attributes"
		if sub == "" {
			return d.pfields.AsRaw(), true
		}
		v, ok := getNestedPValue(d.pfields, sub)
		if !ok {
			return nil, false
		}
		return valueToAny(v), true
	}
}

func (d *Document) setNamespaced(path string, val interface{}) {
	ns, sub := d.resolveNamespace(path)
	switch ns {
	case "@timestamp":
		s, ok := val.(string)
		if !ok {
			s = fmt.Sprintf("%v", val)
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			// Try other common formats
			for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05Z", "2006-01-02"} {
				t, err = time.Parse(layout, s)
				if err == nil {
					break
				}
			}
			if err != nil {
				return
			}
		}
		d.record.SetTimestamp(pcommon.NewTimestampFromTime(t))
	case "severity_text":
		s, ok := val.(string)
		if !ok {
			s = fmt.Sprintf("%v", val)
		}
		d.record.SetSeverityText(s)
	case "severity_number":
		switch v := val.(type) {
		case int:
			d.record.SetSeverityNumber(plog.SeverityNumber(v))
		case int64:
			d.record.SetSeverityNumber(plog.SeverityNumber(v))
		case float64:
			d.record.SetSeverityNumber(plog.SeverityNumber(int32(v)))
		case string:
			if n, err := fmt.Sscanf(v, "%d"); err == nil {
				_ = n
			}
		}
	case "body":
		body := d.record.Body()
		if sub == "" {
			anyToValue(val, body)
			return
		}
		if body.Type() != pcommon.ValueTypeMap {
			body.SetEmptyMap()
		}
		setNestedValue(body.Map(), sub, val)
	case "resource":
		if sub == "" {
			return
		}
		setNestedValue(d.resource.Attributes(), sub, val)
	default: // "attributes"
		if sub == "" {
			return
		}
		setNestedValue(d.pfields, sub, val)
	}
}

func (d *Document) removeNamespaced(path string) bool {
	ns, sub := d.resolveNamespace(path)
	switch ns {
	case "@timestamp":
		if d.record.Timestamp() == 0 {
			return false
		}
		d.record.SetTimestamp(0)
		return true
	case "severity_text":
		if d.record.SeverityText() == "" {
			return false
		}
		d.record.SetSeverityText("")
		return true
	case "severity_number":
		if d.record.SeverityNumber() == plog.SeverityNumberUnspecified {
			return false
		}
		d.record.SetSeverityNumber(plog.SeverityNumberUnspecified)
		return true
	case "body":
		body := d.record.Body()
		if sub == "" {
			if body.Type() == pcommon.ValueTypeEmpty {
				return false
			}
			body.SetEmptyMap()
			return true
		}
		if body.Type() != pcommon.ValueTypeMap {
			return false
		}
		return removeNestedPValue(body.Map(), sub)
	case "resource":
		if sub == "" {
			return false
		}
		return removeNestedPValue(d.resource.Attributes(), sub)
	default: // "attributes"
		if sub == "" {
			return false
		}
		return removeNestedPValue(d.pfields, sub)
	}
}

// Drop marks this document for dropping.
func (d *Document) Drop() {
	d.dropped = true
}

// IsDropped returns whether this document was marked for dropping.
func (d *Document) IsDropped() bool {
	return d.dropped
}

// Fields returns the underlying pdata map for iteration (pdata mode only).
func (d *Document) Fields() pcommon.Map {
	return d.pfields
}

// --- map mode helpers ---

func getMapValue(m map[string]any, path string) (interface{}, bool) {
	parts := strings.Split(path, ".")
	current := interface{}(m)
	for _, part := range parts {
		cm, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = cm[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func setMapValue(m map[string]any, path string, val interface{}) {
	parts := strings.Split(path, ".")
	current := m
	for i := 0; i < len(parts)-1; i++ {
		next, ok := current[parts[i]]
		if !ok {
			child := make(map[string]any)
			current[parts[i]] = child
			current = child
		} else if cm, ok := next.(map[string]any); ok {
			current = cm
		} else {
			child := make(map[string]any)
			current[parts[i]] = child
			current = child
		}
	}
	current[parts[len(parts)-1]] = val
}

func removeMapValue(m map[string]any, path string) bool {
	parts := strings.Split(path, ".")
	current := m
	for i := 0; i < len(parts)-1; i++ {
		next, ok := current[parts[i]]
		if !ok {
			return false
		}
		cm, ok := next.(map[string]any)
		if !ok {
			return false
		}
		current = cm
	}
	key := parts[len(parts)-1]
	if _, ok := current[key]; !ok {
		return false
	}
	delete(current, key)
	return true
}

func removeMapByPrefix(m map[string]any, prefix string) {
	for key := range m {
		if key == prefix || strings.HasPrefix(key, prefix+".") {
			delete(m, key)
		}
	}
	parts := strings.SplitN(prefix, ".", 2)
	if len(parts) == 2 {
		if sub, ok := m[parts[0]]; ok {
			if sm, ok := sub.(map[string]any); ok {
				removeMapByPrefix(sm, parts[1])
			}
		}
	}
}

// --- pdata helpers ---

func getNestedPValue(m pcommon.Map, path string) (pcommon.Value, bool) {
	if v, ok := m.Get(path); ok {
		return v, true
	}
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		return pcommon.Value{}, false
	}
	parent, ok := m.Get(parts[0])
	if !ok || parent.Type() != pcommon.ValueTypeMap {
		return pcommon.Value{}, false
	}
	return getNestedPValue(parent.Map(), parts[1])
}

func setNestedValue(m pcommon.Map, path string, val interface{}) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		anyToValue(val, m.PutEmpty(path))
		return
	}
	parent, ok := m.Get(parts[0])
	if !ok || parent.Type() != pcommon.ValueTypeMap {
		parent = m.PutEmpty(parts[0])
		parent.SetEmptyMap()
	}
	setNestedValue(parent.Map(), parts[1], val)
}

func setNestedPValue(m pcommon.Map, path string, val pcommon.Value) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		val.CopyTo(m.PutEmpty(path))
		return
	}
	parent, ok := m.Get(parts[0])
	if !ok || parent.Type() != pcommon.ValueTypeMap {
		parent = m.PutEmpty(parts[0])
		parent.SetEmptyMap()
	}
	setNestedPValue(parent.Map(), parts[1], val)
}

func removeNestedPValue(m pcommon.Map, path string) bool {
	if _, ok := m.Get(path); ok {
		m.Remove(path)
		return true
	}
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		return false
	}
	parent, ok := m.Get(parts[0])
	if !ok || parent.Type() != pcommon.ValueTypeMap {
		return false
	}
	return removeNestedPValue(parent.Map(), parts[1])
}

func removeByPrefix(m pcommon.Map, prefix string) {
	m.RemoveIf(func(key string, _ pcommon.Value) bool {
		return key == prefix || strings.HasPrefix(key, prefix+".")
	})
	parts := strings.SplitN(prefix, ".", 2)
	if len(parts) == 2 {
		if parent, ok := m.Get(parts[0]); ok && parent.Type() == pcommon.ValueTypeMap {
			removeByPrefix(parent.Map(), parts[1])
		}
	}
}

// valueToAny converts a pcommon.Value to a Go interface{}.
func valueToAny(v pcommon.Value) interface{} {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeInt:
		return v.Int()
	case pcommon.ValueTypeDouble:
		return v.Double()
	case pcommon.ValueTypeBool:
		return v.Bool()
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		out := make([]interface{}, s.Len())
		for i := 0; i < s.Len(); i++ {
			out[i] = valueToAny(s.At(i))
		}
		return out
	case pcommon.ValueTypeMap:
		return v.Map().AsRaw()
	default:
		return nil
	}
}

// anyToValue sets a pcommon.Value from a Go interface{}.
func anyToValue(val interface{}, dest pcommon.Value) {
	switch v := val.(type) {
	case string:
		dest.SetStr(v)
	case int:
		dest.SetInt(int64(v))
	case int64:
		dest.SetInt(v)
	case float64:
		dest.SetDouble(v)
	case bool:
		dest.SetBool(v)
	case []interface{}:
		s := dest.SetEmptySlice()
		for _, item := range v {
			anyToValue(item, s.AppendEmpty())
		}
	case map[string]interface{}:
		m := dest.SetEmptyMap()
		for k, item := range v {
			anyToValue(item, m.PutEmpty(k))
		}
	default:
		dest.SetStr(fmt.Sprintf("%v", val))
	}
}
