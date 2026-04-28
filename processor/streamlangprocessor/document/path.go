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
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// SplitPath returns (firstSegment, rest). If the path has no dot,
// rest is empty.
func SplitPath(p string) (prefix, rest string) {
	if i := strings.IndexByte(p, '.'); i >= 0 {
		return p[:i], p[i+1:]
	}
	return p, ""
}

// mapGetNested resolves dotted path within a pcommon.Map.
//
// Lookup semantics mirror the Rust runtime: try the nested traversal first,
// and if that misses also try the flat dotted key at every level (because
// OTLP wire-format normally stores attribute keys as flat strings containing
// dots).
func mapGetNested(m pcommon.Map, path string) (pcommon.Value, bool) {
	if path == "" {
		return pcommon.Value{}, false
	}
	// Flat key fallthrough at the top level (most common case for wire OTLP).
	if v, ok := m.Get(path); ok {
		return v, true
	}
	// Walk nested.
	parts := strings.Split(path, ".")
	cur, ok := m.Get(parts[0])
	if !ok {
		return pcommon.Value{}, false
	}
	for i := 1; i < len(parts); i++ {
		// Try cumulative flat-key lookup at this level too (e.g. nested map
		// stored a key like "request.method").
		if cur.Type() == pcommon.ValueTypeMap {
			rest := strings.Join(parts[i:], ".")
			if v, ok := cur.Map().Get(rest); ok {
				return v, true
			}
			next, ok := cur.Map().Get(parts[i])
			if !ok {
				return pcommon.Value{}, false
			}
			cur = next
			continue
		}
		return pcommon.Value{}, false
	}
	return cur, true
}

// mapSetNested writes a value at a dotted path within a pcommon.Map,
// creating intermediate maps as needed. Replaces any existing leaf.
//
// Hot path: walks the dotted segments via strings.IndexByte rather than
// allocating a []string. Single-segment paths skip the walk entirely.
func mapSetNested(m pcommon.Map, path string, val Value) {
	if path == "" {
		return
	}
	if strings.IndexByte(path, '.') < 0 {
		m.Remove(path)
		dst := m.PutEmpty(path)
		val.copyTo(dst)
		return
	}
	cur := m
	i := 0
	for {
		j := strings.IndexByte(path[i:], '.')
		if j < 0 {
			leaf := path[i:]
			cur.Remove(leaf)
			dst := cur.PutEmpty(leaf)
			val.copyTo(dst)
			return
		}
		cur = ensureChildMap(cur, path[i:i+j])
		i += j + 1
	}
}

// ensureChildMap returns m[key].Map(), creating an empty map at that key if
// it doesn't exist or is of a non-map type.
func ensureChildMap(m pcommon.Map, key string) pcommon.Map {
	v, ok := m.Get(key)
	if !ok || v.Type() != pcommon.ValueTypeMap {
		m.Remove(key)
		return m.PutEmptyMap(key)
	}
	return v.Map()
}

// mapRemoveNested removes a value at a dotted path. Returns true if anything
// was removed. Mirrors get's dual semantics: tries the flat key first, then
// nested traversal.
func mapRemoveNested(m pcommon.Map, path string) bool {
	if path == "" {
		return false
	}
	// Flat key first.
	if _, ok := m.Get(path); ok {
		m.Remove(path)
		return true
	}
	parts := strings.Split(path, ".")
	if len(parts) == 1 {
		return false
	}
	// Walk to parent.
	cur, ok := m.Get(parts[0])
	if !ok {
		return false
	}
	for i := 1; i < len(parts)-1; i++ {
		if cur.Type() != pcommon.ValueTypeMap {
			return false
		}
		// Try cumulative flat key at this level.
		rest := strings.Join(parts[i:], ".")
		if _, ok := cur.Map().Get(rest); ok {
			cur.Map().Remove(rest)
			return true
		}
		next, ok := cur.Map().Get(parts[i])
		if !ok {
			return false
		}
		cur = next
	}
	if cur.Type() != pcommon.ValueTypeMap {
		return false
	}
	leaf := parts[len(parts)-1]
	if _, ok := cur.Map().Get(leaf); !ok {
		return false
	}
	cur.Map().Remove(leaf)
	return true
}

// mapHasNested reports whether anything resolves at the dotted path.
func mapHasNested(m pcommon.Map, path string) bool {
	_, ok := mapGetNested(m, path)
	return ok
}

// mapRemoveByPrefix removes any key matching `prefix` exactly or starting
// with `prefix.`. Operates on both flat and nested entries at the top level.
func mapRemoveByPrefix(m pcommon.Map, prefix string) {
	if prefix == "" {
		return
	}
	prefixDot := prefix + "."
	// Collect first to avoid mutating during Range.
	var toRemove []string
	m.Range(func(k string, _ pcommon.Value) bool {
		if k == prefix || strings.HasPrefix(k, prefixDot) {
			toRemove = append(toRemove, k)
		}
		return true
	})
	for _, k := range toRemove {
		m.Remove(k)
	}
	// Also recurse into the first segment if it's a nested map. This handles
	// `attributes.user` removing nested `user.id` under a `user` map.
	first, rest := SplitPath(prefix)
	if rest == "" {
		return
	}
	if v, ok := m.Get(first); ok && v.Type() == pcommon.ValueTypeMap {
		mapRemoveByPrefix(v.Map(), rest)
	}
}
