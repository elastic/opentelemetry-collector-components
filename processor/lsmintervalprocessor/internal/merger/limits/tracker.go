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

package limits // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger/limits"

import (
	"fmt"

	"github.com/axiomhq/hyperloglog"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const (
	hllSketchKey = "_limits_hll_sketch"
	counterKey   = "_limits_counter"
)

// Tracker tracks the configured limits while merging. It records the
// observed count as well as the unique overflow counts.
type Tracker[K any] struct {
	maxCardinality int64
	// Note that overflow buckets will NOT be counted in observed count
	// though, overflow buckets can have overflow of their own.
	observedCount  int64
	overflowCounts *hyperloglog.Sketch
}

func NewTracker[K any](maxCardinality int64) *Tracker[K] {
	return &Tracker[K]{maxCardinality: maxCardinality}
}

// CheckOverflow checks if overflow will happen on addition of a new
// entry with the provided hash denoting the entries ID. It assumes
// that any entry passed to this method is a NEW entry and the check
// for this is left to the caller.
func (t *Tracker[K]) CheckOverflow(
	hash uint64,
	attrs pcommon.Map,
) bool {
	if t.maxCardinality == 0 {
		return false
	}
	if t.observedCount == t.maxCardinality {
		if t.overflowCounts == nil {
			// Creates an overflow with 14 precision
			t.overflowCounts = hyperloglog.New14()
		}
		t.overflowCounts.InsertHash(hash)
		return true
	}
	t.observedCount++
	return false
}

// MarshalWithPrefix marshals the tracker with a prefix. To be
// used to encode more than one limit in an attribute map.
func (t *Tracker[K]) MarshalWithPrefix(p string, m pcommon.Map) error {
	m.PutInt(p+counterKey, t.observedCount)
	if t.overflowCounts != nil {
		hll, err := t.overflowCounts.MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal limits: %w", err)
		}
		v := m.PutEmptyBytes(p + hllSketchKey)
		v.FromRaw(hll)
	}
	return nil
}

// Marshal encodes the tracker as attributes in the provided map.
func (t *Tracker[K]) Marshal(m pcommon.Map) error {
	return t.MarshalWithPrefix("", m)
}

// UnmarshalWithPrefix unmarshals the tracker encoded with a prefix.
func (t *Tracker[K]) UnmarshalWithPrefix(p string, m pcommon.Map) (err error) {
	prefixCounterKey := p + counterKey
	prefixHllKey := p + hllSketchKey
	m.RemoveIf(func(k string, v pcommon.Value) bool {
		switch k {
		case prefixCounterKey:
			t.observedCount = v.Int()
			return true
		case prefixHllKey:
			t.overflowCounts = hyperloglog.New14()
			if err := t.overflowCounts.UnmarshalBinary(v.Bytes().AsRaw()); err != nil {
				err = fmt.Errorf(
					"failed to unmarshal overflow estimator hll sketch: %w",
					err,
				)
			}
			return true
		}
		return false
	})
	return err
}

// Unmarshal unmarshals the encoded limits from the attribute map to
// the go struct and removes any attributes used in the encoding logic.
// Example usage:
//
// t := NewTracker[identity.Resource](maxCardinality)
//
//	if err := t.Unmarshal(resourceAttrs); err != nil {
//	    panic(err)
//	}
func (t *Tracker[K]) Unmarshal(m pcommon.Map) error {
	return t.UnmarshalWithPrefix("", m)
}
