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
	"encoding/binary"
	"fmt"

	"github.com/axiomhq/hyperloglog"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Tracker tracks the configured limits while merging. It records the
// observed count as well as the unique overflow counts.
type Tracker[K any] struct {
	maxCardinality uint64
	// Note that overflow buckets will NOT be counted in observed count
	// though, overflow buckets can have overflow of their own.
	observedCount  uint64
	overflowCounts *hyperloglog.Sketch
}

func NewTracker[K any](maxCardinality uint64) *Tracker[K] {
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

// MarshalBinary encodes the tracker to a byte slice. Note that only the
// observed count and the overflow estimator are encoded.
func (t *Tracker[K]) MarshalBinary() ([]byte, error) {
	var (
		hll []byte
		err error
	)
	if t.overflowCounts != nil {
		hll, err = t.overflowCounts.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal overflow estimator: %w", err)
		}
	}

	var offset int
	// 8 bytes for observed count + HLL binary encoded size
	data := make([]byte, 8+len(hll))
	binary.BigEndian.PutUint64(data[offset:], t.observedCount)
	offset += 8

	if len(hll) > 0 {
		copy(data[offset:], hll)
	}
	return data, nil
}

// UnmarshalBinary unmarshals binary encoded tracker to the go struct. Note
// that limit config and the overflow bucket ID are not encoded in the binary
// format as they could be recreated. Example usage:
//
//	t := NewTracker[identity.Resource](cfg)
//	if err := t.UnmarshalBinary(data); err != nil {
//	    panic(err)
//	}
//	t.SetOverflowBucketID(identity.OfResource(decodedResource))
func (t *Tracker[K]) UnmarshalBinary(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("failed to unmarshal observed count, invalid data of length %d received", len(data))
	}

	t.observedCount = binary.BigEndian.Uint64(data[:8])
	if len(data) == 8 {
		return nil
	}
	t.overflowCounts = hyperloglog.New14()
	if err := t.overflowCounts.UnmarshalBinary(data[8:]); err != nil {
		return fmt.Errorf("failed to unmarshal overflow estimator hll sketch: %w", err)
	}
	return nil
}
