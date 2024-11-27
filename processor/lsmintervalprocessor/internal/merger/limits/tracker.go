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
	"errors"
	"fmt"

	"github.com/axiomhq/hyperloglog"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Tracker tracks the configured limits while merging. It records the
// observed count as well as the unique overflow counts.
type Tracker[K any] struct {
	cfg config.LimitConfig

	// TODO: Can we make this deterministic?
	overflowBucketID K
	// Note that overflow buckets will NOT be counted in observed count
	// though, overflow buckets can have overflow of their own.
	observedCount uint64
	// TODO (lahsivjar): This needs to be encoded as an attribute maybe?
	overflowCounts *hyperloglog.Sketch
}

func NewTracker[K any](cfg config.LimitConfig) *Tracker[K] {
	return &Tracker[K]{cfg: cfg}
}

// CheckOverflow matches the passed attributes to the configured
// attributes for the limit. Returns a boolean indicating overflow.
// It assumes that any entry passed to this method is a NEW entry
// and the check for this is left to the caller.
func (t *Tracker[K]) CheckOverflow(
	hash uint64,
	attrs pcommon.Map,
) bool {
	if !t.match(attrs) || t.cfg.MaxCardinality == 0 {
		return false
	}
	if t.observedCount == t.cfg.MaxCardinality {
		t.recordOverflow(hash)
		return true
	}
	t.observedCount++
	return false
}

func (t *Tracker[K]) Decorate(m pcommon.Map) error {
	if len(t.cfg.Overflow.Attributes) == 0 {
		return nil
	}

	var errs []error
	m.EnsureCapacity(len(t.cfg.Overflow.Attributes))
	for _, attr := range t.cfg.Overflow.Attributes {
		v := m.PutEmpty(attr.Key)
		if err := v.FromRaw(attr.Value); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to decorate overflow bucket: %w", errors.Join(errs...))
	}
	return nil
}

func (t *Tracker[K]) SetOverflowBucketID(bucketID K) {
	t.overflowBucketID = bucketID
}

func (t *Tracker[K]) GetOverflowBucketID() K {
	return t.overflowBucketID
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

func (t *Tracker[K]) match(attrs pcommon.Map) bool {
	if len(t.cfg.Attributes) == 0 {
		// If no attributes are defined then it is a match by default
		return true
	}
	var match int
	attrs.Range(func(k string, v pcommon.Value) bool {
		if _, ok := t.cfg.Attributes[k]; ok {
			match++
		}
		return match == len(t.cfg.Attributes)
	})
	return match == len(t.cfg.Attributes)
}

func (t *Tracker[K]) recordOverflow(hash uint64) {
	if t.overflowCounts == nil {
		// Creates an overflow with 14 precision
		t.overflowCounts = hyperloglog.New14()
	}
	t.overflowCounts.InsertHash(hash)
}
