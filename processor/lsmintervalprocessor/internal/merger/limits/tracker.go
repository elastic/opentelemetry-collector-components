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
	"hash"
	"slices"

	"github.com/axiomhq/hyperloglog"
)

// Tracker tracks the configured limits while merging. It records the
// observed count as well as the unique overflow counts.
type Tracker struct {
	maxCardinality uint64
	// Note that overflow buckets will NOT be counted in observed count
	// though, overflow buckets can have overflow of their own.
	observedCount  uint64
	overflowCounts *hyperloglog.Sketch
}

func newTracker(maxCardinality uint64) *Tracker {
	return &Tracker{maxCardinality: maxCardinality}
}

func (t *Tracker) Equal(other *Tracker) bool {
	if t.maxCardinality != other.maxCardinality {
		return false
	}
	if t.observedCount != other.observedCount {
		return false
	}
	return t.EstimateOverflow() == other.EstimateOverflow()
}

func (t *Tracker) HasOverflow() bool {
	return t.overflowCounts != nil
}

func (t *Tracker) EstimateOverflow() uint64 {
	if t.overflowCounts == nil {
		return 0
	}
	return t.overflowCounts.Estimate()
}

// CheckOverflow checks if overflow will happen on addition of a new entry with
// the provided hash denoting the entries ID. It assumes that any entry passed
// to this method is a NEW entry and the check for this is left to the caller.
func (t *Tracker) CheckOverflow(f func() hash.Hash64) bool {
	if t.maxCardinality == 0 {
		return false
	}
	if t.observedCount == t.maxCardinality {
		if t.overflowCounts == nil {
			// Creates an overflow with 14 precision
			t.overflowCounts = hyperloglog.New14()
		}
		t.overflowCounts.InsertHash(f().Sum64())
		return true
	}
	t.observedCount++
	return false
}

// MergeEstimators merges the overflow estimators for the two trackers.
// Note that other required maintenance of the tracker for merge needs to
// done by the caller.
func (t *Tracker) MergeEstimators(other *Tracker) error {
	if other.overflowCounts == nil {
		// nothing to merge
		return nil
	}
	if t.overflowCounts == nil {
		t.overflowCounts = other.overflowCounts.Clone()
		return nil
	}
	return t.overflowCounts.Merge(other.overflowCounts)
}

// Marshal marshals the tracker to a byte slice.
func (t *Tracker) Marshal() ([]byte, error) {
	// TODO (lahsivjar): Estimate the size of the trackers to optimize
	// allocations. Also see: https://github.com/axiomhq/hyperloglog/issues/44
	b := make([]byte, 8) // reserved for observed count

	binary.BigEndian.PutUint64(b[:8], t.observedCount)
	offset := 8
	if t.overflowCounts != nil {
		hll, err := t.overflowCounts.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal limits: %w", err)
		}
		b = slices.Grow(b, len(hll))[:offset+len(hll)]
		copy(b[offset:], hll)
	}
	return b, nil
}

// Unmarshal unmarshals the encoded limits to go struct. Example usage:
//
//	var t Tracker
//	if err := t.Unmarshal(data); err != nil {
//	    panic(err)
//	}
func (t *Tracker) Unmarshal(d []byte) error {
	if len(d) < 8 {
		return errors.New("failed to unmarshal tracker, invalid length")
	}

	t.observedCount = binary.BigEndian.Uint64(d[:8])
	offset := 8
	if len(d) == offset {
		return nil
	}
	t.overflowCounts = hyperloglog.New14()
	if err := t.overflowCounts.UnmarshalBinary(d[offset:]); err != nil {
		return fmt.Errorf("failed to unmarshal tracker: %w", err)
	}
	return nil
}

type trackerType uint8

const (
	resourceTracker trackerType = iota
	scopeTracker
	dpsTracker
)

// Trackers represent multiple tracker in an ordered structure. It takes advantage
// of the fact that pmetric DS is ordered and thus allows trackers to be created
// for each resource, scope, and datapoint independent of the pmetric datastructure.
// Note that this means that the order for pmetric and trackers are implicitly
// related and removing/adding new objects to pmetric should be accompanied by
// adding a corresponding tracker.
type Trackers struct {
	resourceLimit uint64
	scopeLimit    uint64
	scopeDPLimit  uint64

	resource *Tracker
	scope    []*Tracker
	scopeDPs []*Tracker
}

func NewTrackers(resourceLimit, scopeLimit, scopeDPLimit uint64) *Trackers {
	return &Trackers{
		resourceLimit: resourceLimit,
		scopeLimit:    scopeLimit,
		scopeDPLimit:  scopeDPLimit,

		// Create a resource tracker preemptively whenever a tracker is created
		resource: newTracker(resourceLimit),
	}
}

func (t *Trackers) NewScopeTracker() *Tracker {
	newTracker := newTracker(t.scopeLimit)
	t.scope = append(t.scope, newTracker)
	return newTracker
}

func (t *Trackers) NewScopeDPsTracker() *Tracker {
	newTracker := newTracker(t.scopeDPLimit)
	t.scopeDPs = append(t.scopeDPs, newTracker)
	return newTracker
}

func (t *Trackers) GetResourceTracker() *Tracker {
	return t.resource
}

func (t *Trackers) GetScopeTracker(i int) *Tracker {
	if i >= len(t.scope) {
		return nil
	}
	return t.scope[i]
}

func (t *Trackers) GetScopeDPsTracker(i int) *Tracker {
	if i >= len(t.scopeDPs) {
		return nil
	}
	return t.scopeDPs[i]
}

func (t *Trackers) Marshal() ([]byte, error) {
	if t == nil || t.resource == nil {
		// if trackers is nil then nothing to marshal
		return nil, nil
	}

	// TODO (lahsivjar): Estimate total required size including overflow
	// Estimate minimum without overflow:
	// - 1 byte for tracker type
	// - 8 bytes for each trackers length
	// - 8 bytes min for each tracker
	estimatedSize := (1 + len(t.scope) + len(t.scopeDPs)) * 17
	result := make([]byte, 0, estimatedSize)

	var (
		offset int
		err    error
	)
	result, offset, err = marshalTracker(resourceTracker, t.resource, result, offset)
	if err != nil {
		return nil, err
	}
	for _, tracker := range t.scope {
		result, offset, err = marshalTracker(scopeTracker, tracker, result, offset)
		if err != nil {
			return nil, err
		}
	}
	for _, tracker := range t.scopeDPs {
		result, offset, err = marshalTracker(dpsTracker, tracker, result, offset)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (t *Trackers) Unmarshal(d []byte) error {
	if len(d) == 0 {
		return nil
	}

	var offset int
	for offset < len(d) {
		trackerTyp := trackerType(d[offset])
		offset += 1
		// Create the required tracker
		var tracker *Tracker
		switch trackerTyp {
		case resourceTracker:
			tracker = t.GetResourceTracker()
		case scopeTracker:
			tracker = t.NewScopeTracker()
		case dpsTracker:
			tracker = t.NewScopeDPsTracker()
		default:
			return errors.New("invalid tracker found")
		}

		trackerLen := int(binary.BigEndian.Uint64(d[offset : offset+8]))
		offset += 8
		if err := tracker.Unmarshal(d[offset : offset+trackerLen]); err != nil {
			return err
		}
		offset += int(trackerLen)
	}
	return nil
}

func marshalTracker(
	typ trackerType,
	tracker *Tracker,
	result []byte,
	offset int,
) ([]byte, int, error) {
	b, err := tracker.Marshal()
	if err != nil {
		return result, offset, err
	}
	// Encode the tracker type, length of the tracker, and the tracker
	totalEncodeLen := 1 + 8 + len(b)
	result = slices.Grow(result, totalEncodeLen)[:offset+totalEncodeLen]
	result[offset] = byte(typ)
	offset += 1
	binary.BigEndian.PutUint64(result[offset:], uint64(len(b)))
	offset += 8
	// Encode the tracker
	offset += copy(result[offset:], b)
	return result, offset, nil
}
