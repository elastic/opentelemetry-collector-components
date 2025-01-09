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

const version = uint8(1)

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

// AppendBinary marshals the tracker and appends the result to b.
func (t *Tracker) AppendBinary(b []byte) ([]byte, error) {
	// TODO (lahsivjar): Estimate the size of the trackers to optimize
	// allocations. Also see: https://github.com/axiomhq/hyperloglog/issues/44
	b = slices.Grow(b, 9) // reserved for observed count
	b = append(b, version)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], t.observedCount)
	b = append(b, buf[:]...)

	if t.overflowCounts != nil {
		var err error
		b, err = t.overflowCounts.AppendBinary(b)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal limits: %w", err)
		}
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
	if len(d) < 9 {
		return errors.New("failed to unmarshal tracker, invalid length")
	}
	if v := uint8(d[0]); v != version {
		return fmt.Errorf("unsupported version: %d", v)
	}
	offset := 1
	t.observedCount = binary.BigEndian.Uint64(d[offset : offset+8])
	offset += 8
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

func (t *Trackers) AppendBinary(b []byte) ([]byte, error) {
	if t == nil || t.resource == nil {
		// if trackers is nil then nothing to marshal
		return b, nil
	}

	// TODO (lahsivjar): Estimate total required size including overflow
	// Estimate minimum without overflow:
	// - 1 byte for tracker type
	// - 8 bytes for each trackers length
	// - 8 bytes min for each tracker
	estimatedSize := (1 + len(t.scope) + len(t.scopeDPs)) * 17
	b = slices.Grow(b, estimatedSize)
	b, err := marshalTracker(resourceTracker, t.resource, b)
	if err != nil {
		return nil, err
	}
	for _, tracker := range t.scope {
		b, err = marshalTracker(scopeTracker, tracker, b)
		if err != nil {
			return nil, err
		}
	}
	for _, tracker := range t.scopeDPs {
		b, err = marshalTracker(dpsTracker, tracker, b)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
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

func marshalTracker(typ trackerType, tracker *Tracker, result []byte) ([]byte, error) {
	result = slices.Grow(result, 9)
	result = append(result, byte(typ))

	lenOffset := len(result)
	result = append(result, 0, 1, 2, 3, 4, 5, 6, 7) // make space for the length

	result, err := tracker.AppendBinary(result)
	if err != nil {
		return result, err
	}
	trackerLen := len(result) - lenOffset - 8
	binary.BigEndian.PutUint64(result[lenOffset:lenOffset+8], uint64(trackerLen))

	return result, nil
}
