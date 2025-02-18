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

// AppendBinary marshals the tracker and appends the result to b.
func (t *Tracker) AppendBinary(b []byte) ([]byte, error) {
	b = binary.AppendUvarint(b, t.observedCount)

	// Make space for the sketch length. We reserve 2 bytes, which is sufficient
	// for storing the length of a precision 14 sketch.
	lenOffset := len(b)
	b = append(b, 0, 0)
	if t.overflowCounts != nil {
		var err error
		b, err = t.overflowCounts.AppendBinary(b)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal limits: %w", err)
		}
		sketchLength := len(b) - lenOffset - 2
		binary.BigEndian.PutUint16(b[lenOffset:lenOffset+2], uint16(sketchLength))
	}
	return b, nil
}

// unmarshal unmarshals the encoded limits into t, and returns the number of
// bytes consumed.
//
// Example usage:
//
//	var t Tracker
//	n, err := t.Unmarshal(data)
//	if err != nil {
//	    panic(err)
//	}
//	data = data[n:]
func (t *Tracker) Unmarshal(d []byte) (int, error) {
	observedCount, n := binary.Uvarint(d)
	if n <= 0 {
		return 0, errors.New("failed to unmarshal tracker, invalid length")
	}
	t.observedCount = observedCount
	d = d[n:]

	if len(d) < 2 {
		return 0, errors.New("failed to unmarshal tracker, invalid length")
	}
	sketchLength := int(binary.BigEndian.Uint16(d[:2]))
	d = d[2:]

	if sketchLength > 0 {
		if len(d) < sketchLength {
			return 0, errors.New("failed to unmarshal tracker, invalid length")
		}
		t.overflowCounts = hyperloglog.New14()
		if err := t.overflowCounts.UnmarshalBinary(d[:sketchLength]); err != nil {
			return 0, fmt.Errorf("failed to unmarshal tracker: %w", err)
		}
	}
	return n + 2 + sketchLength, nil
}

type trackerType uint8

const (
	resourceTracker trackerType = iota
	scopeTracker
	metricTracker
	dpTracker
)

// Trackers represent multiple tracker in an ordered structure. It takes advantage
// of the fact that pmetric DS is ordered and thus allows trackers to be created
// for each resource, scope, and datapoint independent of the pmetric datastructure.
// Note that this means that the order for pmetric and trackers are implicitly
// related and removing/adding new objects to pmetric should be accompanied by
// adding a corresponding tracker.
type Trackers struct {
	resourceLimit  uint64
	scopeLimit     uint64
	metricLimit    uint64
	datapointLimit uint64

	resource  *Tracker
	scope     []*Tracker
	metric    []*Tracker
	datapoint []*Tracker
}

func NewTrackers(resourceLimit, scopeLimit, metricLimit, datapointLimit uint64) *Trackers {
	return &Trackers{
		resourceLimit:  resourceLimit,
		scopeLimit:     scopeLimit,
		metricLimit:    metricLimit,
		datapointLimit: datapointLimit,

		// Create a resource tracker preemptively whenever a tracker is created
		resource: newTracker(resourceLimit),
	}
}

func (t *Trackers) NewScopeTracker() *Tracker {
	newTracker := newTracker(t.scopeLimit)
	t.scope = append(t.scope, newTracker)
	return newTracker
}

func (t *Trackers) NewMetricTracker() *Tracker {
	newTracker := newTracker(t.metricLimit)
	t.metric = append(t.metric, newTracker)
	return newTracker
}

func (t *Trackers) NewDatapointTracker() *Tracker {
	newTracker := newTracker(t.datapointLimit)
	t.datapoint = append(t.datapoint, newTracker)
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

func (t *Trackers) GetMetricTracker(i int) *Tracker {
	if i >= len(t.metric) {
		return nil
	}
	return t.metric[i]
}

func (t *Trackers) GetDatapointTracker(i int) *Tracker {
	if i >= len(t.datapoint) {
		return nil
	}
	return t.datapoint[i]
}

func (t *Trackers) AppendBinary(b []byte) ([]byte, error) {
	if t == nil || t.resource == nil {
		// if trackers is nil then nothing to marshal
		return b, nil
	}

	n := 1 + len(t.scope) + len(t.metric) + len(t.datapoint)
	// minimum 4 bytes per tracker (type=1, count=1+, sketch length=2)
	b = slices.Grow(b, n*4)

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
	for _, tracker := range t.metric {
		b, err = marshalTracker(metricTracker, tracker, b)
		if err != nil {
			return nil, err
		}
	}
	for _, tracker := range t.datapoint {
		b, err = marshalTracker(dpTracker, tracker, b)
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
		case metricTracker:
			tracker = t.NewMetricTracker()
		case dpTracker:
			tracker = t.NewDatapointTracker()
		default:
			return errors.New("invalid tracker found")
		}

		n, err := tracker.Unmarshal(d[offset:])
		if err != nil {
			return err
		}
		offset += n
	}
	return nil
}

func marshalTracker(typ trackerType, tracker *Tracker, result []byte) ([]byte, error) {
	result = append(result, byte(typ))
	return tracker.AppendBinary(result)
}
