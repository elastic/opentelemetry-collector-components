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
// admitted count per budget as well as the unique overflow counts.
type Tracker struct {
	maxCardinality uint64
	// budgetIdx is the index into Trackers.budgets of the budget that admitted
	// the entity this tracker hangs on (the resource for a scope tracker, the
	// scope for a metric tracker, the metric for a datapoint tracker). Read
	// back on replay so the entity is charged to the same budget. Unused on
	// the resource tracker, which hangs on the value itself.
	budgetIdx uint32
	// Admitted children per budget: budget 0 in budget0Count, budget b >= 1
	// in budgetCounts[b-1]. Budget 0 is a plain field so a value with a
	// single budget allocates nothing per tracker. Overflow buckets are
	// never admitted, though they can have overflow of their own.
	budget0Count   uint64
	budgetCounts   []uint64
	overflowCounts *hyperloglog.Sketch
}

func newTracker(maxCardinality uint64, budgetIdx uint32) *Tracker {
	return &Tracker{maxCardinality: maxCardinality, budgetIdx: budgetIdx}
}

func (t *Tracker) Equal(other *Tracker) bool {
	if t.maxCardinality != other.maxCardinality || t.budgetIdx != other.budgetIdx {
		return false
	}
	if t.budget0Count != other.budget0Count || !slices.Equal(t.budgetCounts, other.budgetCounts) {
		return false
	}
	return t.EstimateOverflow() == other.EstimateOverflow()
}

// BudgetIdx returns the index into Trackers.budgets of the budget that
// admitted the entity this tracker hangs on.
func (t *Tracker) BudgetIdx() uint32 {
	return t.budgetIdx
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
// the provided hash denoting the entries ID, charged to the given budget. It
// assumes that any entry passed to this method is a NEW entry and the check
// for this is left to the caller.
func (t *Tracker) CheckOverflow(budgetIdx uint32, f func() hash.Hash64) bool {
	if t.maxCardinality == 0 {
		return false
	}
	c := t.counter(budgetIdx)
	if *c >= t.maxCardinality {
		if t.overflowCounts == nil {
			// Creates an overflow with 14 precision
			t.overflowCounts = hyperloglog.New14()
		}
		t.overflowCounts.InsertHash(f().Sum64())
		return true
	}
	*c++
	return false
}

// counter returns the admitted count for budget, growing budgetCounts on demand.
func (t *Tracker) counter(budgetIdx uint32) *uint64 {
	if budgetIdx == 0 {
		return &t.budget0Count
	}
	t.budgetCounts = append(t.budgetCounts, make([]uint64, max(0, int(budgetIdx)-len(t.budgetCounts)))...)
	return &t.budgetCounts[budgetIdx-1]
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
	b = binary.AppendUvarint(b, t.budget0Count)
	b = binary.AppendUvarint(b, uint64(t.budgetIdx))
	b = binary.AppendUvarint(b, uint64(len(t.budgetCounts)))
	for _, c := range t.budgetCounts {
		b = binary.AppendUvarint(b, c)
	}

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

// Unmarshal unmarshals the encoded limits into t, and returns the number of
// bytes consumed. v1 reads the layout written before budgets existed: a
// count and a sketch only, with everything on budget 0.
//
// Example usage:
//
//	var t Tracker
//	n, err := t.Unmarshal(data, false)
//	if err != nil {
//	    panic(err)
//	}
//	data = data[n:]
func (t *Tracker) Unmarshal(d []byte, v1 bool) (int, error) {
	var offset int
	budget0Count, err := readUvarint(d, &offset)
	if err != nil {
		return 0, fmt.Errorf("failed to unmarshal tracker: %w", err)
	}
	t.budget0Count = budget0Count
	if !v1 {
		budgetIdx, err := readUvarint(d, &offset)
		if err != nil {
			return 0, fmt.Errorf("failed to unmarshal tracker budget index: %w", err)
		}
		t.budgetIdx = uint32(budgetIdx)
		n, err := readUvarint(d, &offset)
		if err != nil {
			return 0, fmt.Errorf("failed to unmarshal tracker counts: %w", err)
		}
		for range n {
			c, err := readUvarint(d, &offset)
			if err != nil {
				return 0, fmt.Errorf("failed to unmarshal tracker counts: %w", err)
			}
			t.budgetCounts = append(t.budgetCounts, c)
		}
	}

	if len(d) < offset+2 {
		return 0, errors.New("failed to unmarshal tracker, invalid length")
	}
	sketchLength := int(binary.BigEndian.Uint16(d[offset : offset+2]))
	offset += 2

	if sketchLength > 0 {
		if len(d) < offset+sketchLength {
			return 0, errors.New("failed to unmarshal tracker, invalid length")
		}
		t.overflowCounts = hyperloglog.New14()
		if err := t.overflowCounts.UnmarshalBinary(d[offset : offset+sketchLength]); err != nil {
			return 0, fmt.Errorf("failed to unmarshal tracker: %w", err)
		}
	}
	return offset + sketchLength, nil
}

// ScopeTracker tracks cardinality for scope metrics. They have a nested
// structure to track cardinality for each metrics for the scope and datapoints
// for the metrics.
type ScopeTracker struct {
	*Tracker
	metricLimit    uint64
	datapointLimit uint64
	metrics        []*MetricTracker
}

// NewMetricTracker creates new metric trackers to track metrics cardinality
// for the metrics within the current scope. budgetIdx is the index of the
// budget that admitted the scope.
func (st *ScopeTracker) NewMetricTracker(budgetIdx uint32) *MetricTracker {
	mt := &MetricTracker{
		Tracker:        newTracker(st.metricLimit, budgetIdx),
		datapointLimit: st.datapointLimit,
	}
	st.metrics = append(st.metrics, mt)
	return mt
}

// GetMetricTracker returns a metric tracker from the index of the metric in
// the `pmetric.ScopeMetrics` slice of the `pmetric.Metrics` model.
func (st *ScopeTracker) GetMetricTracker(i int) *MetricTracker {
	if i >= len(st.metrics) {
		return nil
	}
	return st.metrics[i]
}

// MetricTracker tracks cardinality for metrics. They have a nested structure
// to track the cardinality of each datapoint for the metrics.
type MetricTracker struct {
	*Tracker
	datapointLimit uint64
	datapoints     []*DatapointTracker
}

// NewDatapointTracker creates a new datapoint tracker to track datapoint
// cardinality for the datapoints within the current metrics. budgetIdx is
// the index of the budget that admitted the metric.
func (mt *MetricTracker) NewDatapointTracker(budgetIdx uint32) *DatapointTracker {
	tracker := &DatapointTracker{Tracker: newTracker(mt.datapointLimit, budgetIdx)}
	mt.datapoints = append(mt.datapoints, tracker)
	return tracker
}

// GetDatapointTracker returns a datapoint tracker from the index of the
// datapoint in the `pmetric.Metrics` slice of the `pmetric.Metrics` model.
func (mt *MetricTracker) GetDatapointTracker(i int) *DatapointTracker {
	if i >= len(mt.datapoints) {
		return nil
	}
	return mt.datapoints[i]
}

// DatapointTracker tracks cardinality for the datapoints of a metric. Unlike
// the levels above, datapoints have no tracker of their own, so the budget
// charged for each datapoint is recorded here.
type DatapointTracker struct {
	*Tracker
	// datapointBudgetIdx[i] is the index of the budget that admitted
	// datapoint i. It stays nil while every datapoint is on budget 0.
	datapointBudgetIdx []uint32
}

// CheckOverflow is Tracker.CheckOverflow for a datapoint of the metric. i is
// the index the datapoint gets in the metric's DataPoints slice once admitted,
// i.e. the slice length before the append; the budget is recorded under it so
// that DatapointBudgetIdx(i) answers for DataPoints().At(i).
func (dt *DatapointTracker) CheckOverflow(i int, budgetIdx uint32, f func() hash.Hash64) bool {
	if dt.Tracker.CheckOverflow(budgetIdx, f) {
		return true
	}
	if budgetIdx == 0 {
		// Positions past the end of the list read as budget 0, so only
		// other budgets are recorded; padding covers the gaps.
		return false
	}
	// The list never reaches past the datapoints already stored, so
	// i >= len(dt.datapointBudgetIdx) and the padding length is never negative.
	dt.datapointBudgetIdx = append(dt.datapointBudgetIdx, make([]uint32, i-len(dt.datapointBudgetIdx))...)
	dt.datapointBudgetIdx = append(dt.datapointBudgetIdx, budgetIdx)
	return false
}

// DatapointBudgetIdx returns the index of the budget that admitted datapoint i.
func (dt *DatapointTracker) DatapointBudgetIdx(i int) uint32 {
	if i < len(dt.datapointBudgetIdx) {
		return dt.datapointBudgetIdx[i]
	}
	return 0
}

type trackerType uint8

const (
	resourceTrackerType trackerType = iota
	scopeTrackerType
	metricTrackerType
	dpTrackerType
)

// Trackers represent multiple tracker in an ordered structure. It takes advantage
// of the fact that pmetric DS is ordered and thus allows trackers to be created
// for each resource, scope, and datapoint independent of the pmetric datastructure.
// Note that this means that the order for pmetric and trackers are implicitly
// related and removing/adding new objects to pmetric should be accompanied by
// adding a corresponding tracker. The different types of trackers are:
//
//   - Resource tracker: one for each `pmetric.Metrics`, tracks the cardinality of
//     resources as per the configured limit.
//   - Scope tracker: one for each `pmetric.ResourceMetrics`, tracks the cardinality
//     of scopes within a resource as per the configured limit.
//   - Metric tracker: one for each `pmetric.ScopeMetrics`, tracks the cardinality of
//     metrics within a scope as per the configured limit.
//   - Datapoint tracker: one for each `pmetric.Metric`, tracks the cardinality of
//     datapoints within a metric as per the configured limit.
//
// Each tracker below the resource tracker also records the budget charged for
// the pmetric entity it mirrors, and the datapoint tracker records the budget
// of each datapoint. Names live in budgets; trackers refer to them by index.
type Trackers struct {
	resourceLimit  uint64
	scopeLimit     uint64
	metricLimit    uint64
	datapointLimit uint64

	budgets  []string
	resource *Tracker
	scope    []*ScopeTracker
}

// NewTrackers creates trackers based on the configured overflow limits.
func NewTrackers(resourceLimit, scopeLimit, metricLimit, datapointLimit uint64) *Trackers {
	return &Trackers{
		resourceLimit:  resourceLimit,
		scopeLimit:     scopeLimit,
		metricLimit:    metricLimit,
		datapointLimit: datapointLimit,

		// Create a resource tracker preemptively whenever a tracker is created
		resource: newTracker(resourceLimit, 0),
	}
}

// BudgetIdx returns the index of the named budget, registering it when new.
func (t *Trackers) BudgetIdx(name string) uint32 {
	if i := slices.Index(t.budgets, name); i >= 0 {
		return uint32(i)
	}
	t.budgets = append(t.budgets, name)
	return uint32(len(t.budgets) - 1)
}

// Budgets returns the registered budget names, indexed by budget.
func (t *Trackers) Budgets() []string {
	return t.budgets
}

// GetResourceTracker returns the resource tracker.
func (t *Trackers) GetResourceTracker() *Tracker {
	return t.resource
}

// GetScopeTracker returns the scope tracker based on the index of the resouce metrics
// whose scopes are to be tracked in the `pmetric.ResourceMetrics` slice of the
// `pmetric.Metrics` datamodel for the resource whose scopes are being tracked.
func (t *Trackers) GetScopeTracker(i int) *ScopeTracker {
	if i >= len(t.scope) {
		return nil
	}
	return t.scope[i]
}

// NewScopeTracker creates a new scope tracker for a resource. budgetIdx is
// the index of the budget that admitted the resource.
func (t *Trackers) NewScopeTracker(budgetIdx uint32) *ScopeTracker {
	scopeTracker := &ScopeTracker{
		Tracker:        newTracker(t.scopeLimit, budgetIdx),
		metricLimit:    t.metricLimit,
		datapointLimit: t.datapointLimit,
	}
	t.scope = append(t.scope, scopeTracker)
	return scopeTracker
}

// AppendBinary marshals the trackers and appends the result to b.
func (t *Trackers) AppendBinary(b []byte) ([]byte, error) {
	if t == nil || t.resource == nil {
		// if trackers is nil then nothing to marshal
		return b, nil
	}

	n := 1 + len(t.scope)
	for _, st := range t.scope {
		n += len(st.metrics)
		for _, mt := range st.metrics {
			n += len(mt.datapoints)
		}
	}
	// minimum 4 bytes per tracker (type=1, count=1+, sketch length=2)
	b = slices.Grow(b, n*4)

	b = binary.AppendUvarint(b, uint64(len(t.budgets)))
	for _, name := range t.budgets {
		b = binary.AppendUvarint(b, uint64(len(name)))
		b = append(b, name...)
	}
	b, err := marshalTracker(resourceTrackerType, t.resource, b)
	if err != nil {
		return nil, err
	}
	for _, st := range t.scope {
		b, err = marshalTracker(scopeTrackerType, st.Tracker, b)
		if err != nil {
			return nil, err
		}
		for _, mt := range st.metrics {
			b, err = marshalTracker(metricTrackerType, mt.Tracker, b)
			if err != nil {
				return nil, err
			}
			for _, dpt := range mt.datapoints {
				b, err = marshalTracker(dpTrackerType, dpt.Tracker, b)
				if err != nil {
					return nil, err
				}
				b = binary.AppendUvarint(b, uint64(len(dpt.datapointBudgetIdx)))
				for _, budgetIdx := range dpt.datapointBudgetIdx {
					b = binary.AppendUvarint(b, uint64(budgetIdx))
				}
			}
		}
	}
	return b, nil
}

// Unmarshal unmarshals the trackers. v1 reads the layout written before
// budgets existed, charging every entity to the default budget.
func (t *Trackers) Unmarshal(d []byte, v1 bool) error {
	if len(d) == 0 {
		return nil
	}

	var offset int
	if !v1 {
		n, err := readUvarint(d, &offset)
		if err != nil {
			return fmt.Errorf("failed to unmarshal budgets: %w", err)
		}
		t.budgets = make([]string, 0, n)
		for range n {
			l, err := readUvarint(d, &offset)
			if err != nil || len(d) < offset+int(l) {
				return errors.New("failed to unmarshal budgets, invalid length")
			}
			t.budgets = append(t.budgets, string(d[offset:offset+int(l)]))
			offset += int(l)
		}
	} else {
		t.budgets = []string{""}
	}

	var (
		latestScopeTracker  *ScopeTracker
		latestMetricTracker *MetricTracker
	)
	for offset < len(d) {
		trackerTyp := trackerType(d[offset])
		offset += 1

		// The below code will panic with NPE if the binary encoding is
		// unexpected. The expected binary encoding must have one resource
		// tracker then scope tracker followed by metric tracker followed by
		// datapoint tracker.
		var tracker *Tracker
		var dpTracker *DatapointTracker
		switch trackerTyp {
		case resourceTrackerType:
			tracker = t.GetResourceTracker()
		case scopeTrackerType:
			latestScopeTracker = t.NewScopeTracker(0)
			tracker = latestScopeTracker.Tracker
			// Nil the previous metric tracker as we expect a new metric
			// tracker for the new scope.
			latestMetricTracker = nil
		case metricTrackerType:
			latestMetricTracker = latestScopeTracker.NewMetricTracker(0)
			tracker = latestMetricTracker.Tracker
		case dpTrackerType:
			dpTracker = latestMetricTracker.NewDatapointTracker(0)
			tracker = dpTracker.Tracker
		default:
			return errors.New("invalid tracker found")
		}
		n, err := tracker.Unmarshal(d[offset:], v1)
		if err != nil {
			return err
		}
		offset += n
		if dpTracker != nil && !v1 {
			n, err := readUvarint(d, &offset)
			if err != nil {
				return fmt.Errorf("failed to unmarshal datapoint budget indexes: %w", err)
			}
			for range n {
				budgetIdx, err := readUvarint(d, &offset)
				if err != nil {
					return fmt.Errorf("failed to unmarshal datapoint budget indexes: %w", err)
				}
				dpTracker.datapointBudgetIdx = append(dpTracker.datapointBudgetIdx, uint32(budgetIdx))
			}
		}
	}
	return nil
}

func marshalTracker(typ trackerType, tracker *Tracker, result []byte) ([]byte, error) {
	result = append(result, byte(typ))
	return tracker.AppendBinary(result)
}

// readUvarint decodes the uvarint at *offset and advances it.
func readUvarint(d []byte, offset *int) (uint64, error) {
	v, n := binary.Uvarint(d[*offset:])
	if n <= 0 {
		return 0, errors.New("invalid length")
	}
	*offset += n
	return v, nil
}
