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
	"math"
	"slices"

	"github.com/axiomhq/hyperloglog"
)

// NoBudget marks an overflow bucket. Overflow buckets are never charged to
// a budget and are not counted as overflow when a stored value is replayed.
const NoBudget = uint32(math.MaxUint32)

// Tracker tracks the configured limits while merging. It records the
// observed count per budget as well as the unique overflow counts.
type Tracker struct {
	maxCardinality uint64
	// budget is the budget charged for the entity this tracker mirrors.
	// Unused on the resource tracker, which mirrors the whole value.
	budget uint32
	// observedCount holds the admitted children charged to budget 0 and
	// extraCounts[b-1] those charged to budget b. Budget 0 stays out of the
	// slice so a value with a single budget does not allocate per tracker.
	// Note that overflow buckets will NOT be counted in observed counts
	// though, overflow buckets can have overflow of their own.
	observedCount  uint64
	extraCounts    []uint64
	overflowCounts *hyperloglog.Sketch
}

func newTracker(maxCardinality uint64, budget uint32) *Tracker {
	return &Tracker{maxCardinality: maxCardinality, budget: budget}
}

func (t *Tracker) Equal(other *Tracker) bool {
	if t.maxCardinality != other.maxCardinality || t.budget != other.budget {
		return false
	}
	if t.observedCount != other.observedCount || !slices.Equal(t.extraCounts, other.extraCounts) {
		return false
	}
	return t.EstimateOverflow() == other.EstimateOverflow()
}

// Budget returns the budget charged for the entity this tracker mirrors.
func (t *Tracker) Budget() uint32 {
	return t.budget
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
func (t *Tracker) CheckOverflow(budget uint32, f func() hash.Hash64) bool {
	if budget == NoBudget {
		return true
	}
	if t.maxCardinality == 0 {
		return false
	}
	c := t.counter(budget)
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

// counter returns the admitted count for budget, growing extraCounts on demand.
func (t *Tracker) counter(budget uint32) *uint64 {
	if budget == 0 {
		return &t.observedCount
	}
	for int(budget) > len(t.extraCounts) {
		t.extraCounts = append(t.extraCounts, 0)
	}
	return &t.extraCounts[budget-1]
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

// AppendBinary marshals the tracker and appends the result to b. The budget
// and the counts beyond budget 0 are only written when withBudgets is set.
func (t *Tracker) AppendBinary(b []byte, withBudgets bool) ([]byte, error) {
	b = binary.AppendUvarint(b, t.observedCount)
	if withBudgets {
		b = binary.AppendUvarint(b, uint64(t.budget))
		b = binary.AppendUvarint(b, uint64(len(t.extraCounts)))
		for _, c := range t.extraCounts {
			b = binary.AppendUvarint(b, c)
		}
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
// bytes consumed. withBudgets must match the value passed to AppendBinary.
//
// Example usage:
//
//	var t Tracker
//	n, err := t.Unmarshal(data, false)
//	if err != nil {
//	    panic(err)
//	}
//	data = data[n:]
func (t *Tracker) Unmarshal(d []byte, withBudgets bool) (int, error) {
	var offset int
	observedCount, err := readUvarint(d, &offset)
	if err != nil {
		return 0, fmt.Errorf("failed to unmarshal tracker: %w", err)
	}
	t.observedCount = observedCount
	if withBudgets {
		budget, err := readUvarint(d, &offset)
		if err != nil {
			return 0, fmt.Errorf("failed to unmarshal tracker budget: %w", err)
		}
		t.budget = uint32(budget)
		n, err := readUvarint(d, &offset)
		if err != nil {
			return 0, fmt.Errorf("failed to unmarshal tracker counts: %w", err)
		}
		for range n {
			c, err := readUvarint(d, &offset)
			if err != nil {
				return 0, fmt.Errorf("failed to unmarshal tracker counts: %w", err)
			}
			t.extraCounts = append(t.extraCounts, c)
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
// for the metrics within the current scope. budget is the budget charged
// for the scope.
func (st *ScopeTracker) NewMetricTracker(budget uint32) *MetricTracker {
	mt := &MetricTracker{
		Tracker:        newTracker(st.metricLimit, budget),
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
// cardinality for the datapoints within the current metrics. budget is the
// budget charged for the metric.
func (mt *MetricTracker) NewDatapointTracker(budget uint32) *DatapointTracker {
	tracker := &DatapointTracker{Tracker: newTracker(mt.datapointLimit, budget)}
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
	// budgets[i] is the budget charged for datapoint i of the metric. It
	// stays nil while every datapoint is charged to budget 0.
	budgets []uint32
}

// CheckOverflow is Tracker.CheckOverflow for the datapoint about to become
// datapoint i of the metric, recording its budget when it is admitted.
func (dt *DatapointTracker) CheckOverflow(i int, budget uint32, f func() hash.Hash64) bool {
	if dt.Tracker.CheckOverflow(budget, f) {
		return true
	}
	if budget == 0 && dt.budgets == nil {
		return false
	}
	for len(dt.budgets) < i {
		dt.budgets = append(dt.budgets, 0)
	}
	dt.budgets = append(dt.budgets, budget)
	return false
}

// DatapointBudget returns the budget charged for datapoint i of the metric.
func (dt *DatapointTracker) DatapointBudget(i int) uint32 {
	if i < len(dt.budgets) {
		return dt.budgets[i]
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
// of each datapoint. Budgets are named by Budget and referred to by index.
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

// Budget returns the index of the named budget, registering it when new.
func (t *Trackers) Budget(name string) uint32 {
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

// DefaultBudgetOnly reports whether every entity is charged to the default
// budget, in which case the trackers can be stored without budgets.
func (t *Trackers) DefaultBudgetOnly() bool {
	return t == nil || len(t.budgets) == 0 || (len(t.budgets) == 1 && t.budgets[0] == "")
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

// NewScopeTracker creates a new scope tracker for a resource. budget is the
// budget charged for the resource.
func (t *Trackers) NewScopeTracker(budget uint32) *ScopeTracker {
	scopeTracker := &ScopeTracker{
		Tracker:        newTracker(t.scopeLimit, budget),
		metricLimit:    t.metricLimit,
		datapointLimit: t.datapointLimit,
	}
	t.scope = append(t.scope, scopeTracker)
	return scopeTracker
}

// AppendBinary marshals the trackers and appends the result to b. With
// withBudgets the budget table and per-entity budgets are written too.
func (t *Trackers) AppendBinary(b []byte, withBudgets bool) ([]byte, error) {
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

	if withBudgets {
		b = binary.AppendUvarint(b, uint64(len(t.budgets)))
		for _, name := range t.budgets {
			b = binary.AppendUvarint(b, uint64(len(name)))
			b = append(b, name...)
		}
	}
	b, err := marshalTracker(resourceTrackerType, t.resource, b, withBudgets)
	if err != nil {
		return nil, err
	}
	for _, st := range t.scope {
		b, err = marshalTracker(scopeTrackerType, st.Tracker, b, withBudgets)
		if err != nil {
			return nil, err
		}
		for _, mt := range st.metrics {
			b, err = marshalTracker(metricTrackerType, mt.Tracker, b, withBudgets)
			if err != nil {
				return nil, err
			}
			for _, dpt := range mt.datapoints {
				b, err = marshalTracker(dpTrackerType, dpt.Tracker, b, withBudgets)
				if err != nil {
					return nil, err
				}
				if withBudgets {
					b = binary.AppendUvarint(b, uint64(len(dpt.budgets)))
					for _, budget := range dpt.budgets {
						b = binary.AppendUvarint(b, uint64(budget))
					}
				}
			}
		}
	}
	return b, nil
}

// Unmarshal unmarshals the trackers. withBudgets must match the value passed
// to AppendBinary; without it every entity is charged to the default budget.
func (t *Trackers) Unmarshal(d []byte, withBudgets bool) error {
	if len(d) == 0 {
		return nil
	}

	var offset int
	if withBudgets {
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
		n, err := tracker.Unmarshal(d[offset:], withBudgets)
		if err != nil {
			return err
		}
		offset += n
		if dpTracker != nil && withBudgets {
			n, err := readUvarint(d, &offset)
			if err != nil {
				return fmt.Errorf("failed to unmarshal datapoint budgets: %w", err)
			}
			for range n {
				budget, err := readUvarint(d, &offset)
				if err != nil {
					return fmt.Errorf("failed to unmarshal datapoint budgets: %w", err)
				}
				dpTracker.budgets = append(dpTracker.budgets, uint32(budget))
			}
		}
	}
	return nil
}

func marshalTracker(typ trackerType, tracker *Tracker, result []byte, withBudgets bool) ([]byte, error) {
	result = append(result, byte(typ))
	return tracker.AppendBinary(result, withBudgets)
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
