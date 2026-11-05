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

package limits

import (
	"hash"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTracker(t *testing.T) {
	for _, tc := range []struct {
		name             string
		maxCardinality   uint64
		inputHashes      []uint64
		expectedOverflow uint64
	}{
		{
			name: "empty",
		},
		{
			name:           "no_overflow",
			maxCardinality: 3,
			inputHashes: []uint64{
				0x00010fffffffffff,
				0x00020fffffffffff,
				0x00030fffffffffff,
			},
			expectedOverflow: 0,
		},
		{
			name:           "overflow",
			maxCardinality: 3,
			inputHashes: []uint64{
				0x00010fffffffffff,
				0x00020fffffffffff,
				0x00030fffffffffff,
				0x00040fffffffffff,
				0x00050fffffffffff,
				0x00060fffffffffff,
				0x00050fffffffffff, // duplicate
			},
			expectedOverflow: 3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := newTracker(tc.maxCardinality, 0)
			assert.False(t, tracker.HasOverflow())
			assert.Zero(t, tracker.EstimateOverflow())
			for _, h := range tc.inputHashes {
				tracker.CheckOverflow(0, testHash(h).Hash)
			}
			if tc.expectedOverflow > 0 {
				assert.True(t, tracker.HasOverflow())
				assert.Equal(t, tc.expectedOverflow, tracker.EstimateOverflow())
			} else {
				assert.False(t, tracker.HasOverflow())
				assert.Zero(t, tracker.EstimateOverflow())
			}

			b, err := tracker.AppendBinary(nil, false)
			require.NoError(t, err)
			newTracker := newTracker(tc.maxCardinality, 0)
			_, err = newTracker.Unmarshal(b, false)
			require.NoError(t, err)
			assert.True(t, tracker.Equal(newTracker))
		})
	}
}

func TestTracker_Merge(t *testing.T) {
	for _, tc := range []struct {
		name             string
		to               *Tracker
		from             *Tracker
		expectedOverflow uint64
	}{
		{
			name: "empty",
			to: func() *Tracker {
				return newTracker(0, 0)
			}(),
			from: func() *Tracker {
				return newTracker(0, 0)
			}(),
			expectedOverflow: 0,
		},
		{
			name: "estimator_overflow",
			to: func() *Tracker {
				t := newTracker(1, 0)
				// 2 overflow, 0x0002 and 0x0003 will overflow
				t.CheckOverflow(0, testHash(0x00010fffffffffff).Hash)
				t.CheckOverflow(0, testHash(0x00020fffffffffff).Hash)
				t.CheckOverflow(0, testHash(0x00030fffffffffff).Hash)
				return t
			}(),
			from: func() *Tracker {
				t := newTracker(1, 0)
				// 2 overflow, 0x0004 and 0x0005 will overflow
				t.CheckOverflow(0, testHash(0x00030fffffffffff).Hash)
				t.CheckOverflow(0, testHash(0x00040fffffffffff).Hash)
				t.CheckOverflow(0, testHash(0x00050fffffffffff).Hash)
				return t
			}(),
			expectedOverflow: 4,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.NoError(t, tc.to.MergeEstimators(tc.from))
			if tc.expectedOverflow > 0 {
				assert.True(t, tc.to.HasOverflow())
				assert.Equal(t, tc.expectedOverflow, tc.to.EstimateOverflow())
			} else {
				assert.False(t, tc.to.HasOverflow())
			}
		})
	}
}

func TestTrackers(t *testing.T) {
	getTestTrackers := func() *Trackers {
		return NewTrackers(1, 1, 1, 1)
	}
	for _, tc := range []struct {
		name     string
		trackers *Trackers
	}{
		{
			name:     "empty",
			trackers: getTestTrackers(),
		},
		{
			name: "with_one_tracker_no_overflow",
			trackers: func() *Trackers {
				trackers := getTestTrackers()
				tr := trackers.GetResourceTracker()
				tr.CheckOverflow(0, testHash(0x00010fffffffffff).Hash)

				ts := trackers.NewScopeTracker(0)
				ts.CheckOverflow(0, testHash(0x00010fffffffffff).Hash)

				tm := ts.NewMetricTracker(0)
				tm.CheckOverflow(0, testHash(0x00010fffffffffff).Hash)

				tdps := tm.NewDatapointTracker(0)
				tdps.CheckOverflow(0, 0, testHash(0x00010fffffffffff).Hash)
				return trackers
			}(),
		},
		{
			name: "with_one_tracker_overflow",
			trackers: func() *Trackers {
				trackers := getTestTrackers()
				tr := trackers.GetResourceTracker()
				tr.CheckOverflow(0, testHash(0x00010fffffffffff).Hash)
				tr.CheckOverflow(0, testHash(0x00020fffffffffff).Hash) // will overflow

				ts := trackers.NewScopeTracker(0)
				ts.CheckOverflow(0, testHash(0x00010fffffffffff).Hash)
				ts.CheckOverflow(0, testHash(0x00020fffffffffff).Hash) // will overflow

				tm := ts.NewMetricTracker(0)
				tm.CheckOverflow(0, testHash(0x00010fffffffffff).Hash)
				tm.CheckOverflow(0, testHash(0x00020fffffffffff).Hash) // will overflow

				tdps := tm.NewDatapointTracker(0)
				tdps.CheckOverflow(0, 0, testHash(0x00010fffffffffff).Hash)
				tdps.CheckOverflow(1, 0, testHash(0x00020fffffffffff).Hash) // will overflow
				return trackers
			}(),
		},
		{
			name: "with_multiple_tracker",
			trackers: func() *Trackers {
				trackers := getTestTrackers()
				tr := trackers.GetResourceTracker()
				tr.CheckOverflow(0, testHash(0x00010fffffffffff).Hash)
				tr.CheckOverflow(0, testHash(0x00020fffffffffff).Hash) // will overflow

				ts1 := trackers.NewScopeTracker(0)
				ts1.CheckOverflow(0, testHash(0x00010fffffffffff).Hash)
				ts1.CheckOverflow(0, testHash(0x00020fffffffffff).Hash) // will overflow
				ts1.CheckOverflow(0, testHash(0x00030fffffffffff).Hash) // will overflow
				ts2 := trackers.NewScopeTracker(0)
				ts2.CheckOverflow(0, testHash(0x00010fffffffffff).Hash)
				ts3 := trackers.NewScopeTracker(0)
				ts3.CheckOverflow(0, testHash(0x00030fffffffffff).Hash)
				trackers.NewScopeTracker(0) // empty tracker

				tm1 := ts1.NewMetricTracker(0)
				tm1.CheckOverflow(0, testHash(0x00010fffffffffff).Hash)
				tm1.CheckOverflow(0, testHash(0x00020fffffffffff).Hash) // will overflow
				tm1.CheckOverflow(0, testHash(0x00030fffffffffff).Hash) // will overflow
				tm2 := ts2.NewMetricTracker(0)
				tm2.CheckOverflow(0, testHash(0x00010fffffffffff).Hash)
				tm3 := ts3.NewMetricTracker(0)
				tm3.CheckOverflow(0, testHash(0x00030fffffffffff).Hash)

				tdps1 := tm1.NewDatapointTracker(0)
				tdps1.CheckOverflow(0, 0, testHash(0x00010fffffffffff).Hash)
				tdps1.CheckOverflow(1, 0, testHash(0x00020fffffffffff).Hash) // will overflow
				tdps1.CheckOverflow(1, 0, testHash(0x00030fffffffffff).Hash) // will overflow
				tdps2 := tm2.NewDatapointTracker(0)
				tdps2.CheckOverflow(0, 0, testHash(0x00040fffffffffff).Hash)
				tdps3 := tm3.NewDatapointTracker(0)
				tdps3.CheckOverflow(0, 0, testHash(0x00050fffffffffff).Hash)
				return trackers
			}(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.trackers.AppendBinary(nil, false)
			require.NoError(t, err)

			newTrackers := getTestTrackers()
			require.NoError(t, newTrackers.Unmarshal(b, false))
			// Assert decoded trackers to be equal to the original
			assertTrackers(t, tc.trackers, newTrackers)
			// Everything stored without budgets is charged to the default budget.
			assert.Equal(t, []string{""}, newTrackers.Budgets())
		})
	}
}

func assertTrackers(t *testing.T, a, b *Trackers) {
	t.Helper()

	// Assert resource tracker
	require.True(t, a.resource.Equal(b.resource))
	// Assert number of scope trackers
	require.Equal(t, len(a.scope), len(b.scope))
	for i := range a.scope {
		// Assert scope trackers for each resource
		ast, bst := a.scope[i], b.scope[i]
		assert.Equal(t, ast.Tracker, bst.Tracker)
		// Assert metrics trackers for each scope
		require.Equal(t, len(ast.metrics), len(bst.metrics))
		for j := range ast.metrics {
			amt, bmt := ast.metrics[j], bst.metrics[j]
			assert.Equal(t, amt.Tracker, bmt.Tracker)
			// Assert dp trackers for each metric
			require.Equal(t, len(amt.datapoints), len(bmt.datapoints))
			for k := range amt.datapoints {
				assert.Equal(t, amt.datapoints[k], bmt.datapoints[k])
			}
		}
	}
}

// testHash is a test implementation of hash.Hash64 to simplify testing
// The testHash is a type alias over uint64 which signifies the return
// value of Sum64().
type testHash uint64

func (h testHash) Hash() hash.Hash64 {
	return h
}

func (h testHash) Sum64() uint64 {
	return uint64(h)
}

func (h testHash) Write(p []byte) (int, error) {
	panic("not implemented")
}

func (h testHash) Sum(b []byte) []byte {
	panic("not implemented")
}

func (h testHash) Reset() {
	panic("not implemented")
}

func (h testHash) Size() int {
	panic("not implemented")
}

func (h testHash) BlockSize() int {
	panic("not implemented")
}

func TestTracker_Budgets(t *testing.T) {
	type check struct {
		budget       uint32
		hash         uint64
		wantOverflow bool
	}
	for _, tc := range []struct {
		name           string
		maxCardinality uint64
		checks         []check
		wantEstimate   uint64
	}{
		{
			name:           "limit_applies_per_budget",
			maxCardinality: 1,
			checks: []check{
				{0, 0x1, false},
				{0, 0x2, true},
				{1, 0x3, false},
				{1, 0x4, true},
				{5, 0x5, false}, // budgets need not be contiguous
			},
			wantEstimate: 2,
		},
		{
			name:           "unlimited",
			maxCardinality: 0,
			checks: []check{
				{0, 0x1, false},
				{7, 0x2, false},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := newTracker(tc.maxCardinality, 0)
			for _, c := range tc.checks {
				assert.Equal(t, c.wantOverflow, tracker.CheckOverflow(c.budget, testHash(c.hash).Hash), "hash %x", c.hash)
			}
			assert.Equal(t, tc.wantEstimate, tracker.EstimateOverflow())

			b, err := tracker.AppendBinary(nil, true)
			require.NoError(t, err)
			decoded := newTracker(tc.maxCardinality, 0)
			n, err := decoded.Unmarshal(b, true)
			require.NoError(t, err)
			assert.Equal(t, len(b), n)
			assert.True(t, tracker.Equal(decoded))
		})
	}
}

func TestTrackers_Budgets(t *testing.T) {
	trackers := NewTrackers(2, 2, 2, 2)
	p0, p1 := trackers.BudgetIdx("p0"), trackers.BudgetIdx("p1")
	require.Equal(t, p0, trackers.BudgetIdx("p0"))
	require.Equal(t, []string{"p0", "p1"}, trackers.Budgets())
	assert.False(t, trackers.DefaultBudgetOnly())

	tr := trackers.GetResourceTracker()
	tr.CheckOverflow(p0, testHash(0x1).Hash)
	tr.CheckOverflow(p1, testHash(0x2).Hash)
	trackers.NewScopeTracker(p0)
	ts1 := trackers.NewScopeTracker(p1)
	ts1.CheckOverflow(p1, testHash(0x3).Hash)
	tm := ts1.NewMetricTracker(p1)
	tm.CheckOverflow(p0, testHash(0x4).Hash)
	tdp := tm.NewDatapointTracker(p0)
	// Datapoint budgets stay unrecorded until a datapoint lands on a
	// budget other than 0, then earlier datapoints are padded.
	tdp.CheckOverflow(0, p0, testHash(0x5).Hash)
	assert.Nil(t, tdp.datapointBudgetIdx)
	tdp.CheckOverflow(1, p1, testHash(0x6).Hash)
	tdp.CheckOverflow(2, p1, testHash(0x7).Hash)
	tdp.CheckOverflow(3, p1, testHash(0x8).Hash) // overflow, not recorded
	tdp.CheckOverflow(3, p0, testHash(0x9).Hash) // budget 0, not recorded
	assert.Equal(t, []uint32{p0, p1, p1}, tdp.datapointBudgetIdx)
	assert.Equal(t, p0, tdp.DatapointBudgetIdx(3))

	b, err := trackers.AppendBinary(nil, true)
	require.NoError(t, err)
	decoded := NewTrackers(2, 2, 2, 2)
	require.NoError(t, decoded.Unmarshal(b, true))
	assertTrackers(t, trackers, decoded)
	assert.Equal(t, trackers.Budgets(), decoded.Budgets())
	dtdp := decoded.GetScopeTracker(1).GetMetricTracker(0).GetDatapointTracker(0)
	assert.Equal(t, p0, dtdp.DatapointBudgetIdx(0))
	assert.Equal(t, p1, dtdp.DatapointBudgetIdx(1))
	assert.Equal(t, p1, dtdp.DatapointBudgetIdx(2))
	assert.Equal(t, uint32(0), dtdp.DatapointBudgetIdx(3))
}
