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
			tracker := newTracker(tc.maxCardinality)
			assert.False(t, tracker.HasOverflow())
			assert.Zero(t, tracker.EstimateOverflow())
			for _, h := range tc.inputHashes {
				tracker.CheckOverflow(testHash(h).Hash)
			}
			if tc.expectedOverflow > 0 {
				assert.True(t, tracker.HasOverflow())
				assert.Equal(t, tc.expectedOverflow, tracker.EstimateOverflow())
			} else {
				assert.False(t, tracker.HasOverflow())
				assert.Zero(t, tracker.EstimateOverflow())
			}

			b, err := tracker.Marshal()
			require.NoError(t, err)
			newTracker := newTracker(tc.maxCardinality)
			require.NoError(t, newTracker.Unmarshal(b))
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
				return newTracker(0)
			}(),
			from: func() *Tracker {
				return newTracker(0)
			}(),
			expectedOverflow: 0,
		},
		{
			name: "estimator_overflow",
			to: func() *Tracker {
				t := newTracker(1)
				// 2 overflow, 0x0002 and 0x0003 will overflow
				t.CheckOverflow(testHash(0x00010fffffffffff).Hash)
				t.CheckOverflow(testHash(0x00020fffffffffff).Hash)
				t.CheckOverflow(testHash(0x00030fffffffffff).Hash)
				return t
			}(),
			from: func() *Tracker {
				t := newTracker(1)
				// 2 overflow, 0x0004 and 0x0005 will overflow
				t.CheckOverflow(testHash(0x00030fffffffffff).Hash)
				t.CheckOverflow(testHash(0x00040fffffffffff).Hash)
				t.CheckOverflow(testHash(0x00050fffffffffff).Hash)
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
				tr.CheckOverflow(testHash(0x00010fffffffffff).Hash)

				ts := trackers.NewScopeTracker()
				ts.CheckOverflow(testHash(0x00010fffffffffff).Hash)

				tm := trackers.NewMetricTracker()
				tm.CheckOverflow(testHash(0x00010fffffffffff).Hash)

				tdps := trackers.NewDatapointTracker()
				tdps.CheckOverflow(testHash(0x00010fffffffffff).Hash)
				return trackers
			}(),
		},
		{
			name: "with_one_tracker_overflow",
			trackers: func() *Trackers {
				trackers := getTestTrackers()
				tr := trackers.GetResourceTracker()
				tr.CheckOverflow(testHash(0x00010fffffffffff).Hash)
				tr.CheckOverflow(testHash(0x00020fffffffffff).Hash) // will overflow

				ts := trackers.NewScopeTracker()
				ts.CheckOverflow(testHash(0x00010fffffffffff).Hash)
				ts.CheckOverflow(testHash(0x00020fffffffffff).Hash) // will overflow

				tm := trackers.NewMetricTracker()
				tm.CheckOverflow(testHash(0x00010fffffffffff).Hash)
				tm.CheckOverflow(testHash(0x00020fffffffffff).Hash) // will overflow

				tdps := trackers.NewDatapointTracker()
				tdps.CheckOverflow(testHash(0x00010fffffffffff).Hash)
				tdps.CheckOverflow(testHash(0x00020fffffffffff).Hash) // will overflow
				return trackers
			}(),
		},
		{
			name: "with_multiple_tracker",
			trackers: func() *Trackers {
				trackers := getTestTrackers()
				tr := trackers.GetResourceTracker()
				tr.CheckOverflow(testHash(0x00010fffffffffff).Hash)
				tr.CheckOverflow(testHash(0x00020fffffffffff).Hash) // will overflow

				ts1 := trackers.NewScopeTracker()
				ts1.CheckOverflow(testHash(0x00010fffffffffff).Hash)
				ts1.CheckOverflow(testHash(0x00020fffffffffff).Hash) // will overflow
				ts1.CheckOverflow(testHash(0x00030fffffffffff).Hash) // will overflow
				ts2 := trackers.NewScopeTracker()
				ts2.CheckOverflow(testHash(0x00010fffffffffff).Hash)
				ts3 := trackers.NewScopeTracker()
				ts3.CheckOverflow(testHash(0x00030fffffffffff).Hash) // will overflow
				trackers.NewScopeTracker()                           // empty tracker

				tm1 := trackers.NewMetricTracker()
				tm1.CheckOverflow(testHash(0x00010fffffffffff).Hash)
				tm1.CheckOverflow(testHash(0x00020fffffffffff).Hash) // will overflow
				tm1.CheckOverflow(testHash(0x00030fffffffffff).Hash) // will overflow
				tm2 := trackers.NewMetricTracker()
				tm2.CheckOverflow(testHash(0x00010fffffffffff).Hash)
				tm3 := trackers.NewMetricTracker()
				tm3.CheckOverflow(testHash(0x00030fffffffffff).Hash) // will overflow

				tdps1 := trackers.NewDatapointTracker()
				tdps1.CheckOverflow(testHash(0x00010fffffffffff).Hash)
				tdps1.CheckOverflow(testHash(0x00020fffffffffff).Hash) // will overflow
				tdps1.CheckOverflow(testHash(0x00030fffffffffff).Hash) // will overflow
				tdps2 := trackers.NewDatapointTracker()
				tdps2.CheckOverflow(testHash(0x00040fffffffffff).Hash)
				tdps3 := trackers.NewDatapointTracker()
				tdps3.CheckOverflow(testHash(0x00050fffffffffff).Hash)
				tdps4 := trackers.NewDatapointTracker()
				tdps4.CheckOverflow(testHash(0x00050fffffffffff).Hash)
				tdps4.CheckOverflow(testHash(0x00060fffffffffff).Hash) // will overflow
				return trackers
			}(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.trackers.Marshal()
			require.NoError(t, err)

			newTrackers := getTestTrackers()
			require.NoError(t, newTrackers.Unmarshal(b))
			assert.True(t, tc.trackers.resource.Equal(newTrackers.resource))
			// Assert decoded trackers to be equal to the original
			allEqual := func(ts1, ts2 []*Tracker) {
				assert.Equal(t, len(ts1), len(ts2))
				for i := range ts1 {
					assert.True(t, ts1[i].Equal(ts2[i]))
				}
			}
			allEqual(tc.trackers.scope, newTrackers.scope)
			allEqual(tc.trackers.metric, newTrackers.metric)
			allEqual(tc.trackers.datapoint, newTrackers.datapoint)
		})
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
