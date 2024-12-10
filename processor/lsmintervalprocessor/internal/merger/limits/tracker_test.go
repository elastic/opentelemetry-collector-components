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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTracker(t *testing.T) {
	for _, tc := range []struct {
		name             string
		maxCardinality   int64
		inputHashes      []uint64
		expectedOverflow uint64
	}{
		{
			name: "empty",
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
			tracker := NewTracker(tc.maxCardinality)
			assert.False(t, tracker.HasOverflow())
			assert.Zero(t, tracker.EstimateOverflow())
			for _, h := range tc.inputHashes {
				tracker.CheckOverflow(h)
			}
			if tc.expectedOverflow > 0 {
				assert.True(t, tracker.HasOverflow())
				assert.Equal(t, tc.expectedOverflow, tracker.EstimateOverflow())
			} else {
				assert.False(t, tracker.HasOverflow())
				assert.Zero(t, tracker.EstimateOverflow())
			}
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
				return NewTracker(0)
			}(),
			from: func() *Tracker {
				return NewTracker(0)
			}(),
			expectedOverflow: 0,
		},
		{
			name: "estimator_overflow",
			to: func() *Tracker {
				t := NewTracker(1)
				// 2 overflow, 0x0002 and 0x0003 will overflow
				t.CheckOverflow(0x00010fffffffffff)
				t.CheckOverflow(0x00020fffffffffff)
				t.CheckOverflow(0x00030fffffffffff)
				return t
			}(),
			from: func() *Tracker {
				t := NewTracker(1)
				// 2 overflow, 0x0004 and 0x0005 will overflow
				t.CheckOverflow(0x00030fffffffffff)
				t.CheckOverflow(0x00040fffffffffff)
				t.CheckOverflow(0x00050fffffffffff)
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
