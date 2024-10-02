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

package merger // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/lsmintervalprocessor/internal/merger"

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestKeyOrdered(t *testing.T) {
	// For querying purposes the key should be ordered and comparable
	ts := time.Unix(0, 0)
	ivl := time.Minute

	before := NewKey(ivl, ts)
	for i := 0; i < 10; i++ {
		beforeBytes, err := before.Marshal()
		require.NoError(t, err)

		ts = ts.Add(time.Minute)
		after := NewKey(ivl, ts)
		afterBytes, err := after.Marshal()
		require.NoError(t, err)

		// before should always come first
		assert.Equal(t, -1, pebble.DefaultComparer.Compare(beforeBytes, afterBytes))
		before = after
	}
}

func TestMergeDeltaExponentialHistogram(t *testing.T) {
	for _, tc := range []struct {
		name     string
		from     pmetric.ExponentialHistogramDataPoint
		to       pmetric.ExponentialHistogramDataPoint
		expected pmetric.ExponentialHistogramDataPoint
	}{
		{
			name: "from_empty",
			from: pmetric.NewExponentialHistogramDataPoint(),
			to: func() pmetric.ExponentialHistogramDataPoint {
				dp := pmetric.NewExponentialHistogramDataPoint()
				dp.SetCount(18)
				dp.SetSum(18)
				dp.SetMin(1.01)
				dp.SetMax(1.11)
				dp.SetScale(5)
				dp.Positive().BucketCounts().FromRaw([]uint64{5, 7, 4, 0, 2})
				return dp
			}(),
			expected: func() pmetric.ExponentialHistogramDataPoint {
				dp := pmetric.NewExponentialHistogramDataPoint()
				dp.SetCount(18)
				dp.SetSum(18)
				dp.SetMin(1.01)
				dp.SetMax(1.11)
				dp.SetScale(5)
				dp.Positive().BucketCounts().FromRaw([]uint64{5, 7, 4, 0, 2})
				return dp
			}(),
		},
		{
			name: "to_empty",
			from: func() pmetric.ExponentialHistogramDataPoint {
				dp := pmetric.NewExponentialHistogramDataPoint()
				dp.SetCount(18)
				dp.SetSum(18)
				dp.SetMin(1.01)
				dp.SetMax(1.11)
				dp.SetScale(5)
				dp.Positive().BucketCounts().FromRaw([]uint64{5, 7, 4, 0, 2})
				return dp
			}(),
			to: pmetric.NewExponentialHistogramDataPoint(),
			expected: func() pmetric.ExponentialHistogramDataPoint {
				dp := pmetric.NewExponentialHistogramDataPoint()
				dp.SetCount(18)
				dp.SetSum(18)
				dp.SetMin(1.01)
				dp.SetMax(1.11)
				dp.SetScale(5)
				dp.Positive().BucketCounts().FromRaw([]uint64{5, 7, 4, 0, 2})
				return dp
			}(),
		},
		{
			name: "no_offset_scaledown",
			from: func() pmetric.ExponentialHistogramDataPoint {
				dp := pmetric.NewExponentialHistogramDataPoint()
				dp.SetCount(18)
				dp.SetSum(18)
				dp.SetMin(1.01)
				dp.SetMax(1.11)
				dp.SetScale(5)
				dp.Positive().BucketCounts().FromRaw([]uint64{5, 7, 4, 0, 2})
				return dp
			}(),
			to: func() pmetric.ExponentialHistogramDataPoint {
				dp := pmetric.NewExponentialHistogramDataPoint()
				dp.SetCount(24)
				dp.SetSum(31)
				dp.SetMin(1.17)
				dp.SetMax(1.81)
				dp.SetScale(2)
				dp.Positive().BucketCounts().FromRaw([]uint64{7, 0, 8, 9})
				return dp
			}(),
			expected: func() pmetric.ExponentialHistogramDataPoint {
				dp := pmetric.NewExponentialHistogramDataPoint()
				dp.SetCount(42)
				dp.SetSum(49)
				dp.SetMin(1.01)
				dp.SetMax(1.81)
				dp.SetScale(2)
				dp.Positive().BucketCounts().FromRaw([]uint64{25, 0, 8, 9, 0})
				return dp
			}(),
		},
		{
			name: "with_offset_scaledown#1",
			from: func() pmetric.ExponentialHistogramDataPoint {
				dp := pmetric.NewExponentialHistogramDataPoint()
				dp.SetCount(18)
				dp.SetSum(18)
				dp.SetMin(1.01)
				dp.SetMax(1.11)
				dp.SetScale(5)
				dp.Positive().SetOffset(10)
				dp.Positive().BucketCounts().FromRaw([]uint64{5, 7, 4, 0, 2})
				return dp
			}(),
			to: func() pmetric.ExponentialHistogramDataPoint {
				dp := pmetric.NewExponentialHistogramDataPoint()
				dp.SetCount(24)
				dp.SetSum(31)
				dp.SetMin(1.17)
				dp.SetMax(1.81)
				dp.SetScale(2)
				dp.Positive().SetOffset(1)
				dp.Positive().BucketCounts().FromRaw([]uint64{7, 0, 8, 9})
				return dp
			}(),
			expected: func() pmetric.ExponentialHistogramDataPoint {
				dp := pmetric.NewExponentialHistogramDataPoint()
				dp.SetCount(42)
				dp.SetSum(49)
				dp.SetMin(1.01)
				dp.SetMax(1.81)
				dp.SetScale(2)
				dp.Positive().SetOffset(1)
				dp.Positive().BucketCounts().FromRaw([]uint64{25, 0, 8, 9, 0})
				return dp
			}(),
		},
		{
			name: "with_offset_scaledown#2",
			from: func() pmetric.ExponentialHistogramDataPoint {
				dp := pmetric.NewExponentialHistogramDataPoint()
				dp.SetCount(18)
				dp.SetSum(18)
				dp.SetMin(1.01)
				dp.SetMax(1.11)
				dp.SetScale(5)
				dp.Positive().SetOffset(33)
				dp.Positive().BucketCounts().FromRaw([]uint64{5, 7, 4, 0, 2})
				return dp
			}(),
			to: func() pmetric.ExponentialHistogramDataPoint {
				dp := pmetric.NewExponentialHistogramDataPoint()
				dp.SetCount(24)
				dp.SetSum(31)
				dp.SetMin(1.17)
				dp.SetMax(1.81)
				dp.SetScale(2)
				dp.Positive().SetOffset(1)
				dp.Positive().BucketCounts().FromRaw([]uint64{7, 0, 8, 9})
				return dp
			}(),
			expected: func() pmetric.ExponentialHistogramDataPoint {
				dp := pmetric.NewExponentialHistogramDataPoint()
				dp.SetCount(42)
				dp.SetSum(49)
				dp.SetMin(1.01)
				dp.SetMax(1.81)
				dp.SetScale(2)
				dp.Positive().SetOffset(1)
				dp.Positive().BucketCounts().FromRaw([]uint64{7, 0, 8, 27, 0, 0, 0, 0})
				return dp
			}(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mergeDeltaExponentialHistogramDP(tc.from, tc.to)
			assert.NoError(t, pmetrictest.CompareExponentialHistogramDataPoint(tc.expected, tc.to))
		})
	}
}
