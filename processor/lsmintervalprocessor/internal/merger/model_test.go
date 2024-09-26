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
	"go.opentelemetry.io/collector/pdata/pcommon"
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

func TestMergeSummaryDP(t *testing.T) {
	now := time.Unix(0, 0).UTC()
	for _, tc := range []struct {
		name       string
		to         pmetric.SummaryDataPoint
		from       pmetric.SummaryDataPoint
		expectedDP pmetric.SummaryDataPoint
	}{
		{
			name: "without_start_ts",
			to: newTestSummaryDP(
				time.Time{},
				now.Add(1*time.Minute),
				421, 77,
				[]quantileValue{{0.5, 30}, {0.75, 100}},
			),
			from: newTestSummaryDP(
				time.Time{},
				now.Add(5*time.Minute),
				111, 11,
				[]quantileValue{{0.5, 70}, {0.75, 90}},
			),
			expectedDP: newTestSummaryDP(
				time.Time{},
				now.Add(5*time.Minute),
				532, 88,
				[]quantileValue{{0.5, 70}, {0.75, 90}},
			),
		},
		{
			name: "with_start_ts_happy_path",
			to: newTestSummaryDP(
				now.Add(1*time.Minute),
				now.Add(4*time.Minute),
				421, 77,
				[]quantileValue{{0.5, 30}, {0.75, 100}},
			),
			from: newTestSummaryDP(
				now.Add(4*time.Minute),
				now.Add(5*time.Minute),
				111, 11,
				[]quantileValue{{0.5, 70}, {0.75, 90}},
			),
			expectedDP: newTestSummaryDP(
				now.Add(1*time.Minute),
				now.Add(5*time.Minute),
				532, 88,
				[]quantileValue{{0.5, 70}, {0.75, 90}},
			),
		},
		{
			name: "with_start_ts_old_data_arriving_late",
			to: newTestSummaryDP(
				now.Add(3*time.Minute),
				now.Add(4*time.Minute),
				421, 77,
				[]quantileValue{{0.5, 30}, {0.75, 100}},
			),
			from: newTestSummaryDP(
				now.Add(1*time.Minute),
				now.Add(2*time.Minute),
				111, 11,
				[]quantileValue{{0.5, 70}, {0.75, 90}},
			),
			expectedDP: newTestSummaryDP(
				now.Add(1*time.Minute),
				now.Add(4*time.Minute),
				532, 88,
				[]quantileValue{{0.5, 30}, {0.75, 100}},
			),
		},
		{
			name: "overlapping_ignore_subset",
			to: newTestSummaryDP(
				now.Add(1*time.Minute),
				now.Add(5*time.Minute),
				421, 77,
				[]quantileValue{{0.5, 30}, {0.75, 100}},
			),
			from: newTestSummaryDP(
				now.Add(1*time.Minute),
				now.Add(3*time.Minute),
				111, 11,
				[]quantileValue{{0.5, 70}, {0.75, 90}},
			),
			expectedDP: newTestSummaryDP(
				now.Add(1*time.Minute),
				now.Add(5*time.Minute),
				421, 77,
				[]quantileValue{{0.5, 30}, {0.75, 100}},
			),
		},
		{
			name: "overlapping_take_latest",
			to: newTestSummaryDP(
				now.Add(1*time.Minute),
				now.Add(5*time.Minute),
				421, 77,
				[]quantileValue{{0.5, 30}, {0.75, 100}},
			),
			from: newTestSummaryDP(
				now.Add(3*time.Minute),
				now.Add(7*time.Minute),
				598, 121,
				[]quantileValue{{0.5, 70}, {0.75, 90}},
			),
			expectedDP: newTestSummaryDP(
				now.Add(1*time.Minute),
				now.Add(7*time.Minute),
				598, 121,
				[]quantileValue{{0.5, 70}, {0.75, 90}},
			),
		},
		{
			name: "overlapping_ignore_older",
			to: newTestSummaryDP(
				now.Add(2*time.Minute),
				now.Add(5*time.Minute),
				421, 77,
				[]quantileValue{{0.5, 30}, {0.75, 100}},
			),
			from: newTestSummaryDP(
				now.Add(1*time.Minute),
				now.Add(3*time.Minute),
				111, 11,
				[]quantileValue{{0.5, 70}, {0.75, 90}},
			),
			expectedDP: newTestSummaryDP(
				now.Add(1*time.Minute),
				now.Add(5*time.Minute),
				421, 77,
				[]quantileValue{{0.5, 30}, {0.75, 100}},
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mergeSummaryDP(tc.from, tc.to)
			expected := pmetric.NewMetrics()
			tc.expectedDP.CopyTo(
				expected.ResourceMetrics().AppendEmpty().
					ScopeMetrics().AppendEmpty().
					Metrics().AppendEmpty().
					SetEmptySummary().DataPoints().AppendEmpty(),
			)
			actual := pmetric.NewMetrics()
			tc.to.CopyTo(
				actual.ResourceMetrics().AppendEmpty().
					ScopeMetrics().AppendEmpty().
					Metrics().AppendEmpty().
					SetEmptySummary().DataPoints().AppendEmpty(),
			)
			assert.NoError(t, pmetrictest.CompareMetrics(expected, actual))
		})
	}
}

type quantileValue struct {
	Quantile float64
	Value    float64
}

func newTestSummaryDP(
	startTs, ts time.Time, sum float64, count uint64, qtiles []quantileValue,
) pmetric.SummaryDataPoint {
	dp := pmetric.NewSummaryDataPoint()
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(startTs))
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	dp.SetSum(sum)
	dp.SetCount(count)
	for _, qtile := range qtiles {
		qv := dp.QuantileValues().AppendEmpty()
		qv.SetQuantile(qtile.Quantile)
		qv.SetValue(qtile.Value)
	}
	return dp
}
