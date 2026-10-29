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

package prometheusscrapereceiver

import (
	"context"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// FuzzAppendNativeHistogram feeds arbitrary native histograms, as a scraped target could
// expose them (the Prometheus parsers don't validate them), to the appender. It must never
// panic or allocate an unbounded bucket layout. A histogram is either dropped, or it is
// valid and converted to a dense layout that matches its spans.
func FuzzAppendNativeHistogram(f *testing.F) {
	// valid integer and float histograms
	f.Add(int8(3), true, int32(1), uint32(2), int32(2), uint32(1), []byte{3, 0xfe, 1}, uint64(12), 12.5, false)
	f.Add(int8(0), false, int32(0), uint32(2), int32(0), uint32(0), []byte{2, 3}, uint64(5), 3.0, true)
	// a span longer than the buckets
	f.Add(int8(3), false, int32(0), uint32(5), int32(0), uint32(0), []byte{1}, uint64(1), 1.0, false)
	// a huge gap between the spans
	f.Add(int8(3), false, int32(0), uint32(1), int32(math.MaxInt32), uint32(1), []byte{1, 0}, uint64(2), 1.0, false)
	// a negative offset on the second span, negative bucket counts
	f.Add(int8(3), false, int32(0), uint32(1), int32(-1), uint32(1), []byte{1, 1}, uint64(2), 1.0, false)
	f.Add(int8(3), true, int32(0), uint32(1), int32(0), uint32(0), []byte{0xff}, uint64(1), 1.0, false)
	// custom buckets (NHCB) and invalid schemas
	f.Add(int8(-53), false, int32(0), uint32(1), int32(0), uint32(0), []byte{1}, uint64(1), 1.0, false)
	f.Add(int8(127), false, int32(0), uint32(1), int32(0), uint32(0), []byte{1}, uint64(1), 1.0, true)

	f.Fuzz(func(t *testing.T, schema int8, negToo bool, off1 int32, len1 uint32, off2 int32, len2 uint32, deltas []byte, count uint64, sum float64, float bool) {
		spans := []histogram.Span{{Offset: off1, Length: len1}, {Offset: off2, Length: len2}}
		var negSpans []histogram.Span
		if negToo {
			negSpans = spans
		}

		var h *histogram.Histogram
		var fh *histogram.FloatHistogram
		if float {
			abs := make([]float64, len(deltas))
			for i, d := range deltas {
				abs[i] = float64(int8(d))
			}
			fh = &histogram.FloatHistogram{
				Schema: int32(schema), Count: float64(count), Sum: sum,
				PositiveSpans: spans, PositiveBuckets: abs, NegativeSpans: negSpans,
			}
			if negToo {
				fh.NegativeBuckets = abs
			}
		} else {
			d := make([]int64, len(deltas))
			for i, b := range deltas {
				d[i] = int64(int8(b))
			}
			h = &histogram.Histogram{
				Schema: int32(schema), Count: count, Sum: sum,
				PositiveSpans: spans, PositiveBuckets: d, NegativeSpans: negSpans,
			}
			if negToo {
				h.NegativeBuckets = d
			}
		}

		sink := new(consumertest.MetricsSink)
		app := newTestAppendable(t, sink, labels.EmptyLabels()).AppenderV2(context.Background())
		require.NotPanics(t, func() {
			_, err := app.Append(0, labels.FromStrings("__name__", "fuzz"), 0, 1, 0, h, fh, storage.AppendV2Options{})
			require.NoError(t, err, "a dropped sample must never fail the scrape")
		})
		require.NoError(t, app.Commit())

		if len(sink.AllMetrics()) == 0 {
			return // dropped
		}

		// Emitted: the histogram was valid and the layout is bounded and matches the spans.
		if float {
			require.NoError(t, fh.Validate())
		} else {
			require.NoError(t, h.Validate())
		}
		m := sink.AllMetrics()[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
		require.Equal(t, pmetric.MetricTypeExponentialHistogram, m.Type())
		dp := m.ExponentialHistogram().DataPoints().At(0)
		assert.EqualValues(t, denseBuckets(spans), dp.Positive().BucketCounts().Len())
		assert.LessOrEqual(t, dp.Positive().BucketCounts().Len(), maxDenseBuckets)
		if negToo {
			assert.EqualValues(t, denseBuckets(spans), dp.Negative().BucketCounts().Len())
		} else {
			assert.Zero(t, dp.Negative().BucketCounts().Len())
		}
	})
}
