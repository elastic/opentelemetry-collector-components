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

package histogram

import (
	"testing"
	"time"

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/config"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/model"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestExplicitBounds(t *testing.T) {
	histogramCfg := config.Histogram{
		Explicit: &config.ExplicitHistogram{
			Buckets: []float64{1, 10, 100, 1000},
		},
	}
	for _, tc := range []struct {
		name              string
		metricDefs        []model.MetricDef
		input             []ptrace.Span
		expectedHistogram pmetric.MetricSlice
	}{
		{
			name:              "empty",
			expectedHistogram: pmetric.NewMetricSlice(),
		},
		{
			name: "no_attribute_configured",
			metricDefs: []model.MetricDef{
				{
					Name:        "metric.1",
					Description: "metric desc 1",
					Attributes:  nil,
					Unit:        config.MetricUnitS,
					Histogram:   histogramCfg,
				},
			},
			input: []ptrace.Span{
				getTestSpan(time.Minute, map[string]any{"key.1": "val.1"}),
				getTestSpan(time.Second, map[string]any{"key.2": "val.2"}),
			},
			expectedHistogram: getTestExplicitHistogram(
				pmetric.NewMetricSlice(),
				"metric.1", "metric desc 1",
				[]createHist{
					{
						buckets: histogramCfg.Explicit.Buckets,
						counts:  []uint64{1, 0, 1, 0, 0},
						count:   2,
						sum:     61, // 1 minute + 1 second to seconds
					},
				},
			),
		},
		{
			name: "attribute_configured",
			metricDefs: []model.MetricDef{
				{
					Name:        "metric.1",
					Description: "metric desc 1",
					Attributes: []model.AttributeKeyValue{
						{Key: "key.1", DefaultValue: pcommon.NewValueEmpty()},
					},
					Unit:      config.MetricUnitS,
					Histogram: histogramCfg,
				},
			},
			input: []ptrace.Span{
				getTestSpan(time.Minute, map[string]any{"key.1": "val.1"}),
				getTestSpan(time.Second, map[string]any{"key.2": "val.2"}),
			},
			expectedHistogram: getTestExplicitHistogram(
				pmetric.NewMetricSlice(),
				"metric.1", "metric desc 1",
				[]createHist{
					{
						buckets: histogramCfg.Explicit.Buckets,
						attrs:   map[string]any{"key.1": "val.1"},
						counts:  []uint64{0, 0, 1, 0, 0},
						count:   1,
						sum:     60, // 1 minute to seconds
					},
				},
			),
		},
		{
			name: "default_attribute_configured",
			metricDefs: []model.MetricDef{
				{
					Name:        "metric.1",
					Description: "metric desc 1",
					Attributes: []model.AttributeKeyValue{
						{Key: "key.3", DefaultValue: pcommon.NewValueStr("val.3")},
					},
					Unit:      config.MetricUnitS,
					Histogram: histogramCfg,
				},
			},
			input: []ptrace.Span{
				getTestSpan(time.Minute, map[string]any{"key.1": "val.1"}),
				getTestSpan(time.Second, map[string]any{"key.2": "val.2"}),
			},
			expectedHistogram: getTestExplicitHistogram(
				pmetric.NewMetricSlice(),
				"metric.1", "metric desc 1",
				[]createHist{
					{
						buckets: histogramCfg.Explicit.Buckets,
						attrs:   map[string]any{"key.3": "val.3"},
						counts:  []uint64{1, 0, 1, 0, 0},
						count:   2,
						sum:     61, // 1 minute + 1 second to seconds
					},
				},
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hist := NewExplicitBounds()
			require.NotNil(t, hist)
			actual := pmetric.NewMetricSlice()
			for _, span := range tc.input {
				duration := time.Duration(span.EndTimestamp() - span.StartTimestamp())
				for _, md := range tc.metricDefs {
					require.NoError(t, hist.Add(md, span.Attributes(), duration))
				}
			}
			require.Equal(t, hist.Size(), tc.expectedHistogram.Len())
			for _, md := range tc.metricDefs {
				hist.Move(md, actual)
			}
			require.Equal(t, tc.expectedHistogram.Len(), actual.Len())

			// Copy into comparable structures and compare
			expectedM := pmetric.NewMetrics()
			tc.expectedHistogram.CopyTo(expectedM.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics())
			actualM := pmetric.NewMetrics()
			actual.CopyTo(actualM.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics())
			assert.NoError(t, pmetrictest.CompareMetrics(expectedM, actualM, pmetrictest.IgnoreTimestamp()))

			hist.Reset()
			assert.Equal(t, 0, hist.Size())
		})
	}
}

func getTestSpan(duration time.Duration, attrs map[string]any) ptrace.Span {
	now := time.Now().UTC()
	span := ptrace.NewSpan()
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-1 * duration)))
	span.Attributes().FromRaw(attrs)
	return span
}

type createHist struct {
	buckets []float64
	counts  []uint64
	attrs   map[string]any
	sum     float64
	count   uint64
}

func getTestExplicitHistogram(
	slice pmetric.MetricSlice,
	name, desc string,
	hists []createHist,
) pmetric.MetricSlice {
	metric := slice.AppendEmpty()
	metric.SetName(name)
	metric.SetDescription(desc)
	destHist := metric.SetEmptyHistogram()
	destHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	for _, hist := range hists {
		dp := destHist.DataPoints().AppendEmpty()
		dp.Attributes().FromRaw(hist.attrs)
		dp.ExplicitBounds().FromRaw(hist.buckets)
		dp.BucketCounts().FromRaw(hist.counts)
		dp.SetCount(hist.count)
		dp.SetSum(hist.sum)
	}
	return slice
}
