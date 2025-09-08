package elasticapmintakereceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/apm-data/model/modelpb"
)

// Test_Receiver_elasticMetricsToOtelMetrics specific test to cover edge
// cases when converting from elastic metrics to otel metrics
func Test_Receiver_elasticMetricsToOtelMetrics(t *testing.T) {
	var (
		intakeReceiver = &elasticAPMIntakeReceiver{}

		currTime                = time.Now()
		apmMetricHistogramEvent = &modelpb.APMEvent{
			Event: &modelpb.Event{
				Outcome: "success",
			},
			Service: &modelpb.Service{
				Name:    "my-service",
				Version: "1.0.0",
			},
			Metricset: &modelpb.Metricset{
				Samples: []*modelpb.MetricsetSample{
					{
						Name: "valid_histogram_metric_1",
						Type: modelpb.MetricType_METRIC_TYPE_HISTOGRAM,
						Unit: "ms",
						Histogram: &modelpb.Histogram{
							Counts: []uint64{1, 3, 5},
							Values: []float64{10, 20, 30},
						},
					},
					{
						Name: "valid_histogram_metric_2",
						Type: modelpb.MetricType_METRIC_TYPE_HISTOGRAM,
						Unit: "ms",
						Histogram: &modelpb.Histogram{
							Counts: []uint64{2, 4, 6, 8, 10},
							Values: []float64{5, 10, 15, 25, 50},
						},
					},
					{
						Name: "empty_values_histogram_metric",
						Type: modelpb.MetricType_METRIC_TYPE_HISTOGRAM,
						Unit: "ms",
						Histogram: &modelpb.Histogram{
							Counts: []uint64{1, 2, 3},
							Values: []float64{},
						},
					},
					{
						Name: "empty_counts_histogram_metric",
						Type: modelpb.MetricType_METRIC_TYPE_HISTOGRAM,
						Unit: "ms",
						Histogram: &modelpb.Histogram{
							Counts: []uint64{},
							Values: []float64{10, 20, 30},
						},
					},
					{
						Name: "value_count_mismatch_histogram_metric",
						Type: modelpb.MetricType_METRIC_TYPE_HISTOGRAM,
						Unit: "ms",
						Histogram: &modelpb.Histogram{
							Counts: []uint64{1, 2, 3},
							Values: []float64{10, 20},
						},
					},
					{
						Name:      "nil_histogram_metric",
						Type:      modelpb.MetricType_METRIC_TYPE_HISTOGRAM,
						Unit:      "ms",
						Histogram: nil,
					},
				},
			},
		}
		emptyMetricEvent = &modelpb.APMEvent{
			Metricset: &modelpb.Metricset{
				Samples: []*modelpb.MetricsetSample{
					{
						Name: "",
						Type: modelpb.MetricType_METRIC_TYPE_HISTOGRAM,
						Unit: "",
					},
				},
			},
		}
		apmMetricSummaryEvent = &modelpb.APMEvent{
			Event: &modelpb.Event{
				Outcome: "success",
			},
			Service: &modelpb.Service{
				Name:    "my-service",
				Version: "1.0.0",
			},
			Metricset: &modelpb.Metricset{
				Samples: []*modelpb.MetricsetSample{
					{
						Name: "valid_summary_metric_1",
						Type: modelpb.MetricType_METRIC_TYPE_SUMMARY,
						Unit: "ms",
						Summary: &modelpb.SummaryMetric{
							Count: 10,
							Sum:   100,
						},
					},
					{
						Name: "empty_summary_metric_1",
						Type: modelpb.MetricType_METRIC_TYPE_SUMMARY,
						Unit: "ms",
						Summary: &modelpb.SummaryMetric{
							Count: 0,
							Sum:   0,
						},
					},
					{
						Name:    "nil_summary_metric_1",
						Type:    modelpb.MetricType_METRIC_TYPE_SUMMARY,
						Unit:    "ms",
						Summary: nil,
					},
				},
			},
		}
	)
	expectedHistogramMetric := pmetric.NewMetrics().ResourceMetrics().AppendEmpty()
	expectedHistogramMetric.Resource().Attributes().PutStr("service.name", "my-service")
	expectedHistogramMetric.Resource().Attributes().PutStr("service.version", "1.0.0")
	sm := expectedHistogramMetric.ScopeMetrics().AppendEmpty()

	histogramMetric1 := sm.Metrics().AppendEmpty()
	histogramMetric1.SetName("valid_histogram_metric_1")
	histogramMetric1.SetUnit("ms")
	histogram1 := histogramMetric1.SetEmptyHistogram()
	dp1 := histogram1.DataPoints().AppendEmpty()
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(currTime))
	dp1.SetSum(10*1 + 20*3 + 30*5) // sum = Σ(value * count)
	dp1.SetCount(1 + 3 + 5)        // count = Σ(count)
	dp1.ExplicitBounds().FromRaw([]float64{10, 20})
	dp1.BucketCounts().FromRaw([]uint64{1, 3, 5})

	histogramMetric2 := sm.Metrics().AppendEmpty()
	histogramMetric2.SetName("valid_histogram_metric_2")
	histogramMetric2.SetUnit("ms")
	histogram2 := histogramMetric2.SetEmptyHistogram()
	dp2 := histogram2.DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(currTime))
	dp2.SetSum(2*5 + 4*10 + 6*15 + 8*25 + 10*50) // sum = Σ(value * count)
	dp2.SetCount(2 + 4 + 6 + 8 + 10)             // count = Σ(count)
	dp2.ExplicitBounds().FromRaw([]float64{5, 10, 15, 25})
	dp2.BucketCounts().FromRaw([]uint64{2, 4, 6, 8, 10})

	// last histograms are expected to be empty as the input histogram metrics are invalid
	histogramMetric3 := sm.Metrics().AppendEmpty()
	histogramMetric3.SetName("empty_values_histogram_metric")
	histogramMetric3.SetUnit("ms")
	emptyDP := histogramMetric3.SetEmptyHistogram().DataPoints().AppendEmpty()
	emptyDP.SetTimestamp(pcommon.NewTimestampFromTime(currTime))

	histogramMetric4 := sm.Metrics().AppendEmpty()
	histogramMetric4.SetName("empty_counts_histogram_metric")
	histogramMetric4.SetUnit("ms")
	emptyDP = histogramMetric4.SetEmptyHistogram().DataPoints().AppendEmpty()
	emptyDP.SetTimestamp(pcommon.NewTimestampFromTime(currTime))

	histogramMetric5 := sm.Metrics().AppendEmpty()
	histogramMetric5.SetName("value_count_mismatch_histogram_metric")
	histogramMetric5.SetUnit("ms")
	emptyDP = histogramMetric5.SetEmptyHistogram().DataPoints().AppendEmpty()
	emptyDP.SetTimestamp(pcommon.NewTimestampFromTime(currTime))

	histogramMetric6 := sm.Metrics().AppendEmpty()
	histogramMetric6.SetName("nil_histogram_metric")
	histogramMetric6.SetUnit("ms")
	emptyDP = histogramMetric6.SetEmptyHistogram().DataPoints().AppendEmpty()
	emptyDP.SetTimestamp(pcommon.NewTimestampFromTime(currTime))

	expectedSummaryMetric := pmetric.NewMetrics().ResourceMetrics().AppendEmpty()
	expectedSummaryMetric.Resource().Attributes().PutStr("service.name", "my-service")
	expectedSummaryMetric.Resource().Attributes().PutStr("service.version", "1.0.0")
	sm = expectedSummaryMetric.ScopeMetrics().AppendEmpty()

	summaryMetric1 := sm.Metrics().AppendEmpty()
	summaryMetric1.SetName("valid_summary_metric_1")
	summaryMetric1.SetUnit("ms")
	summary1 := summaryMetric1.SetEmptySummary()
	dp := summary1.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(currTime))
	dp.SetSum(100)
	dp.SetCount(10)

	summaryMetric2 := sm.Metrics().AppendEmpty()
	summaryMetric2.SetName("empty_summary_metric_1")
	summaryMetric2.SetUnit("ms")
	emptySummaryDP := summaryMetric2.SetEmptySummary().DataPoints().AppendEmpty()
	emptySummaryDP.SetTimestamp(pcommon.NewTimestampFromTime(currTime))

	summaryMetric3 := sm.Metrics().AppendEmpty()
	summaryMetric3.SetName("nil_summary_metric_1")
	summaryMetric3.SetUnit("ms")
	emptySummaryDP = summaryMetric3.SetEmptySummary().DataPoints().AppendEmpty()
	emptySummaryDP.SetTimestamp(pcommon.NewTimestampFromTime(currTime))

	expectedEmptyMetric := pmetric.NewMetrics().ResourceMetrics().AppendEmpty()
	sm = expectedEmptyMetric.ScopeMetrics().AppendEmpty()
	emptyMetric := sm.Metrics().AppendEmpty()
	emptyHistrogramDP := emptyMetric.SetEmptyHistogram().DataPoints().AppendEmpty()
	emptyHistrogramDP.SetTimestamp(pcommon.NewTimestampFromTime(currTime))

	testCases := []struct {
		name             string
		receiver         *elasticAPMIntakeReceiver
		apmMetricEvent   *modelpb.APMEvent
		resourceMetric   pmetric.ResourceMetrics
		timestamp        time.Time
		expectedResource pmetric.ResourceMetrics
		expectError      error
	}{
		{
			name:             "event with multiple histogram metrics samples",
			receiver:         intakeReceiver,
			apmMetricEvent:   apmMetricHistogramEvent,
			timestamp:        currTime,
			resourceMetric:   pmetric.NewMetrics().ResourceMetrics().AppendEmpty(),
			expectedResource: expectedHistogramMetric,
			expectError:      nil,
		},
		{
			name:             "event with multiple summary metrics samples",
			receiver:         intakeReceiver,
			apmMetricEvent:   apmMetricSummaryEvent,
			timestamp:        currTime,
			resourceMetric:   pmetric.NewMetrics().ResourceMetrics().AppendEmpty(),
			expectedResource: expectedSummaryMetric,
			expectError:      nil,
		},
		{
			name:             "event with empty metric samples",
			receiver:         intakeReceiver,
			apmMetricEvent:   emptyMetricEvent,
			timestamp:        currTime,
			resourceMetric:   pmetric.NewMetrics().ResourceMetrics().AppendEmpty(),
			expectedResource: expectedEmptyMetric,
			expectError:      nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.receiver.elasticMetricsToOtelMetrics(&tc.resourceMetric, tc.apmMetricEvent, tc.timestamp)
			require.Equal(t, tc.expectError, err)

			require.Equal(t, tc.expectedResource.ScopeMetrics().Len(), tc.resourceMetric.ScopeMetrics().Len())
			for scopeIndex, sm := range tc.resourceMetric.ScopeMetrics().All() {
				require.Equal(t, tc.expectedResource.ScopeMetrics().At(scopeIndex).Metrics().Len(), sm.Metrics().Len())
				for metricIndex, m := range sm.Metrics().All() {
					expectedMetric := tc.expectedResource.ScopeMetrics().At(scopeIndex).Metrics().At(metricIndex)

					// validate common fields
					assert.Equal(t, expectedMetric.Type(), m.Type(), "metric type at scopeIndex: %d, metricIndex: %d", scopeIndex, metricIndex)
					assert.Equal(t, expectedMetric.Name(), m.Name(), "metric name at scopeIndex: %d, metricIndex: %d", scopeIndex, metricIndex)
					assert.Equal(t, expectedMetric.Unit(), m.Unit(), "metric unit at scopeIndex: %d, metricIndex: %d", scopeIndex, metricIndex)
					assert.Equal(t, expectedMetric.Description(), m.Description(), "metric description at scopeIndex: %d, metricIndex: %d", scopeIndex, metricIndex)

					switch m.Type() {
					case pmetric.MetricTypeHistogram:
						require.Equal(t, expectedMetric.Histogram().DataPoints().Len(), m.Histogram().DataPoints().Len(), "histogram data points length at scopeIndex: %d, metricIndex: %d", scopeIndex, metricIndex)
						for dpIndex, dp := range m.Histogram().DataPoints().All() {
							expectedDP := expectedMetric.Histogram().DataPoints().At(dpIndex)
							assert.Equal(t, expectedDP.Timestamp().AsTime(), dp.Timestamp().AsTime(), "histogram data point timestamp at scopeIndex: %d, metricIndex: %d, dpIndex: %d", scopeIndex, metricIndex, dpIndex)
							assert.Equal(t, expectedDP.Sum(), dp.Sum(), "histogram data point sum at scopeIndex: %d, metricIndex: %d, dpIndex: %d", scopeIndex, metricIndex, dpIndex)
							assert.Equal(t, expectedDP.Count(), dp.Count(), "histogram data point count at scopeIndex: %d, metricIndex: %d, dpIndex: %d", scopeIndex, metricIndex, dpIndex)
							assert.Equal(t, expectedDP.BucketCounts().AsRaw(), dp.BucketCounts().AsRaw(), "histogram data point bucket counts at scopeIndex: %d, metricIndex: %d, dpIndex: %d", scopeIndex, metricIndex, dpIndex)
							assert.Equal(t, expectedDP.ExplicitBounds().AsRaw(), dp.ExplicitBounds().AsRaw(), "histogram data point explicit bounds at scopeIndex: %d, metricIndex: %d, dpIndex: %d", scopeIndex, metricIndex, dpIndex)
						}
					case pmetric.MetricTypeSummary:
						require.Equal(t, expectedMetric.Summary().DataPoints().Len(), m.Summary().DataPoints().Len(), "summary data points length at scopeIndex: %d, metricIndex: %d", scopeIndex, metricIndex)
						for dpIndex, dp := range m.Summary().DataPoints().All() {
							expectedDP := expectedMetric.Summary().DataPoints().At(dpIndex)
							assert.Equal(t, expectedDP.Timestamp().AsTime(), dp.Timestamp().AsTime(), "summary data point timestamp at scopeIndex: %d, metricIndex: %d, dpIndex: %d", scopeIndex, metricIndex, dpIndex)
							assert.Equal(t, expectedDP.Sum(), dp.Sum(), "summary data point sum at scopeIndex: %d, metricIndex: %d, dpIndex: %d", scopeIndex, metricIndex, dpIndex)
							assert.Equal(t, expectedDP.Count(), dp.Count(), "summary data point count at scopeIndex: %d, metricIndex: %d, dpIndex: %d", scopeIndex, metricIndex, dpIndex)
						}
					default:
						assert.Fail(t, "unexpected metric type %s at scopeIndex: %d, metricIndex: %d", m.Type(), scopeIndex, metricIndex)
					}
				}
			}
		})
	}
}
