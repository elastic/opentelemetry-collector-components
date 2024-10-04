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

package aggregator // import "github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/aggregator"
import (
	"time"

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type spanDurationDP struct {
	expHistogramDP      *exponentialHistogramDP
	explicitHistogramDP *explicitHistogramDP
	summaryDP           *summaryDP
	sumAndCountDP       *sumAndCountDP
}

func newSpanDurationDP[K any](
	md model.ValueCountMetric[K],
	attrs pcommon.Map,
) *spanDurationDP {
	var dp spanDurationDP
	if md.ExponentialHistogram != nil {
		dp.expHistogramDP = newExponentialHistogramDP(
			attrs, md.ExponentialHistogram.MaxSize,
		)
	}
	if md.ExplicitHistogram != nil {
		dp.explicitHistogramDP = newExplicitHistogramDP(
			attrs, md.ExplicitHistogram.Buckets,
		)
	}
	if md.Summary != nil {
		dp.summaryDP = newSummaryDP(attrs)
	}
	if md.SumAndCount != nil {
		dp.sumAndCountDP = newsumAndCountDP(attrs)
	}
	return &dp
}

func (dp *spanDurationDP) Aggregate(value float64, count uint64) {
	if dp.expHistogramDP != nil {
		dp.expHistogramDP.Aggregate(value, count)
	}
	if dp.explicitHistogramDP != nil {
		dp.explicitHistogramDP.Aggregate(value, count)
	}
	if dp.summaryDP != nil {
		dp.summaryDP.Aggregate(value, count)
	}
	if dp.sumAndCountDP != nil {
		dp.sumAndCountDP.Aggregate(value, count)
	}
}

func (dp *spanDurationDP) Copy(
	timestamp time.Time,
	destExpHist pmetric.ExponentialHistogram,
	destExplicitHist pmetric.Histogram,
	destSummary pmetric.Summary,
	destSum pmetric.Sum,
	destCount pmetric.Sum,
) {
	if dp.expHistogramDP != nil {
		dp.expHistogramDP.Copy(timestamp, destExpHist.DataPoints().AppendEmpty())
	}
	if dp.explicitHistogramDP != nil {
		dp.explicitHistogramDP.Copy(timestamp, destExplicitHist.DataPoints().AppendEmpty())
	}
	if dp.summaryDP != nil {
		dp.summaryDP.Copy(timestamp, destSummary.DataPoints().AppendEmpty())
	}
	if dp.sumAndCountDP != nil {
		dp.sumAndCountDP.Copy(
			timestamp,
			destSum.DataPoints().AppendEmpty(),
			destCount.DataPoints().AppendEmpty(),
		)
	}
}
