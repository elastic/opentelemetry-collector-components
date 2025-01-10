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

package aggregator // import "github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/aggregator"
import (
	"time"

	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// valueCountDP is a wrapper DP to aggregate all datapoints that record
// value and count.
type valueCountDP struct {
	expHistogramDP      *exponentialHistogramDP
	explicitHistogramDP *explicitHistogramDP
}

func newValueCountDP[K any](
	md model.MetricDef[K],
	attrs pcommon.Map,
) *valueCountDP {
	var dp valueCountDP
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
	return &dp
}

func (dp *valueCountDP) Aggregate(value float64, count int64) {
	if dp.expHistogramDP != nil {
		dp.expHistogramDP.Aggregate(value, count)
	}
	if dp.explicitHistogramDP != nil {
		dp.explicitHistogramDP.Aggregate(value, count)
	}
}

func (dp *valueCountDP) Copy(
	timestamp time.Time,
	destExpHist pmetric.ExponentialHistogram,
	destExplicitHist pmetric.Histogram,
) {
	if dp.expHistogramDP != nil {
		dp.expHistogramDP.Copy(timestamp, destExpHist.DataPoints().AppendEmpty())
	}
	if dp.explicitHistogramDP != nil {
		dp.explicitHistogramDP.Copy(timestamp, destExplicitHist.DataPoints().AppendEmpty())
	}
}
