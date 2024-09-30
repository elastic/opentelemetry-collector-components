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

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// countersDP records sum and count of span duration as two counter metrics.
// Both counters are assumed to be delta temporality cumulative counters
// accepting double values.
type countersDP struct {
	attrs pcommon.Map

	sum   float64
	count uint64
}

func newCountersDP(attrs pcommon.Map) *countersDP {
	return &countersDP{
		attrs: attrs,
	}
}

func (dp *countersDP) Add(value float64, count uint64) {
	dp.sum += value * float64(count)
	dp.count += count
}

func (dp *countersDP) Copy(
	timestamp time.Time,
	destSum pmetric.NumberDataPoint,
	destCount pmetric.NumberDataPoint,
) {
	dp.attrs.CopyTo(destSum.Attributes())
	dp.attrs.CopyTo(destCount.Attributes())
	destSum.SetDoubleValue(dp.sum)
	destCount.SetDoubleValue(float64(dp.count))
	// TODO determine appropriate start time
	ts := pcommon.NewTimestampFromTime(timestamp)
	destSum.SetTimestamp(ts)
	destCount.SetTimestamp(ts)
}
