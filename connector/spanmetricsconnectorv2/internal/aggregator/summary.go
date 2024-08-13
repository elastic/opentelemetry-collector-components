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

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/config"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/model"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// summary represents a summary metric which calculates count and sum over
// span duration.
type summary struct {
	// TODO (lahsivjar): Attribute hash collisions are not considered
	datapoints map[model.MetricKey]map[[16]byte]*summaryDP
	timestamp  time.Time
}

func newSummary() *summary {
	return &summary{
		datapoints: make(map[model.MetricKey]map[[16]byte]*summaryDP),
		timestamp:  time.Now(),
	}
}

func (s *summary) Add(
	key model.MetricKey,
	value float64,
	attributes pcommon.Map,
	histoCfg config.Summary,
) error {
	if _, ok := s.datapoints[key]; !ok {
		s.datapoints[key] = make(map[[16]byte]*summaryDP)
	}

	var attrKey [16]byte
	if attributes.Len() > 0 {
		attrKey = pdatautil.MapHash(attributes)
	}

	if _, ok := s.datapoints[key][attrKey]; !ok {
		s.datapoints[key][attrKey] = newSummaryDP(attributes)
	}

	dp := s.datapoints[key][attrKey]
	dp.sum += value
	dp.count++
	return nil
}

func (s *summary) Move(
	key model.MetricKey,
	dest pmetric.MetricSlice,
) {
	srcDps, ok := s.datapoints[key]
	if !ok || len(srcDps) == 0 {
		return
	}

	destMetric := dest.AppendEmpty()
	destMetric.SetName(key.Name)
	destMetric.SetDescription(key.Description)
	destSummary := destMetric.SetEmptySummary()
	destSummary.DataPoints().EnsureCapacity(len(srcDps))
	for _, srcDp := range srcDps {
		destDp := destSummary.DataPoints().AppendEmpty()
		srcDp.attrs.CopyTo(destDp.Attributes())
		destDp.SetCount(srcDp.count)
		destDp.SetSum(srcDp.sum)
		// TODO determine appropriate start time
		destDp.SetTimestamp(pcommon.NewTimestampFromTime(s.timestamp))
	}
	// If there are two metric defined with the same key required by metricKey
	// then they will be aggregated within the same histogram and produced
	// together. Deleting the key ensures this while preventing duplicates.
	delete(s.datapoints, key)
}

func (s *summary) Size() int {
	return len(s.datapoints)
}

func (s *summary) Reset() {
	clear(s.datapoints)
}

type summaryDP struct {
	attrs pcommon.Map

	sum   float64
	count uint64
}

func newSummaryDP(attrs pcommon.Map) *summaryDP {
	return &summaryDP{
		attrs: attrs,
	}
}
