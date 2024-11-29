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

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// sumDP counts the number of events (supports all event types)
type sumDP struct {
	attrs pcommon.Map

	isDbl  bool
	intVal int64
	dblVal float64
}

func newSumDP(attrs pcommon.Map, isDbl bool) *sumDP {
	return &sumDP{
		isDbl: isDbl,
		attrs: attrs,
	}
}

func (dp *sumDP) AggregateInt(v int64) {
	if dp.isDbl {
		panic("unexpected usage of sum datapoint, only integer value expected")
	}
	dp.intVal += v
}

func (dp *sumDP) AggregateDouble(v float64) {
	if !dp.isDbl {
		panic("unexpected usage of sum datapoint, only double value expected")
	}
	dp.dblVal += v
}

func (dp *sumDP) Copy(
	timestamp time.Time,
	dest pmetric.NumberDataPoint,
) {
	dp.attrs.CopyTo(dest.Attributes())
	if dp.isDbl {
		dest.SetDoubleValue(dp.dblVal)
	} else {
		dest.SetIntValue(dp.intVal)
	}
	// TODO determine appropriate start time
	dest.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
}
