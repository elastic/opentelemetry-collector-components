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

package merger // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type DataPointSlice[DP DataPoint[DP]] interface {
	Len() int
	At(i int) DP
	AppendEmpty() DP
}

type DataPoint[Self any] interface {
	pmetric.NumberDataPoint | pmetric.SummaryDataPoint | pmetric.HistogramDataPoint | pmetric.ExponentialHistogramDataPoint

	Timestamp() pcommon.Timestamp
	SetTimestamp(pcommon.Timestamp)
	Attributes() pcommon.Map
	CopyTo(dest Self)
}
