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

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// This is a copy of the internal module from opentelemetry-collector-contrib:
// https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/deltatocumulativeprocessor/internal/data

package expo // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Merge combines the counts of buckets a and b into a.
// Both buckets MUST be of same scale
func Merge(arel, brel Buckets) {
	if brel.BucketCounts().Len() == 0 {
		return
	}
	if arel.BucketCounts().Len() == 0 {
		brel.CopyTo(arel)
		return
	}

	a, b := Abs(arel), Abs(brel)

	lo := min(a.Lower(), b.Lower())
	up := max(a.Upper(), b.Upper())

	size := up - lo

	counts := pcommon.NewUInt64Slice()
	counts.Append(make([]uint64, size-counts.Len())...)

	for i := 0; i < counts.Len(); i++ {
		counts.SetAt(i, a.Abs(lo+i)+b.Abs(lo+i))
	}

	a.SetOffset(int32(lo))
	counts.MoveTo(a.BucketCounts())
}
