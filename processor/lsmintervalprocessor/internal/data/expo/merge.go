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

	lo := min(AbsoluteLower(arel), AbsoluteLower(brel))
	up := max(AbsoluteUpper(arel), AbsoluteUpper(brel))

	// Skip leading and trailing zeros to reduce number of buckets.
	// As we cap number of buckets this allows us to have higher scale.
	for lo < up && AbsoluteAt(arel, lo) == 0 && AbsoluteAt(brel, lo) == 0 {
		lo++
	}
	for lo < up-1 && AbsoluteAt(arel, up-1) == 0 && AbsoluteAt(brel, up-1) == 0 {
		up--
	}

	size := up - lo

	// TODO (lahsivjar): the below optimization is not able to take advantage
	// of slices with greater length than what is required as the pdata model
	// does not allow reslicing:
	// https://github.com/open-telemetry/opentelemetry-collector/issues/12004
	aBucketCounts := arel.BucketCounts()
	bBucketCounts := brel.BucketCounts()
	switch {
	case AbsoluteLower(arel) == lo && size == aBucketCounts.Len():
		counts := aBucketCounts
		for i := 0; i < size; i++ {
			val := AbsoluteAt(arel, lo+i) + AbsoluteAt(brel, lo+i)
			counts.SetAt(i, val)
		}
	case AbsoluteLower(brel) == lo && size == bBucketCounts.Len():
		counts := bBucketCounts
		for i := 0; i < size; i++ {
			val := AbsoluteAt(arel, lo+i) + AbsoluteAt(brel, lo+i)
			counts.SetAt(i, val)
		}
		counts.MoveTo(aBucketCounts)
	default:
		counts := pcommon.NewUInt64Slice()
		counts.EnsureCapacity(size)
		for i := 0; i < size; i++ {
			val := AbsoluteAt(arel, lo+i) + AbsoluteAt(brel, lo+i)
			counts.Append(val)
		}
		counts.MoveTo(aBucketCounts)
	}
	arel.SetOffset(int32(lo))
}
