// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

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

	// Skip leading and trailing zeros to reduce number of buckets.
	// As we cap number of buckets this allows us to have higher scale.
	for lo < up && a.Abs(lo) == 0 && b.Abs(lo) == 0 {
		lo++
	}
	for lo < up-1 && a.Abs(up-1) == 0 && b.Abs(up-1) == 0 {
		up--
	}

	size := up - lo

	// TODO (lahsivjar): the below optimization is not able to take advantage
	// of slices with greater length than what is required as the pdata model
	// does not allow reslicing:
	// https://github.com/open-telemetry/opentelemetry-collector/issues/12004
	switch {
	case a.Lower() == lo && size == a.BucketCounts().Len():
		counts := a.BucketCounts()
		for i := 0; i < size; i++ {
			val := a.Abs(lo+i) + b.Abs(lo+i)
			counts.SetAt(i, val)
		}
	case b.Lower() == lo && size == b.BucketCounts().Len():
		counts := b.BucketCounts()
		for i := 0; i < size; i++ {
			val := a.Abs(lo+i) + b.Abs(lo+i)
			counts.SetAt(i, val)
		}
		counts.MoveTo(a.BucketCounts())
	default:
		counts := pcommon.NewUInt64Slice()
		counts.EnsureCapacity(size)
		for i := 0; i < size; i++ {
			val := a.Abs(lo+i) + b.Abs(lo+i)
			counts.Append(val)
		}
		counts.MoveTo(a.BucketCounts())
	}

	a.SetOffset(int32(lo))
}
