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
	"cmp"
	"fmt"
)

// WidenZero widens the zero-bucket to span at least [-width,width], possibly wider
// if min falls in the middle of a bucket.
//
// Both buckets counts MUST be of same scale.
func WidenZero(dp DataPoint, width float64) {
	switch {
	case width == dp.ZeroThreshold():
		return
	case width < dp.ZeroThreshold():
		panic(fmt.Sprintf("min must be larger than current threshold (%f)", dp.ZeroThreshold()))
	}

	scale := Scale(dp.Scale())
	zero := scale.Idx(width) // the largest bucket index inside the zero width

	widen := func(bs Buckets) {
		abs := Abs(bs)
		for i := abs.Lower(); i <= zero; i++ {
			dp.SetZeroCount(dp.ZeroCount() + abs.Abs(i))
		}

		// right next to the new zero bucket, constrained to slice range
		lo := clamp(zero+1, abs.Lower(), abs.Upper())
		abs.Slice(lo, abs.Upper())
	}

	widen(dp.Positive())
	widen(dp.Negative())

	_, max := scale.Bounds(zero)
	dp.SetZeroThreshold(max)
}

// Slice drops data outside the range from <= i < to from the bucket counts. It behaves the same as Go's [a:b]
//
// Limitations:
//   - due to a limitation of the pcommon package, slicing cannot happen in-place and allocates
//   - in consequence, data outside the range is garbage collected
func (a Absolute) Slice(from, to int) {
	lo, up := a.Lower(), a.Upper()
	switch {
	case from > to:
		panic(fmt.Sprintf("bad bounds: must be from<=to (got %d<=%d)", from, to))
	case from < lo || to > up:
		panic(fmt.Sprintf("%d:%d is out of bounds for %d:%d", from, to, lo, up))
	}

	first := from - lo
	last := to - lo

	a.BucketCounts().FromRaw(a.BucketCounts().AsRaw()[first:last])
	a.SetOffset(int32(from))
}

// clamp constraints v to the range up..=lo
func clamp[N cmp.Ordered](v, lo, up N) N {
	return max(lo, min(v, up))
}
