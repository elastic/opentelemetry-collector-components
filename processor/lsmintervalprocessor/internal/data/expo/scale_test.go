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

package expo_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/datatest"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo"
)

func TestDownscale(t *testing.T) {
	type Repr[T any] struct {
		scale expo.Scale
		bkt   T
	}

	cases := [][]Repr[string]{{
		{scale: 2, bkt: "1 1 1 1 1 1 1 1 1 1 1 1"},
		{scale: 1, bkt: " 2   2   2   2   2   2 "},
		{scale: 0, bkt: "   4       4       4   "},
	}, {
		{scale: 2, bkt: "ø 1 1 1 1 1 1 1 1 1 1 1"},
		{scale: 1, bkt: " 1   2   2   2   2   2 "},
		{scale: 0, bkt: "   3       4       4   "},
	}, {
		{scale: 2, bkt: "ø ø 1 1 1 1 1 1 1 1 1 1"},
		{scale: 1, bkt: " ø   2   2   2   2   2 "},
		{scale: 0, bkt: "   2       4       4   "},
	}, {
		{scale: 2, bkt: "ø ø ø ø 1 1 1 1 1 1 1 1"},
		{scale: 1, bkt: " ø   ø   2   2   2   2 "},
		{scale: 0, bkt: "   ø       4       4   "},
	}, {
		{scale: 2, bkt: "1 1 1 1 1 1 1 1 1      "},
		{scale: 1, bkt: " 2   2   2   2   1     "},
		{scale: 0, bkt: "   4       4       1   "},
	}, {
		{scale: 2, bkt: "1 1 1 1 1 1 1 1 1 1 1 1"},
		{scale: 0, bkt: "   4       4       4   "},
	}, {
		{scale: 1, bkt: "ø 1 1 0"},
		{scale: 0, bkt: " 1   1 "},
	}, {
		{scale: 1, bkt: "ø 1 1 "},
		{scale: 0, bkt: " 1   1"},
	}, {
		{scale: 1, bkt: " - 1 1 "},
		{scale: 0, bkt: "- 1   1"},
	}, {
		{scale: 5, bkt: "-  4 0 3 0 3 0 0 8   "},
		{scale: 4, bkt: "- 4   3   3   0   8  "},
	}}

	type B = expo.Buckets
	for i, reprs := range cases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			buckets := make([]Repr[B], len(reprs))
			for i, r := range reprs {
				bkt := pmetric.NewExponentialHistogramDataPointBuckets()
				for _, elem := range strings.Fields(r.bkt) {
					if elem == "ø" {
						bkt.SetOffset(bkt.Offset() + 1)
						continue
					}
					if elem == "-" {
						bkt.SetOffset(bkt.Offset() - 1)
						continue
					}
					n, err := strconv.Atoi(elem)
					if err != nil {
						panic(err)
					}
					bkt.BucketCounts().Append(uint64(n))
				}
				buckets[i] = Repr[B]{scale: r.scale, bkt: bkt}
			}

			is := datatest.New(t)
			for i := 0; i < len(buckets)-1; i++ {
				expo.Downscale(buckets[i].bkt, buckets[i].scale, buckets[i+1].scale)

				is.Equalf(buckets[i+1].bkt.Offset(), buckets[i].bkt.Offset(), "offset")

				want := buckets[i+1].bkt.BucketCounts().AsRaw()
				got := buckets[i].bkt.BucketCounts().AsRaw()

				is.Equalf(want, got[:len(want)], "counts")
				is.Equalf(make([]uint64, len(got)-len(want)), got[len(want):], "extra-space")
			}
		})
	}

	t.Run("panics", func(t *testing.T) {
		assert.PanicsWithValue(t, "cannot upscale without introducing error (8 -> 12)", func() {
			expo.Downscale(bins{}.Into(), 8, 12)
		})
	})
}
