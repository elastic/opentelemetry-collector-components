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

package expotest // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/deltatocumulativeprocessor/internal/data/expo/expotest"

import (
	"fmt"
	"math"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo"
)

const (
	Empty = math.MaxUint64
	ø     = Empty
)

// index:  0  1  2 3 4 5 6 7
// bucket: -3 -2 -1 0 1 2 3 4
// bounds: (0.125,0.25], (0.25,0.5], (0.5,1], (1,2], (2,4], (4,8], (8,16], (16,32]
type Bins [8]uint64

func (bins Bins) Into() expo.Buckets {
	start := 0
	for i := 0; i < len(bins); i++ {
		if bins[i] != ø {
			start = i
			break
		}
	}

	end := len(bins)
	for i := start; i < len(bins); i++ {
		if bins[i] == ø {
			end = i
			break
		}
	}

	counts := bins[start:end]

	buckets := pmetric.NewExponentialHistogramDataPointBuckets()
	buckets.SetOffset(int32(start - 3))
	buckets.BucketCounts().FromRaw(counts)
	return buckets
}

func ObserveInto(bs expo.Buckets, scale expo.Scale, pts ...float64) {
	counts := bs.BucketCounts()

	for _, pt := range pts {
		pt = math.Abs(pt)
		if pt <= 0.125 || pt > 32 {
			panic(fmt.Sprintf("out of bounds: 0.125 < %f <= 32", pt))
		}

		idx := scale.Idx(pt) - int(bs.Offset())
		switch {
		case idx < 0:
			bs.SetOffset(bs.Offset() + int32(idx))
			counts.FromRaw(append(make([]uint64, -idx), counts.AsRaw()...))
			idx = 0
		case idx >= counts.Len():
			counts.Append(make([]uint64, idx-counts.Len()+1)...)
		}

		counts.SetAt(idx, counts.At(idx)+1)
	}
}

func Observe(scale expo.Scale, pts ...float64) expo.Buckets {
	bs := pmetric.NewExponentialHistogramDataPointBuckets()
	ObserveInto(bs, scale, pts...)
	return bs
}

func Observe0(pts ...float64) expo.Buckets {
	return Observe(0, pts...)
}
