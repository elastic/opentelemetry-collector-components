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

package histo // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/deltatocumulativeprocessor/internal/data/histo"

import (
	"slices"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

type DataPoint = pmetric.HistogramDataPoint

type Bounds []float64

// Default boundaries, as defined per SDK spec:
// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/sdk.md#explicit-bucket-histogram-aggregation
var DefaultBounds = Bounds{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000}

func (bs Bounds) Observe(observations ...float64) DataPoint {
	dp := pmetric.NewHistogramDataPoint()
	dp.ExplicitBounds().FromRaw(bs)
	dp.BucketCounts().EnsureCapacity(len(bs) + 1)
	dp.BucketCounts().FromRaw(make([]uint64, len(bs)+1))

	for _, obs := range observations {
		at, _ := slices.BinarySearch(bs, obs)
		dp.BucketCounts().SetAt(at, dp.BucketCounts().At(at))
		dp.SetCount(dp.Count() + 1)
		dp.SetSum(dp.Sum() + obs)

		if !dp.HasMin() {
			dp.SetMin(obs)
		} else {
			dp.SetMin(min(dp.Min(), obs))
		}

		if !dp.HasMax() {
			dp.SetMax(obs)
		} else {
			dp.SetMax(max(dp.Max(), obs))
		}
	}

	return dp
}
