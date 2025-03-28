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

package histotest // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/histo/histotest"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/histo"
)

type Histogram struct {
	Ts pcommon.Timestamp

	Bounds  histo.Bounds
	Buckets []uint64

	Count uint64
	Sum   *float64

	Min, Max *float64
}

func (hist Histogram) Into() pmetric.HistogramDataPoint {
	dp := pmetric.NewHistogramDataPoint()

	dp.SetTimestamp(hist.Ts)

	dp.ExplicitBounds().FromRaw(hist.Bounds)
	if hist.Bounds == nil {
		dp.ExplicitBounds().FromRaw(histo.DefaultBounds)
	}
	dp.BucketCounts().FromRaw(hist.Buckets)

	dp.SetCount(hist.Count)
	if hist.Sum != nil {
		dp.SetSum(*hist.Sum)
	}

	if hist.Min != nil {
		dp.SetMin(*hist.Min)
	}
	if hist.Max != nil {
		dp.SetMax(*hist.Max)
	}

	return dp
}

type Bounds histo.Bounds

func (bs Bounds) Observe(observations ...float64) Histogram {
	dp := histo.Bounds(bs).Observe(observations...)
	return Histogram{
		Ts:      dp.Timestamp(),
		Bounds:  dp.ExplicitBounds().AsRaw(),
		Buckets: dp.BucketCounts().AsRaw(),
		Count:   dp.Count(),
		Sum:     ptr(dp.Sum()),
		Min:     ptr(dp.Min()),
		Max:     ptr(dp.Max()),
	}
}

func ptr[T any](v T) *T {
	return &v
}
