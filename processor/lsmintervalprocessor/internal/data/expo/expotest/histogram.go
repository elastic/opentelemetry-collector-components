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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo"
)

type Histogram struct {
	Ts pcommon.Timestamp

	Pos, Neg expo.Buckets
	PosNeg   expo.Buckets

	Scale int
	Count uint64
	Sum   *float64

	Min, Max *float64

	Zt float64
	Zc uint64
}

func (hist Histogram) Into() expo.DataPoint {
	dp := pmetric.NewExponentialHistogramDataPoint()
	dp.SetTimestamp(hist.Ts)

	if !zero(hist.PosNeg) {
		hist.PosNeg.CopyTo(dp.Positive())
		hist.PosNeg.CopyTo(dp.Negative())
	}

	if !zero(hist.Pos) {
		hist.Pos.MoveTo(dp.Positive())
	}
	if !zero(hist.Neg) {
		hist.Neg.MoveTo(dp.Negative())
	}

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

	dp.SetScale(int32(hist.Scale))
	dp.SetZeroThreshold(hist.Zt)
	dp.SetZeroCount(hist.Zc)
	return dp
}

func zero[T comparable](v T) bool {
	return v == *new(T)
}
