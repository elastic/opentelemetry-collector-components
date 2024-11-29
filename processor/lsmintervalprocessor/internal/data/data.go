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

package data // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo"
)

var (
	_ Point[Number]       = Number{}
	_ Point[Histogram]    = Histogram{}
	_ Point[ExpHistogram] = ExpHistogram{}
	_ Point[Summary]      = Summary{}
)

type Point[Self any] interface {
	StartTimestamp() pcommon.Timestamp
	Timestamp() pcommon.Timestamp
	Attributes() pcommon.Map

	Clone() Self
	CopyTo(Self)

	Add(Self) Self
}

type Typed[Self any] interface {
	Point[Self]
	Number | Histogram | ExpHistogram | Summary
}

type Number struct {
	pmetric.NumberDataPoint
}

func Zero[P Typed[P]]() P {
	var point P
	switch ty := any(&point).(type) {
	case *Number:
		ty.NumberDataPoint = pmetric.NewNumberDataPoint()
	case *Histogram:
		ty.HistogramDataPoint = pmetric.NewHistogramDataPoint()
	case *ExpHistogram:
		ty.DataPoint = pmetric.NewExponentialHistogramDataPoint()
	}
	return point
}

func (dp Number) Clone() Number {
	clone := Number{NumberDataPoint: pmetric.NewNumberDataPoint()}
	if dp.NumberDataPoint != (pmetric.NumberDataPoint{}) {
		dp.CopyTo(clone)
	}
	return clone
}

func (dp Number) CopyTo(dst Number) {
	dp.NumberDataPoint.CopyTo(dst.NumberDataPoint)
}

type Histogram struct {
	pmetric.HistogramDataPoint
}

func (dp Histogram) Clone() Histogram {
	clone := Histogram{HistogramDataPoint: pmetric.NewHistogramDataPoint()}
	if dp.HistogramDataPoint != (pmetric.HistogramDataPoint{}) {
		dp.CopyTo(clone)
	}
	return clone
}

func (dp Histogram) CopyTo(dst Histogram) {
	dp.HistogramDataPoint.CopyTo(dst.HistogramDataPoint)
}

type ExpHistogram struct {
	expo.DataPoint
}

func (dp ExpHistogram) Clone() ExpHistogram {
	clone := ExpHistogram{DataPoint: pmetric.NewExponentialHistogramDataPoint()}
	if dp.DataPoint != (expo.DataPoint{}) {
		dp.CopyTo(clone)
	}
	return clone
}

func (dp ExpHistogram) CopyTo(dst ExpHistogram) {
	dp.DataPoint.CopyTo(dst.DataPoint)
}

type mustPoint[D Point[D]] struct{ _ D }

var (
	_ = mustPoint[Number]{}
	_ = mustPoint[Histogram]{}
	_ = mustPoint[ExpHistogram]{}
)

type Summary struct {
	pmetric.SummaryDataPoint
}

func (dp Summary) Clone() Summary {
	clone := Summary{SummaryDataPoint: pmetric.NewSummaryDataPoint()}
	if dp.SummaryDataPoint != (pmetric.SummaryDataPoint{}) {
		dp.CopyTo(clone)
	}
	return clone
}

func (dp Summary) CopyTo(dst Summary) {
	dp.SummaryDataPoint.CopyTo(dst.SummaryDataPoint)
}
