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

// Package expo implements various operations on exponential histograms and their bucket counts
package expo // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo"

import "go.opentelemetry.io/collector/pdata/pmetric"

type (
	DataPoint = pmetric.ExponentialHistogramDataPoint
	Buckets   = pmetric.ExponentialHistogramDataPointBuckets
)

// Abs returns a view into the buckets using an absolute scale
func Abs(bs Buckets) Absolute {
	return Absolute{buckets: bs}
}

type buckets = Buckets

// Absolute addresses bucket counts using an absolute scale, such that it is
// interoperable with [Scale].
//
// It spans from [[Absolute.Lower]:[Absolute.Upper]]
//
// NOTE: The zero-value is unusable, use [Abs] to construct
type Absolute struct {
	buckets
}

// Abs returns the value at absolute index 'at'
func (a Absolute) Abs(at int) uint64 {
	if i, ok := a.idx(at); ok {
		return a.BucketCounts().At(i)
	}
	return 0
}

// Upper returns the minimal index outside the set, such that every i < Upper
func (a Absolute) Upper() int {
	return a.BucketCounts().Len() + int(a.Offset())
}

// Lower returns the minimal index inside the set, such that every i >= Lower
func (a Absolute) Lower() int {
	return int(a.Offset())
}

func (a Absolute) idx(at int) (int, bool) {
	idx := at - a.Lower()
	return idx, idx >= 0 && idx < a.BucketCounts().Len()
}
