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

// AbsoluteAt returns the value at absolute index 'at'.
func AbsoluteAt(b Buckets, at int) uint64 {
	idx := at - AbsoluteLower(b)
	if idx >= 0 && idx < b.BucketCounts().Len() {
		return b.BucketCounts().At(idx)
	}
	return 0
}

// AbsoluteUpper returns the minimal index outside the set, such
// that every i < Upper.
func AbsoluteUpper(b Buckets) int {
	return b.BucketCounts().Len() + int(b.Offset())
}

// AbsoluteLower returns the minimal index inside the set, such
// that every i >= Lower.
func AbsoluteLower(b Buckets) int {
	return int(b.Offset())
}
