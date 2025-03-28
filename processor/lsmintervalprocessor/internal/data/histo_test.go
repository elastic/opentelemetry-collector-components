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

package data

import (
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/datatest"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/histo"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/histo/histotest"
)

func TestHistoAdd(t *testing.T) {
	type histdp = histotest.Histogram
	obs := histotest.Bounds(histo.DefaultBounds).Observe

	cases := []struct {
		name   string
		dp, in histdp
		want   histdp
		flip   bool
	}{{
		name: "noop",
	}, {
		name: "simple",
		dp:   obs(-12, 5.5, 7.3, 43.3, 412.4 /*              */),
		in:   obs( /*                      */ 4.3, 14.5, 2677.4),
		want: obs(-12, 5.5, 7.3, 43.3, 412.4, 4.3, 14.5, 2677.4),
	}, {
		name: "diff-len",
		dp:   histdp{Buckets: []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, Count: 11},
		in:   histdp{Buckets: []uint64{1, 1, 1, 1, 1 /*             */}, Count: 5},
		want: histdp{Buckets: []uint64{2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1}, Count: 11 + 5},
	}, {
		name: "diff-bounds",
		dp:   histotest.Bounds{12, 17}.Observe(3, 14, 187),
		in:   histotest.Bounds{34, 55}.Observe(8, 77, 142),
		want: histotest.Bounds{34, 55}.Observe(8, 77, 142),
	}, {
		name: "no-counts",
		dp:   histdp{Count: 42 /**/, Sum: ptr(777.12 /*   */), Min: ptr(12.3), Max: ptr(66.8)},
		in:   histdp{Count: /**/ 33, Sum: ptr( /*   */ 568.2), Min: ptr(8.21), Max: ptr(23.6)},
		want: histdp{Count: 42 + 33, Sum: ptr(777.12 + 568.2), Min: ptr(8.21), Max: ptr(66.8)},
	}, {
		name: "optional-missing",
		dp:   histdp{Count: 42 /**/, Sum: ptr(777.0) /*   */, Min: ptr(12.3), Max: ptr(66.8)},
		in:   histdp{Count: /**/ 33},
		want: histdp{Count: 42 + 33},
	}}

	for _, cs := range cases {
		t.Run(cs.name, func(t *testing.T) {
			var add Adder
			is := datatest.New(t)

			var (
				dp   = cs.dp.Into()
				in   = cs.in.Into()
				want = cs.want.Into()
			)

			err := add.Histograms(dp, in)
			is.Equal(nil, err)
			is.Equal(want, dp)
		})
	}
}

func ptr[T any](v T) *T {
	return &v
}
