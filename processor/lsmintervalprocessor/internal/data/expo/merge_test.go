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
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/datatest"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo/expotest"
)

const ø = expotest.Empty

type bins = expotest.Bins

func TestMerge(t *testing.T) {
	cases := []struct {
		a, b bins
		want bins
	}{{
		//         -3 -2 -1 0  1  2  3  4
		a:    bins{ø, ø, ø, ø, ø, ø, ø, ø},
		b:    bins{ø, ø, ø, ø, ø, ø, ø, ø},
		want: bins{ø, ø, ø, ø, ø, ø, ø, ø},
	}, {
		a:    bins{ø, ø, 1, 1, 1, ø, ø, ø},
		b:    bins{ø, 1, 1, ø, ø, ø, ø, ø},
		want: bins{ø, 1, 2, 1, 1, ø, ø, ø},
	}, {
		a:    bins{ø, ø, ø, ø, 1, 1, 1, ø},
		b:    bins{ø, ø, ø, ø, 1, 1, 1, ø},
		want: bins{ø, ø, ø, ø, 2, 2, 2, ø},
	}, {
		a:    bins{ø, 1, 1, ø, ø, ø, ø, ø},
		b:    bins{ø, ø, ø, ø, 1, 1, ø, ø},
		want: bins{ø, 1, 1, 0, 1, 1, ø, ø},
	}}

	for _, cs := range cases {
		a := cs.a.Into()
		b := cs.b.Into()
		want := cs.want.Into()

		name := fmt.Sprintf("(%+d,%d)+(%+d,%d)=(%+d,%d)", a.Offset(), a.BucketCounts().Len(), b.Offset(), b.BucketCounts().Len(), want.Offset(), want.BucketCounts().Len())
		t.Run(name, func(t *testing.T) {
			expo.Merge(a, b)
			is := datatest.New(t)
			is.Equal(want, a)
		})
	}
}
