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
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/datatest"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo/expotest"
)

func TestAbsoluteAt(t *testing.T) {
	is := datatest.New(t)

	bs := expotest.Bins{ø, 1, 2, 3, 4, 5, ø, ø}.Into()

	lo, up := expo.AbsoluteLower(bs), expo.AbsoluteUpper(bs)
	is.Equalf(-2, lo, "lower-bound")
	is.Equalf(3, up, "upper-bound")

	for i := lo; i < up; i++ {
		got := expo.AbsoluteAt(bs, i)
		is.Equal(bs.BucketCounts().At(i+2), got)
	}
}
