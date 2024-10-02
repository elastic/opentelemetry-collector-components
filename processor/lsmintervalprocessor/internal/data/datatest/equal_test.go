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

package datatest

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo/expotest"
)

var t testing.TB = fakeT{}

var datatest = struct{ New func(t testing.TB) T }{New: New}

func ExampleT_Equal() {
	is := datatest.New(t)

	want := expotest.Histogram{
		PosNeg: expotest.Observe(expo.Scale(0), 1, 2, 3, 4),
		Scale:  0,
	}.Into()

	got := expotest.Histogram{
		PosNeg: expotest.Observe(expo.Scale(1), 1, 1, 1, 1),
		Scale:  1,
	}.Into()

	is.Equal(want, got)

	// Output:
	// equal_test.go:55: Negative().BucketCounts().AsRaw(): [1 1 2] != [4]
	// equal_test.go:55: Negative().BucketCounts().Len(): 3 != 1
	// equal_test.go:55: Positive().BucketCounts().AsRaw(): [1 1 2] != [4]
	// equal_test.go:55: Positive().BucketCounts().Len(): 3 != 1
	// equal_test.go:55: Scale(): 0 != 1
}

func TestNone(*testing.T) {}

type fakeT struct {
	testing.TB
}

func (t fakeT) Helper() {}

func (t fakeT) Errorf(format string, args ...any) {
	var from string
	for i := 0; ; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		fn := runtime.FuncForPC(pc)
		if strings.HasSuffix(fn.Name(), ".ExampleT_Equal") {
			from = filepath.Base(file) + ":" + strconv.Itoa(line)
			break
		}
	}

	fmt.Printf("%s: %s\n", from, fmt.Sprintf(format, args...))
}
