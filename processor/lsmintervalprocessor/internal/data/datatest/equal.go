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

package datatest // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/deltatocumulativeprocessor/internal/data/datatest"

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/datatest/compare"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
)

// T is the testing helper. Most notably it provides [T.Equal]
type T struct {
	testing.TB
}

func New(tb testing.TB) T {
	return T{TB: tb}
}

// Equal reports whether want and got are deeply equal.
//
// Unlike [reflect.DeepEqual] it first recursively checks exported fields
// and "getters", which are defined as an exported method with:
//   - exactly zero input arguments
//   - exactly one return value
//   - does not start with 'Append'
//   - does not start with 'Clone'
//   - does not return a function
//
// If this yields differences, those are reported and the test fails.
// If the compared values are [pmetric.ExponentialHistogramDataPoint], then
// [pmetrictest.CompareExponentialHistogramDataPoint] is also called.
//
// If no differences are found, it falls back to [assert.Equal].
//
// This was done to aid readability when comparing deeply nested [pmetric]/[pcommon] types,
// because in many cases [assert.Equal] output was found to be barely understandable.
func (is T) Equal(want, got any) {
	is.Helper()
	equal(is.TB, want, got, "")
}

func (is T) Equalf(want, got any, name string) {
	is.Helper()
	equal(is.TB, want, got, name)
}

func equal(tb testing.TB, want, got any, name string) bool {
	tb.Helper()
	require.IsType(tb, want, got)

	vw := reflect.ValueOf(want)
	vg := reflect.ValueOf(got)

	if vw.Kind() != reflect.Struct {
		ok := compare.Equal(want, got)
		if !ok {
			tb.Errorf("%s: %+v != %+v", name, want, got)
		}
		return ok
	}

	ok := true
	// compare all "getters" of the struct
	for i := 0; i < vw.NumMethod(); i++ {
		mname := vw.Type().Method(i).Name
		fname := strings.TrimPrefix(name+"."+mname+"()", ".")

		mw := vw.Method(i)
		mg := vg.Method(i)

		// only compare "getters"
		if mw.Type().NumIn() != 0 || mw.Type().NumOut() != 1 {
			continue
		}

		// skip equality check for methods returning functions
		ret := mw.Type().Out(0)
		if ret.Kind() == reflect.Func {
			continue
		}

		// Append(Empty) fails above heuristic, exclude it
		if strings.HasPrefix(mname, "Append") || strings.HasPrefix(mname, "Clone") {
			continue
		}

		rw := mw.Call(nil)[0].Interface()
		rg := mg.Call(nil)[0].Interface()

		ok = equal(tb, rw, rg, fname) && ok
	}

	// compare all exported fields of the struct
	for i := 0; i < vw.NumField(); i++ {
		if !vw.Type().Field(i).IsExported() {
			continue
		}
		fname := name + "." + vw.Type().Field(i).Name
		fw := vw.Field(i).Interface()
		fg := vg.Field(i).Interface()
		ok = equal(tb, fw, fg, fname) && ok
	}
	if !ok {
		return false
	}

	if _, ok := want.(expo.DataPoint); ok {
		err := pmetrictest.CompareExponentialHistogramDataPoint(want.(expo.DataPoint), got.(expo.DataPoint))
		if err != nil {
			tb.Error(err)
		}
	}

	// fallback to a full deep-equal for rare cases (unexported fields, etc)
	if diff := compare.Diff(want, got); diff != "" {
		tb.Error(diff)
		return false
	}

	return true
}
