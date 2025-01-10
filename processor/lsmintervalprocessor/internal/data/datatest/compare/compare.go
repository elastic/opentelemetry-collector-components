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

package compare // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/datatest/compare"

import (
	"reflect"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var Opts = []cmp.Option{
	cmpopts.EquateApprox(0, 1e-9),
	cmp.Exporter(func(ty reflect.Type) bool {
		return strings.HasPrefix(ty.PkgPath(), "go.opentelemetry.io/collector/pdata") || strings.HasPrefix(ty.PkgPath(), "github.com/open-telemetry/opentelemetry-collector-contrib")
	}),
}

func Equal[T any](a, b T, opts ...cmp.Option) bool {
	return cmp.Equal(a, b, append(Opts, opts...)...)
}

func Diff[T any](a, b T, opts ...cmp.Option) string {
	return cmp.Diff(a, b, append(Opts, opts...)...)
}
