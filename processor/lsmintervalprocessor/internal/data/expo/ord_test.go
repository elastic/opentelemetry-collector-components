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

	"github.com/stretchr/testify/assert"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/data/expo"
)

func TestHiLo(t *testing.T) {
	type T struct {
		int int
		str string
	}

	a := T{int: 0, str: "foo"}
	b := T{int: 1, str: "bar"}

	{
		hi, lo := expo.HiLo(a, b, func(v T) int { return v.int })
		assert.Equal(t, a, lo)
		assert.Equal(t, b, hi)
	}

	{
		hi, lo := expo.HiLo(a, b, func(v T) string { return v.str })
		assert.Equal(t, b, lo)
		assert.Equal(t, a, hi)
	}

	{
		hi, lo := expo.HiLo(a, b, func(T) int { return 0 })
		assert.Equal(t, a, hi)
		assert.Equal(t, b, lo)
	}
}
