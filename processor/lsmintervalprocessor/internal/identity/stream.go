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
// https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/internal/exp/metrics/identity

package identity // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/identity"

import (
	"hash"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
)

type Stream struct {
	metric
	attrs [16]byte
}

func (i Stream) Hash() hash.Hash64 {
	sum := i.metric.Hash()
	sum.Write(i.attrs[:])
	return sum
}

func (i Stream) Metric() Metric {
	return i.metric
}

func OfStream[DataPoint attrPoint](m Metric, dp DataPoint) Stream {
	return Stream{metric: m, attrs: pdatautil.MapHash(dp.Attributes())}
}

type attrPoint interface {
	Attributes() pcommon.Map
}
