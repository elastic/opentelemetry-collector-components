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

package identity // import "github.com/elastic/opentelemetry-collector-components/connector/profilingmetricsconnector/internal/identity"

import (
	"fmt"
	"hash"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type Metric struct {
	scope

	name string
	unit string
	ty   pmetric.MetricType

	monotonic   bool
	temporality pmetric.AggregationTemporality
}

func (m Metric) Hash() hash.Hash64 {
	sum := m.scope.Hash()
	sum.Write([]byte(m.name))
	sum.Write([]byte(m.unit))

	var mono byte
	if m.monotonic {
		mono = 1
	}
	sum.Write([]byte{byte(m.ty), mono, byte(m.temporality)})
	return sum
}

func (m Metric) Scope() Scope {
	return m.scope
}

func OfMetric(scope Scope, m pmetric.Metric) Metric {
	id := Metric{
		scope: scope,
		name:  m.Name(),
		unit:  m.Unit(),
		ty:    m.Type(),
	}

	switch m.Type() {
	case pmetric.MetricTypeSum:
		sum := m.Sum()
		id.monotonic = sum.IsMonotonic()
		id.temporality = sum.AggregationTemporality()
	case pmetric.MetricTypeExponentialHistogram:
		exp := m.ExponentialHistogram()
		id.monotonic = true
		id.temporality = exp.AggregationTemporality()
	case pmetric.MetricTypeHistogram:
		hist := m.Histogram()
		id.monotonic = true
		id.temporality = hist.AggregationTemporality()
	}

	return id
}

func (m Metric) String() string {
	return fmt.Sprintf("metric/%x", m.Hash().Sum64())
}

func OfResourceMetric(res pcommon.Resource, scope pcommon.InstrumentationScope, metric pmetric.Metric) Metric {
	return OfMetric(OfScope(OfResource(res), scope), metric)
}
