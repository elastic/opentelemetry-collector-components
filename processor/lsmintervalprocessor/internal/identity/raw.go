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

package identity // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/identity"

import "go.opentelemetry.io/collector/pdata/pmetric"

// The functions in this file build identities from their stored parts, so
// that encoded values can be indexed without decoding their attributes. An
// identity built this way is equal to the one built from the original pdata.

// ResourceFromAttrsHash returns the identity of a resource whose attributes
// hash to attrs.
func ResourceFromAttrsHash(attrs [16]byte) Resource {
	return Resource{attrs: attrs}
}

// AttrsHash returns the hash of the resource attributes.
func (r Resource) AttrsHash() [16]byte {
	return r.attrs
}

// NewScope returns the identity of a scope of res.
func NewScope(res Resource, name, version string, attrs [16]byte) Scope {
	return Scope{resource: res, name: name, version: version, attrs: attrs}
}

// Name returns the scope name.
func (s Scope) Name() string { return s.name }

// Version returns the scope version.
func (s Scope) Version() string { return s.version }

// AttrsHash returns the hash of the scope attributes.
func (s Scope) AttrsHash() [16]byte { return s.attrs }

// NewMetric returns the identity of a metric of scope.
func NewMetric(
	scope Scope,
	name, unit string,
	ty pmetric.MetricType,
	monotonic bool,
	temporality pmetric.AggregationTemporality,
) Metric {
	return Metric{
		scope:       scope,
		name:        name,
		unit:        unit,
		ty:          ty,
		monotonic:   monotonic,
		temporality: temporality,
	}
}

// Name returns the metric name.
func (i Metric) Name() string { return i.name }

// Unit returns the metric unit.
func (i Metric) Unit() string { return i.unit }

// Type returns the metric type.
func (i Metric) Type() pmetric.MetricType { return i.ty }

// Monotonic reports whether the metric is monotonic.
func (i Metric) Monotonic() bool { return i.monotonic }

// Temporality returns the aggregation temporality of the metric.
func (i Metric) Temporality() pmetric.AggregationTemporality { return i.temporality }

// NewStream returns the identity of a stream of m whose attributes hash to
// attrs.
func NewStream(m Metric, attrs [16]byte) Stream {
	return Stream{metric: m, attrs: attrs}
}

// AttrsHash returns the hash of the stream attributes.
func (i Stream) AttrsHash() [16]byte { return i.attrs }
