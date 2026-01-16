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

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
)

// Stream represents the identity of a metric stream.
//
// It combines a metric identity with a specific set of data point attributes,
// forming a hashable identifier for a distinct metric time series.
type Stream struct {
	metric Metric
	attrs  [16]byte
}

func (s Stream) Hash() hash.Hash64 {
	sum := s.metric.Hash()
	sum.Write(s.attrs[:])
	return sum
}

func (s Stream) Metric() Metric {
	return s.metric
}

func (s Stream) String() string {
	return fmt.Sprintf("stream/%x", s.Hash().Sum64())
}

// OfStream constructs a Stream identity from a metric identity and a data point.
func OfStream[DataPoint attrPoint](m Metric, dp DataPoint) Stream {
	return Stream{metric: m, attrs: pdatautil.MapHash(dp.Attributes())}
}

type attrPoint interface {
	Attributes() pcommon.Map
}
