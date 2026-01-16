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

// Scope represents the identity of an OpenTelemetry instrumentation scope.
//
// It combines the resource identity with the scope name, version, and
// attributes to form a hashable identifier for metrics produced
// by a specific instrumentation scope.
type Scope struct {
	resource Resource

	name    string
	version string
	attrs   [16]byte
}

func (s Scope) Hash() hash.Hash64 {
	sum := s.resource.Hash()
	sum.Write([]byte(s.name))
	sum.Write([]byte(s.version))
	sum.Write(s.attrs[:])
	return sum
}

func (s Scope) Resource() Resource {
	return s.resource
}

func (s Scope) String() string {
	return fmt.Sprintf("scope/%x", s.Hash().Sum64())
}

// OfScope constructs a Scope identity from a resource identity and an
// OpenTelemetry instrumentation scope.
func OfScope(res Resource, scope pcommon.InstrumentationScope) Scope {
	return Scope{
		resource: res,
		name:     scope.Name(),
		version:  scope.Version(),
		attrs:    pdatautil.MapHash(scope.Attributes()),
	}
}
