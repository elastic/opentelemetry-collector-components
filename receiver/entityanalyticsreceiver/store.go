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

package entityanalyticsreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/entityanalyticsreceiver"

import (
	"fmt"

	"go.opentelemetry.io/collector/component"

	"github.com/elastic/entcollect"
)

// resolveRegistry looks up a storage extension by component ID and
// asserts it implements entcollect.Registry. The storage extension
// (e.g. elasticsearchstorage from beats) satisfies this interface
// at runtime; the receiver has no compile-time dependency on beats.
func resolveRegistry(host component.Host, storageID string) (entcollect.Registry, error) {
	var id component.ID
	if err := id.UnmarshalText([]byte(storageID)); err != nil {
		return nil, fmt.Errorf("invalid storage extension ID %q: %w", storageID, err)
	}
	ext, ok := host.GetExtensions()[id]
	if !ok {
		return nil, fmt.Errorf("storage extension %s not found", id)
	}
	reg, ok := ext.(entcollect.Registry)
	if !ok {
		return nil, fmt.Errorf("extension %s does not implement entcollect.Registry", id)
	}
	return reg, nil
}
