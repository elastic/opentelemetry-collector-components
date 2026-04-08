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
	"sync"

	"go.opentelemetry.io/collector/confmap"

	"github.com/elastic/entcollect"
)

// ProviderFactory creates an entcollect.Provider from receiver
// configuration. Concrete providers (Okta, EntraID, etc.) register
// their factory at init time via blank imports; the receiver looks
// up the factory by name at Start.
type ProviderFactory func(cfg *confmap.Conf) (entcollect.Provider, error)

var (
	mu        sync.RWMutex
	factories = map[string]ProviderFactory{}
)

// Register adds a named provider factory. Providers call this from
// init() so that blank-importing the provider package is sufficient
// to make it available.
func Register(name string, f ProviderFactory) {
	mu.Lock()
	defer mu.Unlock()
	factories[name] = f
}

// Get returns the factory for name and whether it was found.
func Get(name string) (ProviderFactory, bool) {
	mu.RLock()
	defer mu.RUnlock()
	f, ok := factories[name]
	return f, ok
}

// Has reports whether a provider factory is registered under name.
func Has(name string) bool {
	mu.RLock()
	defer mu.RUnlock()
	_, ok := factories[name]
	return ok
}

func unregister(name string) {
	mu.Lock()
	defer mu.Unlock()
	delete(factories, name)
}
