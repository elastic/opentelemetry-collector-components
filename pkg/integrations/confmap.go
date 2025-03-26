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

package integrations // import "github.com/elastic/opentelemetry-collector-components/pkg/integrations"

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/confmap"
)

const (
	varProviderScheme = "var"
)

var _ confmap.Provider = &variablesProvider{}

func newVariablesProviderFactory(variables map[string]any) confmap.ProviderFactory {
	return confmap.NewProviderFactory(createVariablesProvider(variables))
}

func createVariablesProvider(variables map[string]any) confmap.CreateProviderFunc {
	return func(_ confmap.ProviderSettings) confmap.Provider {
		return &variablesProvider{variables: variables}
	}
}

type variablesProvider struct {
	variables map[string]any
}

// Retrieves obtains variables preconfigured in the provider on creation.
func (p *variablesProvider) Retrieve(ctx context.Context, uri string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
	scheme, key, found := strings.Cut(uri, ":")
	if !found || scheme != varProviderScheme {
		return nil, fmt.Errorf("unexpected scheme in uri %q, expected %q", uri, varProviderScheme)
	}
	value, found := p.variables[key]
	if !found {
		return nil, fmt.Errorf("variable %q not found", key)
	}

	return confmap.NewRetrieved(value)
}

// Scheme returns the scheme of this provider.
func (p *variablesProvider) Scheme() string {
	return varProviderScheme
}

// Shutdown does not do anything on this provider.
func (p *variablesProvider) Shutdown(ctx context.Context) error {
	return nil
}
