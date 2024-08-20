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

package templateprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/templateprocessor"

import (
	"context"
	"strings"

	"go.opentelemetry.io/collector/confmap"
)

const (
	varProviderScheme = "var"
)

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

func (p *variablesProvider) Retrieve(ctx context.Context, uri string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
	key := strings.TrimLeft(uri, varProviderScheme+":")
	value, found := p.variables[key]
	if !found {
		// FIXME: Resolve relevant configuration only instead of the whole template, so we don't need to ignore
		// variables not found in unrelevant parts.
		//return nil, fmt.Errorf("variable %q not found", key)
		return confmap.NewRetrieved("")
	}

	return confmap.NewRetrieved(value)
}

func (p *variablesProvider) Scheme() string {
	return varProviderScheme
}

func (p *variablesProvider) Shutdown(ctx context.Context) error {
	return nil
}
