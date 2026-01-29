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

package enrichments

import (
	"testing"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestScopeEnrich(t *testing.T) {
	for _, tc := range []struct {
		name          string
		input         pcommon.InstrumentationScope
		config        config.ScopeConfig
		enrichedAttrs map[string]any
	}{
		{
			name:          "all_disabled",
			input:         pcommon.NewInstrumentationScope(),
			enrichedAttrs: map[string]any{},
		},
		{
			name: "with_scope_name",
			input: func() pcommon.InstrumentationScope {
				scope := pcommon.NewInstrumentationScope()
				scope.SetName("test")
				return scope
			}(),
			config: config.Enabled().Scope,
			enrichedAttrs: map[string]any{
				elasticattr.ServiceFrameworkName:    "test",
				elasticattr.ServiceFrameworkVersion: "",
			},
		},
		{
			name: "with_scope_name_version",
			input: func() pcommon.InstrumentationScope {
				scope := pcommon.NewInstrumentationScope()
				scope.SetName("test")
				scope.SetVersion("v1.0.0")
				return scope
			}(),
			config: config.Enabled().Scope,
			enrichedAttrs: map[string]any{
				elasticattr.ServiceFrameworkName:    "test",
				elasticattr.ServiceFrameworkVersion: "v1.0.0",
			},
		},
		{
			name: "existing_attributes_not_overridden",
			input: func() pcommon.InstrumentationScope {
				scope := pcommon.NewInstrumentationScope()
				scope.SetName("test")
				scope.SetVersion("v1.0.0")
				scope.Attributes().PutStr(elasticattr.ServiceFrameworkName, "existing-framework-name")
				scope.Attributes().PutStr(elasticattr.ServiceFrameworkVersion, "existing-framework-version")
				return scope
			}(),
			config: config.Enabled().Scope,
			enrichedAttrs: map[string]any{
				elasticattr.ServiceFrameworkName:    "existing-framework-name",
				elasticattr.ServiceFrameworkVersion: "existing-framework-version",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Merge existing resource attrs with the attrs added
			// by enrichment to get the expected attributes.
			expectedAttrs := tc.input.Attributes().AsRaw()
			for k, v := range tc.enrichedAttrs {
				expectedAttrs[k] = v
			}

			EnrichScope(tc.input, config.Config{
				Scope: tc.config,
			})

			assert.Empty(t, cmp.Diff(expectedAttrs, tc.input.Attributes().AsRaw()))
		})
	}
}
