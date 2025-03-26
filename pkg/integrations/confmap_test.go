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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVarConfmapRetrieve(t *testing.T) {
	cases := []struct {
		title       string
		uri         string
		expectedRaw any
		expectedErr bool
	}{
		{
			title:       "number found",
			uri:         "var:number",
			expectedRaw: 42,
		},
		{
			title: "object found",
			uri:   "var:object",
			expectedRaw: map[string]any{
				"element": "value",
			},
		},
		{
			title:       "list found",
			uri:         "var:list",
			expectedRaw: []any{"foo", "bar", "baz"},
		},
		{
			title:       "wrong scheme",
			uri:         "config:number",
			expectedErr: true,
		},
		{
			title:       "no scheme",
			uri:         "number",
			expectedErr: true,
		},
	}

	provider := variablesProvider{
		variables: map[string]any{
			"number": 42,
			"object": map[string]any{
				"element": "value",
			},
			"list": []any{"foo", "bar", "baz"},
		},
	}

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			retrieved, err := provider.Retrieve(context.Background(), c.uri, nil)
			if c.expectedErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, retrieved)

			raw, err := retrieved.AsRaw()
			require.NoError(t, err)
			assert.EqualValues(t, c.expectedRaw, raw)
		})
	}
}
