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

package fileintegrationextension // import "github.com/elastic/opentelemetry-collector-components/extension/fileintegrationextension"

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindIntegration(t *testing.T) {
	cases := []struct {
		title       string
		template    string
		expectedErr string
	}{
		{
			title:       "template does not exist",
			template:    "unavailable",
			expectedErr: "not found",
		},
		{
			title:       "invalid template",
			template:    "invalid",
			expectedErr: "invalid integration template format",
		},
		{
			title:    "valid",
			template: "valid",
		},
	}

	extension := newFileTemplateExtension(&Config{Path: "testdata/templates"})

	// Start and Stop have empty implementations, checking just in case.
	err := extension.Start(context.Background(), nil)
	require.NoError(t, err)
	defer func() {
		err := extension.Shutdown(context.Background())
		require.NoError(t, err)
	}()

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			template, err := extension.FindIntegration(context.Background(), c.template)
			if c.expectedErr != "" {
				assert.ErrorContains(t, err, c.expectedErr)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, template)
		})
	}
}
