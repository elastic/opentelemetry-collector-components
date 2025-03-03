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

package integrations // import "github.com/elastic/opentelemetry-collector-components/internal/integrations"

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tj/assert"

	"go.opentelemetry.io/collector/component"
)

func TestRawTemplateResolve(t *testing.T) {
	cases := []struct {
		title       string
		file        string
		fileErr     string
		params      map[string]any
		pipelines   []component.ID
		expected    Config
		expectedErr string
	}{
		{
			title: "valid without pipelines",
			file:  "template-simple.yaml",
			params: map[string]any{
				"somevalue":  "foo",
				"value":      "bar",
				"othervalue": "baz",
				"option":     "stuff",
			},
			expected: Config{
				Receivers: map[component.ID]map[string]any{
					component.MustNewID("foo"): {
						"somesetting": "foo",
					},
					component.MustNewID("bar"): {
						"somecomplexsetting": map[string]any{
							"someobject": map[string]any{
								"value": "bar",
								"other": "baz",
							},
						},
					},
				},
				Processors: map[component.ID]map[string]any{
					component.MustNewID("otherprocessor"): nil,
					component.MustNewID("someprocessor"):  nil,
					component.MustNewID("third"): {
						"option": "stuff",
					},
				},
				Pipelines: map[component.ID]PipelineConfig{
					component.MustNewID("metrics"): {
						Receiver: component.MustNewID("foo"),
						Processors: []component.ID{
							component.MustNewID("someprocessor"),
							component.MustNewID("otherprocessor"),
						},
					},
					component.MustNewID("logs"): {
						Receiver: component.MustNewID("bar"),
						Processors: []component.ID{
							component.MustNewID("third"),
						},
					},
					component.MustNewIDWithName("logs", "raw"): {
						Receiver:   component.MustNewID("bar"),
						Processors: []component.ID{},
					},
				},
			},
		},
		{
			title: "selected pipeline",
			file:  "template-simple.yaml",
			pipelines: []component.ID{
				component.MustNewID("metrics"),
			},
			params: map[string]any{
				"somevalue": "xxx",
			},
			expected: Config{
				Receivers: map[component.ID]map[string]any{
					component.MustNewID("foo"): {
						"somesetting": "xxx",
					},
				},
				Processors: map[component.ID]map[string]any{
					component.MustNewID("otherprocessor"): nil,
					component.MustNewID("someprocessor"):  nil,
				},
				Pipelines: map[component.ID]PipelineConfig{
					component.MustNewID("metrics"): {
						Receiver: component.MustNewID("foo"),
						Processors: []component.ID{
							component.MustNewID("someprocessor"),
							component.MustNewID("otherprocessor"),
						},
					},
				},
			},
		},
		{
			title: "complex type in variable",
			file:  "template-simple.yaml",
			pipelines: []component.ID{
				component.MustNewID("metrics"),
			},
			params: map[string]any{
				"somevalue": map[string]any{
					"subsetting": "foo",
					"array":      []int{1, 2, 3},
				},
			},
			expected: Config{
				Receivers: map[component.ID]map[string]any{
					component.MustNewID("foo"): {
						"somesetting": map[string]any{
							"subsetting": "foo",
							"array":      []int{1, 2, 3},
						},
					},
				},
				Processors: map[component.ID]map[string]any{
					component.MustNewID("otherprocessor"): nil,
					component.MustNewID("someprocessor"):  nil,
				},
				Pipelines: map[component.ID]PipelineConfig{
					component.MustNewID("metrics"): {
						Receiver: component.MustNewID("foo"),
						Processors: []component.ID{
							component.MustNewID("someprocessor"),
							component.MustNewID("otherprocessor"),
						},
					},
				},
			},
		},
		{
			title: "selected pipeline with name",
			file:  "template-simple.yaml",
			pipelines: []component.ID{
				component.MustNewIDWithName("logs", "raw"),
			},
			params: map[string]any{
				"value":      "bar",
				"othervalue": "baz",
			},
			expected: Config{
				Receivers: map[component.ID]map[string]any{
					component.MustNewID("bar"): {
						"somecomplexsetting": map[string]any{
							"someobject": map[string]any{
								"value": "bar",
								"other": "baz",
							},
						},
					},
				},
				Processors: nil,
				Pipelines: map[component.ID]PipelineConfig{
					component.MustNewIDWithName("logs", "raw"): {
						Receiver:   component.MustNewID("bar"),
						Processors: []component.ID{},
					},
				},
			},
		},
		{
			title: "missing variable",
			file:  "template-simple.yaml",
			pipelines: []component.ID{
				component.MustNewID("metrics"),
			},
			expectedErr: "variable \"somevalue\" not found",
		},
		{
			title: "missing pipeline",
			file:  "template-simple.yaml",
			pipelines: []component.ID{
				component.MustNewID("traces"),
			},
			expectedErr: "selecting pipelines: component \"traces\" not found",
		},
		{
			title:   "missing receiver",
			file:    "template-missing-receiver.yaml",
			fileErr: "receiver \"foo/missing\" not defined",
		},
		{
			title:   "missing processor",
			file:    "template-missing-processor.yaml",
			fileErr: "processor \"third/missing\" not defined",
		},
		{
			title:   "missing processor",
			file:    "template-unknown-fields.yaml",
			fileErr: "field extensions not found",
		},
	}

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			var template Template
			var err error
			template, err = NewRawTemplate(readTemplateFile(c.file))
			if c.fileErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), c.fileErr)
				return
			}
			require.NoError(t, err)

			config, err := template.Resolve(context.Background(), c.params, c.pipelines)
			if c.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), c.expectedErr)
				return
			}

			require.NoError(t, err)
			assertEqualConfigs(t, c.expected, *config)
		})
	}
}

func readTemplateFile(fileName string) []byte {
	d, err := os.ReadFile(filepath.Join("testdata", fileName))
	if err != nil {
		panic(err)
	}
	return d
}

func assertEqualConfigs(t *testing.T, expected, found Config) {
	assert.EqualValues(t, expected, found)
}
