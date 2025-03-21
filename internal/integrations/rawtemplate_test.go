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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pipeline"
)

func TestRawTemplateResolve(t *testing.T) {
	cases := []struct {
		title       string
		file        string
		fileErr     string
		params      map[string]any
		pipelines   []pipeline.ID
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
				Receivers: map[component.ID]ComponentConfig{
					component.MustNewID("foo"): map[string]any{
						"somesetting": "foo",
					},
					component.MustNewID("bar"): map[string]any{
						"somecomplexsetting": map[string]any{
							"someobject": map[string]any{
								"value": "bar",
								"other": "baz",
							},
						},
					},
				},
				Processors: map[component.ID]ComponentConfig{
					component.MustNewID("otherprocessor"): nil,
					component.MustNewID("someprocessor"):  nil,
					component.MustNewID("third"): map[string]any{
						"option": "stuff",
					},
				},
				Pipelines: map[pipeline.ID]PipelineConfig{
					pipeline.MustNewID("metrics"): {
						Receiver: idPtr(component.MustNewID("foo")),
						Processors: []component.ID{
							component.MustNewID("someprocessor"),
							component.MustNewID("otherprocessor"),
						},
					},
					pipeline.MustNewID("logs"): {
						Receiver: idPtr(component.MustNewID("bar")),
						Processors: []component.ID{
							component.MustNewID("third"),
						},
					},
					pipeline.MustNewIDWithName("logs", "raw"): {
						Receiver:   idPtr(component.MustNewID("bar")),
						Processors: []component.ID{},
					},
				},
			},
		},
		{
			title: "selected pipeline",
			file:  "template-simple.yaml",
			pipelines: []pipeline.ID{
				pipeline.MustNewID("metrics"),
			},
			params: map[string]any{
				"somevalue": "xxx",
			},
			expected: Config{
				Receivers: map[component.ID]ComponentConfig{
					component.MustNewID("foo"): map[string]any{
						"somesetting": "xxx",
					},
				},
				Processors: map[component.ID]ComponentConfig{
					component.MustNewID("otherprocessor"): nil,
					component.MustNewID("someprocessor"):  nil,
				},
				Pipelines: map[pipeline.ID]PipelineConfig{
					pipeline.MustNewID("metrics"): {
						Receiver: idPtr(component.MustNewID("foo")),
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
			pipelines: []pipeline.ID{
				pipeline.MustNewID("metrics"),
			},
			params: map[string]any{
				"somevalue": map[string]any{
					"subsetting": "foo",
					"array":      []int{1, 2, 3},
				},
			},
			expected: Config{
				Receivers: map[component.ID]ComponentConfig{
					component.MustNewID("foo"): map[string]any{
						"somesetting": map[string]any{
							"subsetting": "foo",
							"array":      []int{1, 2, 3},
						},
					},
				},
				Processors: map[component.ID]ComponentConfig{
					component.MustNewID("otherprocessor"): nil,
					component.MustNewID("someprocessor"):  nil,
				},
				Pipelines: map[pipeline.ID]PipelineConfig{
					pipeline.MustNewID("metrics"): {
						Receiver: idPtr(component.MustNewID("foo")),
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
			pipelines: []pipeline.ID{
				pipeline.MustNewIDWithName("logs", "raw"),
			},
			params: map[string]any{
				"value":      "bar",
				"othervalue": "baz",
			},
			expected: Config{
				Receivers: map[component.ID]ComponentConfig{
					component.MustNewID("bar"): map[string]any{
						"somecomplexsetting": map[string]any{
							"someobject": map[string]any{
								"value": "bar",
								"other": "baz",
							},
						},
					},
				},
				Processors: nil,
				Pipelines: map[pipeline.ID]PipelineConfig{
					pipeline.MustNewIDWithName("logs", "raw"): {
						Receiver:   idPtr(component.MustNewID("bar")),
						Processors: []component.ID{},
					},
				},
			},
		},
		{
			title: "missing variable",
			file:  "template-simple.yaml",
			pipelines: []pipeline.ID{
				pipeline.MustNewID("metrics"),
			},
			expectedErr: "variable \"somevalue\" not found",
		},
		{
			title: "missing pipeline",
			file:  "template-simple.yaml",
			pipelines: []pipeline.ID{
				pipeline.MustNewID("traces"),
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
		{
			title:   "no processors configured",
			file:    "template-no-processors.yaml",
			fileErr: "missing pipelines",
		},
	}

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			var template Integration
			var err error
			template, err = NewRawTemplate(readTemplateFile(c.file))
			if c.fileErr != "" {
				assert.ErrorContains(t, err, c.fileErr)
				return
			}
			require.NoError(t, err)

			config, err := template.Resolve(context.Background(), c.params, c.pipelines)
			if c.expectedErr != "" {
				assert.ErrorContains(t, err, c.expectedErr)
				return
			}

			require.NoError(t, err)
			assertEqualConfigs(t, c.expected, *config)
		})
	}
}

func TestValidateRawTemplate(t *testing.T) {
	cases := []struct {
		title       string
		file        string
		expectedErr string
	}{
		{
			title: "valid without pipelines",
			file:  "template-simple.yaml",
		},
		{
			title:       "missing receiver",
			file:        "template-missing-receiver.yaml",
			expectedErr: "receiver \"foo/missing\" not defined",
		},
		{
			title:       "missing processor",
			file:        "template-missing-processor.yaml",
			expectedErr: "processor \"third/missing\" not defined",
		},
		{
			title:       "missing processor",
			file:        "template-unknown-fields.yaml",
			expectedErr: "field extensions not found",
		},
		{
			title:       "no processors configured",
			file:        "template-no-processors.yaml",
			expectedErr: "missing pipelines",
		},
	}

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			err := ValidateRawTemplate(readTemplateFile(c.file))
			if c.expectedErr != "" {
				assert.ErrorContains(t, err, c.expectedErr)
				return
			}
			assert.NoError(t, err)
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

func idPtr(id component.ID) *component.ID {
	return &id
}
