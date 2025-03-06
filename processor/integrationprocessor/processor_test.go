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

package integrationprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/integrationprocessor"

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/elastic/opentelemetry-collector-components/internal/integrations"
	"github.com/elastic/opentelemetry-collector-components/processor/integrationprocessor/internal/metadata"
)

func TestConsumeLogs(t *testing.T) {
	cases := []struct {
		title        string
		setup        func(*Config)
		inputFile    string
		expectedFile string
		startErr     string
	}{
		{
			title: "empty pipeline",
			setup: func(config *Config) {
				config.Name = "empty"
				config.Pipeline = pipeline.MustNewID("logs")
			},
			inputFile:    "logs.yaml",
			expectedFile: "logs-no-processing.yaml",
		},
		{
			title: "undefined integration",
			setup: func(config *Config) {
				config.Name = "undefined"
			},
			startErr: `failed to find integration "undefined": not found`,
		},
		{
			title: "undefined pipeline",
			setup: func(config *Config) {
				config.Name = "empty"
				config.Pipeline = pipeline.MustNewID("traces")
			},
			startErr: `component "traces" not found`,
		},

		{
			title: "use variable",
			setup: func(config *Config) {
				config.Name = "addattribute"
				config.Pipeline = pipeline.MustNewID("logs")
				config.Parameters = map[string]any{
					"resource": "test",
				}
			},
			inputFile:    "logs.yaml",
			expectedFile: "logs-add-attribute.yaml",
		},
		{
			title: "missing variable",
			setup: func(config *Config) {
				config.Name = "addattribute"
				config.Pipeline = pipeline.MustNewID("logs")
			},
			startErr: `variable "resource" not found`,
		},

		{
			title: "correct order in multiple processors",
			setup: func(config *Config) {
				config.Name = "multipleprocessors"
				config.Pipeline = pipeline.MustNewID("logs")
				config.Parameters = map[string]any{
					"resource": "test",
				}
			},
			inputFile:    "logs.yaml",
			expectedFile: "logs-processors-order.yaml",
		},
	}

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			testConsumeLogs(t, c.setup, c.inputFile, c.expectedFile, c.startErr)
		})
	}
}

func testConsumeLogs(t *testing.T, setup func(*Config), inputFile, expectedFile, startErr string) {
	factory := NewFactory()
	config := factory.CreateDefaultConfig().(*Config)
	setup(config)

	sink := new(consumertest.LogsSink)
	p, err := factory.CreateLogs(context.Background(), processortest.NewNopSettings(metadata.Type), config, sink)
	require.NoError(t, err)

	c := p.(component.Component)
	err = c.Start(context.Background(), newMockHost("testdata/templates"))
	if startErr != "" {
		require.Error(t, err)
		assert.Contains(t, err.Error(), startErr)
		return
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Shutdown(context.Background())) })

	input, err := golden.ReadLogs(filepath.Join("testdata", inputFile))
	require.NoError(t, err)

	assert.NoError(t, p.ConsumeLogs(context.Background(), input))

	actual := sink.AllLogs()
	require.True(t, len(actual) > 0)

	expectedFile = filepath.Join("testdata", "expected-"+expectedFile)
	if _, err := os.Stat(expectedFile); errors.Is(err, os.ErrNotExist) {
		err := golden.WriteLogs(t, expectedFile, actual[0])
		require.NoError(t, err)
	}
	expected, err := golden.ReadLogs(expectedFile)
	require.NoError(t, err)

	assert.NoError(t, plogtest.CompareLogs(expected, actual[0]))
}

func newMockHost(path string) *mockedHost {
	return &mockedHost{path: path}
}

type mockedHost struct {
	path string
}

func (m *mockedHost) GetExtensions() map[component.ID]component.Component {
	return map[component.ID]component.Component{
		component.MustNewID("testfinder"): m,
	}
}

func (m *mockedHost) GetFactory(kind component.Kind, ctype component.Type) component.Factory {
	switch {
	case kind == component.KindProcessor && ctype.String() == "transform":
		return transformprocessor.NewFactory()
	}
	return nil
}

func (m *mockedHost) Start(context.Context, component.Host) error {
	return nil
}

func (m *mockedHost) Shutdown(context.Context) error {
	return nil
}

func (m *mockedHost) FindIntegration(ctx context.Context, name string) (integrations.Integration, error) {
	path := filepath.Join(m.path, name+".yaml")
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, integrations.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return integrations.NewRawTemplate(raw)
}
