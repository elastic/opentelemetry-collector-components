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

package integrationreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/integrationreceiver"

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/elastic/opentelemetry-collector-components/internal/integrations"
	"github.com/elastic/opentelemetry-collector-components/receiver/integrationreceiver/internal/metadata"
)

func TestConsumeLogs(t *testing.T) {
	cases := []struct {
		title        string
		setup        func(*Config)
		expectedFile string
		startErr     string
	}{
		{
			title: "empty processing pipeline",
			setup: func(config *Config) {
				config.Name = "filelog"
				config.Pipelines = []component.ID{component.MustNewID("logs")}
				config.Parameters = map[string]any{
					"paths": filepath.Join("testdata", "logs", "test-simple.log"),
				}
			},
			expectedFile: "logs-no-processing.yaml",
		},
		{
			title: "pipeline with processors",
			setup: func(config *Config) {
				config.Name = "filelog"
				config.Pipelines = []component.ID{component.MustNewIDWithName("logs", "processed")}
				config.Parameters = map[string]any{
					"paths":    filepath.Join("testdata", "logs", "test-simple.log"),
					"resource": "test",
				}
			},
			expectedFile: "logs-with-attributes.yaml",
		},
		{
			title: "missing variable in processor",
			setup: func(config *Config) {
				config.Name = "filelog"
				config.Pipelines = []component.ID{component.MustNewIDWithName("logs", "processed")}
				config.Parameters = map[string]any{
					"paths": filepath.Join("testdata", "logs", "test-simple.log"),
				}
			},
			startErr: `variable "resource" not found`,
		},
		{
			title: "receiver without factory",
			setup: func(config *Config) {
				config.Name = "filelog"
				config.Pipelines = []component.ID{component.MustNewIDWithName("logs", "undefined")}
			},
			startErr: `could not find receiver factory for "undefined"`,
		},

		{
			title: "no receiver in pipeline",
			setup: func(config *Config) {
				config.Name = "no-receiver"
				config.Pipelines = []component.ID{component.MustNewID("logs")}
			},
			startErr: "no receiver in pipeline configuration",
		},
	}

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			testConsumeLogs(t, c.setup, c.expectedFile, c.startErr)
		})
	}
}

func testConsumeLogs(t *testing.T, setup func(*Config), expectedFile, startErr string) {
	factory := NewFactory()
	config := factory.CreateDefaultConfig().(*Config)
	setup(config)

	sink := new(consumertest.LogsSink)
	p, err := factory.CreateLogs(context.Background(), receivertest.NewNopSettings(metadata.Type), config, sink)
	require.NoError(t, err)

	err = p.Start(context.Background(), newMockHost("testdata/templates"))
	if startErr != "" {
		require.Error(t, err)
		assert.Contains(t, err.Error(), startErr)
		return
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Shutdown(context.Background())) })

	waitLogsSink(t, sink)
	actual := sink.AllLogs()

	expectedFile = filepath.Join("testdata", "expected-"+expectedFile)
	if _, err := os.Stat(expectedFile); errors.Is(err, os.ErrNotExist) {
		err := golden.WriteLogs(t, expectedFile, actual[0])
		require.NoError(t, err)
	}
	expected, err := golden.ReadLogs(expectedFile)
	require.NoError(t, err)

	assert.NoError(t, plogtest.CompareLogs(expected, actual[0], plogtest.IgnoreObservedTimestamp()))
}

func waitLogsSink(t *testing.T, sink *consumertest.LogsSink) {
	timeout := time.After(10 * time.Second)
	retry := time.NewTicker(10 * time.Millisecond)
	defer retry.Stop()
	for {
		select {
		case <-timeout:
			t.Fatal("timeout while waiting for logs")
		case <-retry.C:
		}
		if sink.LogRecordCount() > 0 {
			return
		}
	}
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
	case kind == component.KindReceiver && ctype.String() == "filelog":
		return filelogreceiver.NewFactory()
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
