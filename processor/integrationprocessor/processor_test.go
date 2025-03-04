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

	"github.com/stretchr/testify/require"
	"github.com/tj/assert"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/elastic/opentelemetry-collector-components/internal/integrations"
	"github.com/elastic/opentelemetry-collector-components/processor/integrationprocessor/internal/metadata"
)

func TestEmptyProcessLogs(t *testing.T) {
	factory := NewFactory()
	config := factory.CreateDefaultConfig().(*Config)
	config.Name = "empty"
	config.Pipeline = component.MustNewID("logs")

	sink := new(consumertest.LogsSink)
	p, err := factory.CreateLogs(context.Background(), processortest.NewNopSettingsWithType(metadata.Type), config, sink)
	require.NoError(t, err)

	c := p.(component.Component)
	err = c.Start(context.Background(), newMockHost("testdata/templates"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Shutdown(context.Background())) })

	input, err := golden.ReadLogs(filepath.Join("testdata", "logs.yaml"))
	require.NoError(t, err)

	assert.NoError(t, p.ConsumeLogs(context.Background(), input))

	actual := sink.AllLogs()
	require.Len(t, actual, 1)

	expectedFile := filepath.Join("testdata", "expected-logs-no-processing.yaml")
	//golden.WriteLogs(t, expectedFile, actual[0])
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

func (m *mockedHost) GetFactory(component.Kind, component.Type) component.Factory {
	return nil
}

func (m *mockedHost) Start(context.Context, component.Host) error {
	return nil
}

func (m *mockedHost) Shutdown(context.Context) error {
	return nil
}

func (m *mockedHost) FindTemplate(ctx context.Context, name string) (integrations.Template, error) {
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
