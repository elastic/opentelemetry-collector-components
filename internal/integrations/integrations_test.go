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
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap"
)

//go:embed testdata/template-simple.yaml
var rawTemplate1 []byte

//go:embed testdata/template-simple-2.yaml
var rawTemplate2 []byte

func TestFind(t *testing.T) {
	integration1 := mustNewIntegration(rawTemplate1)
	integration2 := mustNewIntegration(rawTemplate2)

	cases := []struct {
		title            string
		skip             string
		name             string
		expectedErr      bool
		expectedPipeline pipeline.ID
		params           map[string]any
		host             Host
	}{
		{
			title:       "nil host",
			expectedErr: true,
		},
		{
			title:       "host without finders",
			name:        "something",
			expectedErr: true,
			host:        newDummyHost(nil),
		},
		{
			title: "find integration",
			name:  "simple",
			host: newDummyHost(map[component.ID]component.Component{
				component.MustNewID("finder"): newDummyFinder(map[string]Integration{
					"simple": integration1,
				}),
			}),
		},
		{
			title:            "find integration multiple finders",
			name:             "simple",
			expectedPipeline: pipeline.NewIDWithName(pipeline.SignalLogs, "raw"),
			params: map[string]any{
				"value":      "foo",
				"othervalue": "foo",
			},
			host: newDummyHost(map[component.ID]component.Component{
				component.MustNewID("finder"): newDummyFinder(map[string]Integration{
					"simple": integration1,
				}),
				component.MustNewID("finder2"): newDummyFinder(map[string]Integration{
					"simple2": integration2,
				}),
			}),
		},
		{
			title:            "find integration available in multiple finders",
			skip:             "TODO: current host interface does not guarantee order of extensions",
			name:             "simple",
			expectedPipeline: pipeline.NewIDWithName(pipeline.SignalLogs, "raw"),
			params: map[string]any{
				"value":      "foo",
				"othervalue": "foo",
			},
			host: newDummyHost(map[component.ID]component.Component{
				component.MustNewID("finder1"): newDummyFinder(map[string]Integration{
					"simple": integration1,
				}),
				component.MustNewID("finder2"): newDummyFinder(map[string]Integration{
					"simple": integration2,
				}),
			}),
		},
	}

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			if len(c.skip) > 0 {
				t.Skip(c.skip)
			}
			integration, err := Find(context.Background(), logger, c.host, c.name)
			if c.expectedErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, integration)

			if c.expectedPipeline.String() != "" {
				c, err := integration.Resolve(context.Background(), c.params, []pipeline.ID{c.expectedPipeline})
				require.NoError(t, err)
				require.NotNil(t, c)
			}
		})
	}
}

type dummyHost struct {
	extensions map[component.ID]component.Component
}

func newDummyHost(extensions map[component.ID]component.Component) *dummyHost {
	return &dummyHost{extensions: extensions}
}

func (h *dummyHost) GetExtensions() map[component.ID]component.Component {
	return h.extensions
}

type dummyFinder struct {
	dummyComponent

	integrations map[string]Integration
}

func newDummyFinder(integrations map[string]Integration) *dummyFinder {
	return &dummyFinder{integrations: integrations}
}

func (f *dummyFinder) FindIntegration(ctx context.Context, name string) (Integration, error) {
	i, found := f.integrations[name]
	if !found {
		return nil, ErrNotFound
	}
	return i, nil
}

type dummyComponent struct{}

func (*dummyComponent) Start(ctx context.Context, host component.Host) error { return nil }

func (*dummyComponent) Shutdown(ctx context.Context) error { return nil }

func mustNewIntegration(raw []byte) Integration {
	i, err := NewRawTemplate(raw)
	if err != nil {
		panic(err)
	}
	return i
}
