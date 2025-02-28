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
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

// RawTemplate implements the Template interface for raw YAML content.
// Unused components are removed after resolving it, so the variables they contain are not required.
type RawTemplate struct {
	source rawYAMLConfig
}

// NewRawTemplate creates a new RawTemplate from raw YAML content.
func NewRawTemplate(raw []byte) (*RawTemplate, error) {
	var source rawYAMLConfig
	err := yaml.Unmarshal(raw, &source)
	if err != nil {
		return nil, fmt.Errorf("invalid integration template format: %w", err)
	}
	if err := source.Validate(); err != nil {
		return nil, fmt.Errorf("integration template validation failed: %w", err)
	}
	return &RawTemplate{source: source}, nil
}

// Resolve resolves the template using a confmap resolver.
func (t *RawTemplate) Resolve(ctx context.Context, params map[string]any, pipelines []component.ID) (*Config, error) {
	selectedPipelines := t.source.Pipelines
	if len(pipelines) > 0 {
		var err error
		selectedPipelines, err = selectComponents(t.source.Pipelines, pipelines)
		if err != nil {
			return nil, fmt.Errorf("selecting pipelines: %w", err)
		}
	}
	selectedReceivers, err := selectComponents(t.source.Receivers, listReceivers(selectedPipelines))
	if err != nil {
		return nil, fmt.Errorf("selecting receivers: %w", err)
	}
	selectedProcessors, err := selectComponents(t.source.Processors, listProcessors(selectedPipelines))
	if err != nil {
		return nil, fmt.Errorf("selecting processors: %w", err)
	}

	factory := newConfmapProviderFactory(rawYAMLConfig{
		Pipelines:  selectedPipelines,
		Receivers:  selectedReceivers,
		Processors: selectedProcessors,
	})
	resolver, err := newResolver(factory, params)
	if err != nil {
		return nil, err
	}
	conf, err := resolver.Resolve(ctx)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := conf.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to generate effective configuration: %w", err)
	}
	return &config, config.Validate()
}

func newResolver(factory confmap.ProviderFactory, variables map[string]any) (*confmap.Resolver, error) {
	settings := confmap.ResolverSettings{
		URIs: []string{"config:main"},
		ProviderFactories: []confmap.ProviderFactory{
			factory,
			newVariablesProviderFactory(variables),
		},
	}
	return confmap.NewResolver(settings)
}

func selectComponents[C any](from map[component.ID]C, selection []component.ID) (map[component.ID]C, error) {
	if len(selection) == 0 {
		return nil, nil
	}
	selected := make(map[component.ID]C)
	for _, id := range selection {
		component, found := from[id]
		if !found {
			return nil, fmt.Errorf("component %q not found", id.String())
		}
		selected[id] = component
	}
	return selected, nil
}

func listReceivers(pipelines map[component.ID]PipelineConfig) []component.ID {
	var list []component.ID
	for _, pipeline := range pipelines {
		list = append(list, pipeline.Receiver)
	}
	slices.SortFunc(list, func(a, b component.ID) int { return strings.Compare(a.String(), b.String()) })
	return slices.Compact(list)
}

func listProcessors(pipelines map[component.ID]PipelineConfig) []component.ID {
	var list []component.ID
	for _, pipeline := range pipelines {
		list = append(list, pipeline.Processors...)
	}
	slices.SortFunc(list, func(a, b component.ID) int { return strings.Compare(a.String(), b.String()) })
	return slices.Compact(list)
}

type rawYAMLConfig struct {
	Receivers  map[component.ID]yaml.Node      `mapstructure:"receivers"`
	Processors map[component.ID]yaml.Node      `mapstructure:"processors"`
	Pipelines  map[component.ID]PipelineConfig `mapstructure:"pipelines"`
}

func (c *rawYAMLConfig) Validate() error {
	for id, pipeline := range c.Pipelines {
		if err := pipeline.Validate(); err != nil {
			return fmt.Errorf("invalid pipeline %q: %w", id, err)
		}

		if _, found := c.Receivers[pipeline.Receiver]; !found {
			return fmt.Errorf("receiver %q not defined", pipeline.Receiver.String())
		}

		for _, processor := range pipeline.Processors {
			if _, found := c.Processors[processor]; !found {
				return fmt.Errorf("processor %q not defined", processor.String())
			}

		}
	}

	return nil
}

const configProviderScheme = "config"

func newConfmapProviderFactory(main rawYAMLConfig) confmap.ProviderFactory {
	return confmap.NewProviderFactory(createRawConfigProvider(main))
}

func createRawConfigProvider(main rawYAMLConfig) confmap.CreateProviderFunc {
	return func(_ confmap.ProviderSettings) confmap.Provider {
		return &rawConfigProvider{main: main}
	}
}

type rawConfigProvider struct {
	main rawYAMLConfig
}

func (p *rawConfigProvider) Retrieve(ctx context.Context, uri string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
	key := strings.TrimPrefix(uri, configProviderScheme+":")

	switch key {
	case "main":
		return confmap.NewRetrievedFromYAML(p.buildMainYML())
	case "receivers":
		d, err := yaml.Marshal(p.main.Receivers)
		if err != nil {
			return nil, err
		}
		return confmap.NewRetrievedFromYAML(d)
	case "processors":
		d, err := yaml.Marshal(p.main.Processors)
		if err != nil {
			return nil, err
		}
		return confmap.NewRetrievedFromYAML(d)
	case "pipelines":
		d, err := yaml.Marshal(p.main.Pipelines)
		if err != nil {
			return nil, err
		}
		return confmap.NewRetrievedFromYAML(d)
	}

	return nil, fmt.Errorf("%s not found", uri)
}

func (p *rawConfigProvider) buildMainYML() []byte {
	var buf bytes.Buffer
	if len(p.main.Receivers) > 0 {
		fmt.Fprintf(&buf, "receivers: ${%s:receivers}\n", configProviderScheme)
	}
	if len(p.main.Processors) > 0 {
		fmt.Fprintf(&buf, "processors: ${%s:processors}\n", configProviderScheme)
	}
	if len(p.main.Pipelines) > 0 {
		fmt.Fprintf(&buf, "pipelines: ${%s:pipelines}\n", configProviderScheme)
	}
	return buf.Bytes()
}

func (p *rawConfigProvider) Scheme() string {
	return configProviderScheme
}

func (p *rawConfigProvider) Shutdown(ctx context.Context) error {
	return nil
}
