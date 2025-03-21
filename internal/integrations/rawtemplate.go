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
	"errors"
	"fmt"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/pipeline"
)

var _ Integration = &RawTemplate{}

// RawTemplate implements the Template interface for raw YAML content.
// Unused components are removed after resolving it, so the variables they contain are not required.
type RawTemplate struct {
	source rawYAMLConfig
}

// NewRawTemplate creates a new RawTemplate from raw YAML content.
func NewRawTemplate(raw []byte) (*RawTemplate, error) {
	var source rawYAMLConfig
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	err := decoder.Decode(&source)
	if err != nil {
		return nil, fmt.Errorf("invalid integration template format: %w", err)
	}
	if err := source.validate(); err != nil {
		return nil, fmt.Errorf("integration template validation failed: %w", err)
	}
	return &RawTemplate{source: source}, nil
}

// ValidateRawTemplate checks if an integration template is syntactically valid.
func ValidateRawTemplate(raw []byte) error {
	_, err := NewRawTemplate(raw)
	return err
}

// Resolve resolves the template using a confmap resolver.
func (t *RawTemplate) Resolve(ctx context.Context, params map[string]any, pipelines []pipeline.ID) (*Config, error) {
	selectedPipelines := t.source.Pipelines
	if len(pipelines) > 0 {
		var err error
		selectedPipelines, err = selectComponents(t.source.Pipelines, pipelines)
		if err != nil {
			return nil, fmt.Errorf("selecting pipelines: %v", err)
		}
	}
	selectedReceivers, err := selectComponents(t.source.Receivers, listReceivers(selectedPipelines))
	if err != nil {
		return nil, fmt.Errorf("selecting receivers: %v", err)
	}
	selectedProcessors, err := selectComponents(t.source.Processors, listProcessors(selectedPipelines))
	if err != nil {
		return nil, fmt.Errorf("selecting processors: %v", err)
	}

	rawConfig := rawYAMLConfig{
		Pipelines:  selectedPipelines,
		Receivers:  selectedReceivers,
		Processors: selectedProcessors,
	}
	resolver, err := newResolver(rawConfig, params)
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
	return &config, config.validate()
}

func newResolver(rawConfig rawYAMLConfig, variables map[string]any) (*confmap.Resolver, error) {
	settings := confmap.ResolverSettings{
		URIs: []string{"config:main"},
		ProviderFactories: []confmap.ProviderFactory{
			newConfmapProviderFactory(rawConfig),
			newVariablesProviderFactory(variables),
		},
	}
	return confmap.NewResolver(settings)
}

type selectableID interface {
	comparable
	fmt.Stringer
}

func selectComponents[C any, ID selectableID](from map[ID]C, selection []ID) (map[ID]C, error) {
	selected := make(map[ID]C, len(selection))
	for _, id := range selection {
		component, found := from[id]
		if !found {
			return nil, fmt.Errorf("component %q not found", id)
		}
		selected[id] = component
	}
	return selected, nil
}

func listComponents(pipelines map[pipeline.ID]PipelineConfig, listIDs func(PipelineConfig) []component.ID) []component.ID {
	var list []component.ID
	for _, pipeline := range pipelines {
		list = append(list, listIDs(pipeline)...)
	}
	slices.SortFunc(list, func(a, b component.ID) int { return strings.Compare(a.String(), b.String()) })
	return slices.Compact(list)
}

// listReceivers lists the IDs of all the receivers found in pipelines
func listReceivers(pipelines map[pipeline.ID]PipelineConfig) []component.ID {
	return listComponents(pipelines, func(c PipelineConfig) []component.ID {
		return []component.ID{*c.Receiver}
	})
}

// listReceivers lists the IDs of all the processors found in pipelines
func listProcessors(pipelines map[pipeline.ID]PipelineConfig) []component.ID {
	return listComponents(pipelines, func(c PipelineConfig) []component.ID {
		return c.Processors
	})
}

type rawYAMLConfig struct {
	Receivers  map[component.ID]yaml.Node     `mapstructure:"receivers" yaml:"receivers,omitempty"`
	Processors map[component.ID]yaml.Node     `mapstructure:"processors" yaml:"processors,omitempty"`
	Pipelines  map[pipeline.ID]PipelineConfig `mapstructure:"pipelines" yaml:"pipelines,omitempty"`
}

func (c *rawYAMLConfig) validate() error {
	if len(c.Pipelines) == 0 {
		return errors.New("missing pipelines")
	}
	for _, pipeline := range c.Pipelines {
		if pipeline.Receiver != nil {
			if _, found := c.Receivers[*pipeline.Receiver]; !found {
				return fmt.Errorf("receiver %q not defined", pipeline.Receiver.String())
			}
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
	if uri != configProviderScheme+":main" {
		return nil, fmt.Errorf("unexpected use of %s provider, requested uri: %s", configProviderScheme, uri)
	}

	main, err := yaml.Marshal(p.main)
	if err != nil {
		return nil, fmt.Errorf("failed to encode config: %w", err)
	}
	return confmap.NewRetrievedFromYAML(main)
}

func (p *rawConfigProvider) Scheme() string {
	return configProviderScheme
}

func (p *rawConfigProvider) Shutdown(ctx context.Context) error {
	return nil
}
