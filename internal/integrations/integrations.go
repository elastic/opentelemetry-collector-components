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
	"errors"
	"fmt"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
)

var ErrNotFound = errors.New("not found")

type Template interface {
	Resolve(ctx context.Context, params map[string]any, pipelines []component.ID) (*Config, error)
}

type TemplateFinder interface {
	FindTemplate(ctx context.Context, name string) (Template, error)
}

func Find(ctx context.Context, logger *zap.Logger, host component.Host, name string) (Template, error) {
	anyExtension := false
	for eid, extension := range host.GetExtensions() {
		finder, ok := extension.(TemplateFinder)
		if !ok {
			continue
		}
		anyExtension = true

		integration, err := finder.FindTemplate(ctx, name)
		if errors.Is(ErrNotFound, err) {
			continue
		}
		if err != nil {
			logger.Error("integration finder failed",
				zap.String("component", eid.String()),
				zap.Error(err))
			return nil, err
		}

		return integration, nil
	}
	if !anyExtension {
		return nil, errors.New("no integration finder extension found")
	}

	return nil, ErrNotFound
}

type Config struct {
	Receivers  map[component.ID]map[string]any `mapstructure:"receivers"`
	Processors map[component.ID]map[string]any `mapstructure:"processors"`
	Pipelines  map[component.ID]PipelineConfig `mapstructure:"pipelines"`
}

func (c *Config) Validate() error {
	for id, pipeline := range c.Pipelines {
		if err := pipeline.Validate(); err != nil {
			return fmt.Errorf("invalid pipeline %q: %w", id, err)
		}

		if pipeline.Receiver != nil {
			if _, found := c.Receivers[*pipeline.Receiver]; !found {
				return fmt.Errorf("receiver %s not defined", pipeline.Receiver.String())
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

type PipelineConfig struct {
	Receiver   *component.ID  `mapstructure:"receiver"`
	Processors []component.ID `mapstructure:"processors"`
}

func (p *PipelineConfig) Validate() error {
	return nil
}
