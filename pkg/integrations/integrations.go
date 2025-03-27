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

package integrations // import "github.com/elastic/opentelemetry-collector-components/pkg/integrations"

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pipeline"
)

// ErrNotFound is the error returned when an integration cannot be found.
var (
	ErrNotFound      = errors.New("not found")
	ErrNotConfigured = fmt.Errorf("%w: no integration finder extension found", ErrNotFound)
)

// Integration is the interface for integrations that can be resolved as configuration.
type Integration interface {
	// Resolve resolves the parametrizable integration. It uses the params to replace placeholders
	// in the integration, and removes or ignores everything not referenced by the indicated
	// pipelines.
	Resolve(ctx context.Context, params map[string]any, pipelines []pipeline.ID) (*Config, error)
}

// Finder is the interface for extensions that can be used to find integrations by their names.
type Finder interface {
	FindIntegration(ctx context.Context, name string) (Integration, error)
}

// ExtensionGetter is the interface a host must implement to be used to locate finders
// along extensions.
// It must be a subset of the component.Host interface.
type ExtensionGetter interface {
	// GetExtensions returns the map of extensions. Only enabled and created extensions will be returned.
	// GetExtensions can be called by the component anytime after Component.Start() begins and
	// until Component.Shutdown() ends.
	GetExtensions() map[component.ID]component.Component
}

// Compile-time check to ensure that ExtensionGetter is a subset of the component.Host interface.
var _ ExtensionGetter = (component.Host)(nil)

// Find looks for integrations in extensions of the host that implement the IntegrationFinder interface.
// If multiple integrations match, only the first one is returned.
// It returns ErrNotFound if the integration cannot be found, or ErrNotConfigured if there are no
// configured finder extensions. ErrNotConfigured also wraps ErrNotFound.
func Find(ctx context.Context, logger *zap.Logger, host ExtensionGetter, integrationName string) (Integration, error) {
	if host == nil {
		logger.Error("received nil host")
		return nil, ErrNotConfigured
	}
	anyExtension := false
	for eid, extension := range host.GetExtensions() {
		finder, ok := extension.(Finder)
		if !ok {
			continue
		}
		anyExtension = true

		integration, err := finder.FindIntegration(ctx, integrationName)
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
		return nil, ErrNotConfigured
	}

	return nil, ErrNotFound
}

// Config contains an structured integration. At this point we only handle component IDs, and not
// their specific configurations.
type Config struct {
	Receivers  map[component.ID]component.Config `mapstructure:"receivers"`
	Processors map[component.ID]component.Config `mapstructure:"processors"`
	Pipelines  map[pipeline.ID]PipelineConfig    `mapstructure:"pipelines"`
}

// validate validates that the integration configuration is valid and all the components referenced in the
// pipelines are defined.
func (c *Config) validate() error {
	if len(c.Pipelines) == 0 {
		return errors.New("missing pipelines")
	}
	for _, pipeline := range c.Pipelines {
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

// PipelineConfig contains the definition of a pipeline in the integration.
type PipelineConfig struct {
	// Receiver is the receiver to be used in the pipeline. It is optional, if a pipeline does not define a
	// receiver it cannot be used to instantiate receivers.
	Receiver *component.ID `mapstructure:"receiver"`

	// Processors is the chain of processors of the pipeline, to be used as part of the receiver in receiver
	// components, or as a combined processor when the pipeline is used as processor.
	Processors []component.ID `mapstructure:"processors"`
}
