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

package configintegrationextension // import "github.com/elastic/opentelemetry-collector-components/extension/configintegrationextension"

import (
	"context"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"

	"github.com/elastic/opentelemetry-collector-components/internal/integrations"
)

type configTemplateExtension struct {
	config *Config
}

var _ integrations.TemplateFinder = &configTemplateExtension{}

func newConfigTemplateExtension(config *Config) *configTemplateExtension {
	return &configTemplateExtension{
		config: config,
	}
}

func (e *configTemplateExtension) FindTemplate(ctx context.Context, name, version string) (integrations.Template, error) {
	integration, found := e.config.Integrations[name]
	if !found {
		return nil, integrations.ErrNotFound
	}

	return &integrationConfig{
		name:        name,
		integration: strings.ReplaceAll(integration, `$$`, `$`),
	}, nil
}

func (*configTemplateExtension) Start(context.Context, component.Host) error {
	return nil
}

func (*configTemplateExtension) Shutdown(context.Context) error {
	return nil
}

type integrationConfig struct {
	name        string
	integration string
}

func (t *integrationConfig) URI() string {
	return t.Scheme() + ":" + t.name
}

func (t *integrationConfig) ProviderFactory() confmap.ProviderFactory {
	return t
}

func (t *integrationConfig) Create(_ confmap.ProviderSettings) confmap.Provider {
	return t
}

func (t *integrationConfig) Retrieve(ctx context.Context, _ string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
	return confmap.NewRetrievedFromYAML([]byte(t.integration))
}

func (t *integrationConfig) Scheme() string {
	return "config"
}

func (t *integrationConfig) Shutdown(ctx context.Context) error {
	return nil
}
