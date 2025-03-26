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

	"github.com/elastic/opentelemetry-collector-components/pkg/integrations"
)

type configTemplateExtension struct {
	config *Config
}

var (
	_ component.Component = &configTemplateExtension{}
	_ integrations.Finder = &configTemplateExtension{}
)

func newConfigTemplateExtension(config *Config) *configTemplateExtension {
	return &configTemplateExtension{
		config: config,
	}
}

// FindIntegration looks for integrations along the ones embedded in the configuration.
func (e *configTemplateExtension) FindIntegration(ctx context.Context, name string) (integrations.Integration, error) {
	integration, found := e.config.Integrations[name]
	if !found {
		return nil, integrations.ErrNotFound
	}

	raw := []byte(strings.ReplaceAll(integration, `$$`, `$`))
	return integrations.NewRawTemplate(raw)
}

func (*configTemplateExtension) Start(context.Context, component.Host) error {
	return nil
}

func (*configTemplateExtension) Shutdown(context.Context) error {
	return nil
}
