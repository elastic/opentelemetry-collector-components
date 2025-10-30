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

package elasticpipelineextension

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/extension/extensiontest"

	"github.com/elastic/opentelemetry-collector-components/extension/elasticpipelineextension/internal/metadata"
)

func TestFactoryCreation(t *testing.T) {
	factory := NewFactory()
	require.NotNil(t, factory)
	require.Equal(t, metadata.Type, factory.Type())
}

func TestDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NotNil(t, cfg)

	// Verify the default configuration is valid
	require.NoError(t, cfg.(*Config).Validate())
}

func TestCreateExtension(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	// Create the extension using the correct method signature
	ext, err := createExtension(
		context.Background(),
		extensiontest.NewNopSettings(metadata.Type),
		cfg,
	)

	require.NoError(t, err)
	require.NotNil(t, ext)
}
