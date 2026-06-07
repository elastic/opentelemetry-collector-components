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

package kubetunnelprobeextension

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/extension/extensiontest"

	"github.com/elastic/opentelemetry-collector-components/extension/kubetunnelprobeextension/internal/metadata"
	kubetunnel "github.com/elastic/opentelemetry-collector-components/internal/kubetunnel"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	// Endpoint is required, so the default config is intentionally invalid.
	assert.Error(t, cfg.Validate())

	cfg.Relay.Endpoint = "relay:4317"
	assert.NoError(t, cfg.Validate())
}

func TestCreateExtension(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Relay.Endpoint = "relay:4317"
	ext, err := createExtension(context.Background(), extensiontest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	require.NotNil(t, ext)
}

// errResult is part of the executor's read-only contract: anything outside the
// allowlist must be rejected without touching the API. We can assert the
// dispatch decision without a cluster by checking a disallowed operation.
func TestExecuteRejectsNonReadOperations(t *testing.T) {
	e := &executor{}
	res := e.Execute(context.Background(), kubetunnel.ReadRequest{Operation: "delete", Resource: "pods", Name: "x"})
	assert.Contains(t, res.Error, "not allowed")
}
