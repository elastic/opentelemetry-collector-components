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

package beatsauthextension // import "github.com/elastic/opentelemetry-collector-components/extension/beatsauthextension"

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/extension/beatsauthextension/internal/metadata"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configauth"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
)

func TestAuthenticator(t *testing.T) {
	httpClientConfig := confighttp.NewDefaultClientConfig()
	httpClientConfig.Auth = configoptional.Some(configauth.Config{
		AuthenticatorID: component.NewID(metadata.Type),
	})

	cfg := &Config{}

	settings := componenttest.NewNopTelemetrySettings()
	auth, err := newAuthenticator(cfg, settings)
	require.NoError(t, err)

	host := extensionsMap{component.NewID(metadata.Type): auth}
	err = auth.Start(context.Background(), host)
	require.NoError(t, err)

	client, err := httpClientConfig.ToClient(context.Background(), host, settings)
	require.NoError(t, err)
	require.NotNil(t, client)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	t.Cleanup(srv.Close)

	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	resp.Body.Close()
}

type extensionsMap map[component.ID]component.Component

func (m extensionsMap) GetExtensions() map[component.ID]component.Component {
	return m
}
