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

package httpexporter

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/elastic/opentelemetry-collector-components/internal/exporter/httpexporter/internal/metadata"
)

func TestPushLogsPostsNDJSONBodies(t *testing.T) {
	var gotMethod, gotContentType, gotAuth, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotContentType = r.Header.Get("Content-Type")
		gotAuth = r.Header.Get("Authorization")
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		gotBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = srv.URL + "/inputs/vercel/_default_"
	cfg.Headers.Set("Authorization", configopaque.String("ApiKey test-key"))

	set := exportertest.NewNopSettings(metadata.Type)
	exp, err := newExporter(cfg, set)
	require.NoError(t, err)
	require.NoError(t, exp.start(t.Context(), componenttest.NewNopHost()))

	logs := plog.NewLogs()
	records := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	records.AppendEmpty().Body().SetStr(`{"id":"1","projectId":"p1"}`)
	records.AppendEmpty().Body().SetStr(`{"id":"2","projectId":"p1"}`)

	require.NoError(t, exp.pushLogs(t.Context(), logs))

	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "application/json", gotContentType)
	assert.Equal(t, "ApiKey test-key", gotAuth)
	assert.Equal(t, "{\"id\":\"1\",\"projectId\":\"p1\"}\n{\"id\":\"2\",\"projectId\":\"p1\"}", gotBody)
}

func TestPushLogsStatusCodeErrors(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		permanent bool
	}{
		{name: "unauthorized is permanent", status: http.StatusUnauthorized, permanent: true},
		{name: "forbidden is permanent", status: http.StatusForbidden, permanent: true},
		{name: "bad request is permanent", status: http.StatusBadRequest, permanent: true},
		{name: "too many requests is retryable", status: http.StatusTooManyRequests, permanent: false},
		{name: "server error is retryable", status: http.StatusInternalServerError, permanent: false},
		{name: "bad gateway is retryable", status: http.StatusBadGateway, permanent: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
			}))
			t.Cleanup(srv.Close)

			cfg := createDefaultConfig().(*Config)
			cfg.Endpoint = srv.URL

			set := exportertest.NewNopSettings(metadata.Type)
			exp, err := newExporter(cfg, set)
			require.NoError(t, err)
			require.NoError(t, exp.start(t.Context(), componenttest.NewNopHost()))

			logs := plog.NewLogs()
			logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(`{}`)

			err = exp.pushLogs(t.Context(), logs)
			require.Error(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("%d", tt.status))
			assert.Equal(t, tt.permanent, consumererror.IsPermanent(err))
		})
	}
}

func TestConfigValidate(t *testing.T) {
	cfg := &Config{ClientConfig: confighttp.NewDefaultClientConfig()}
	assert.Error(t, cfg.Validate())

	cfg.Endpoint = "ftp://example.com"
	assert.Error(t, cfg.Validate())

	cfg.Endpoint = "https://example.com/inputs/vercel/_default_"
	assert.NoError(t, cfg.Validate())
}

func TestFactoryCreateLogs(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Endpoint = "https://example.com/inputs/vercel/_default_"

	exp, err := factory.CreateLogs(t.Context(), exportertest.NewNopSettings(factory.Type()), cfg)
	require.NoError(t, err)
	require.NotNil(t, exp)
	require.NoError(t, exp.Shutdown(t.Context()))
}
