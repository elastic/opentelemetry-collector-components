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
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/elastic/elastic-agent-libs/transport/tlscommontest"
	"github.com/elastic/opentelemetry-collector-components/extension/beatsauthextension/internal/metadata"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configauth"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
)

func TestAuthenticator(t *testing.T) {

	settings := componenttest.NewNopTelemetrySettings()
	httpClientConfig := confighttp.NewDefaultClientConfig()
	httpClientConfig.Auth = configoptional.Some(configauth.Config{
		AuthenticatorID: component.NewID(metadata.Type),
	})

	caCert, err := tlscommontest.GenCA()
	if err != nil {
		t.Fatalf("could not generate root CA certificate: %s", err)
	}

	// create certificates for server setup
	certs, err := tlscommontest.GenSignedCert(caCert, x509.KeyUsageCertSign, false, "", []string{}, []net.IP{net.IPv4(127, 0, 0, 1)}, false)
	if err != nil {
		t.Fatalf("could not generate certificates: %s", err)
	}

	// start server with generated cer
	serverURL := startTestServer(t, []tls.Certificate{certs})

	cfg := &Config{
		BeatAuthconfig: map[string]any{
			"ssl": map[string]any{
				"enabled":           "true",
				"verification_mode": "full",
				"certificate_authorities": []string{
					string(
						pem.EncodeToMemory(&pem.Block{
							Type:  "CERTIFICATE",
							Bytes: caCert.Leaf.Raw,
						})),
				},
			},
		},
	}

	auth, err := newAuthenticator(cfg, settings)
	require.NoError(t, err)

	host := extensionsMap{component.NewID(metadata.Type): auth}
	err = auth.Start(context.Background(), host)
	require.NoError(t, err)

	httpClientConfig.TLS.ServerName = "localhost"
	client, err := httpClientConfig.ToClient(context.Background(), host, settings)
	require.NoError(t, err)
	require.NotNil(t, client)

	resp, err := client.Get(serverURL)
	require.NoError(t, err)
	resp.Body.Close()
}

// startTestServer starts a HTTP server for testing using the provided
// certificates
//
// All requests are responded with an HTTP 200 OK and a plain
// text string
//
// The HTTP server will shutdown at the end of the test.
func startTestServer(t *testing.T, serverCerts []tls.Certificate) string {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write([]byte("SSL test server")); err != nil {
			t.Errorf("coluld not write to client: %s", err)
		}
	}))
	server.TLS = &tls.Config{}
	server.TLS.Certificates = serverCerts
	server.StartTLS()
	t.Cleanup(func() { server.Close() })
	return server.URL
}

type extensionsMap map[component.ID]component.Component

func (m extensionsMap) GetExtensions() map[component.ID]component.Component {
	return m
}
