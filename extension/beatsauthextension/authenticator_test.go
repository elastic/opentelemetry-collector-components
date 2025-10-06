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
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configauth"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
)

func TestAuthenticator(t *testing.T) {
	// Pre-generate CA and certificates for the TLS test case
	caCert, err := tlscommontest.GenCA()
	require.NoError(t, err)

	serverCerts, err := tlscommontest.GenSignedCert(caCert, x509.KeyUsageCertSign, false, "", []string{}, []net.IP{net.IPv4(127, 0, 0, 1)}, false)
	require.NoError(t, err)

	testCases := []struct {
		name                     string
		setupConfig              func(t *testing.T) *Config
		continueOnError          bool
		skipStart                bool
		expectStartError         bool
		expectStatus             componentstatus.Status
		expectHTTPClientType     string // "httpClientProvider" or "errorRoundTripperProvider"
		testHTTPRequest          bool
		testRoundTripError       bool
		testRoundTripperPreStart bool
		tlsCerts                 *tls.Certificate // for test server
	}{
		{
			name: "successful authentication with valid TLS config",
			setupConfig: func(t *testing.T) *Config {
				return &Config{
					BeatAuthConfig: map[string]any{
						"proxy_disable":           true,
						"timeout":                 "60s",
						"idle_connection_timeout": "3s",
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
			},
			expectStartError:     false,
			expectStatus:         componentstatus.StatusOK,
			expectHTTPClientType: "httpClientProvider",
			testHTTPRequest:      true,
			tlsCerts:             &serverCerts,
		},
		{
			name: "invalid TLS certificate - continueOnError false",
			setupConfig: func(t *testing.T) *Config {
				return &Config{
					BeatAuthConfig: map[string]any{
						"ssl": map[string]any{
							"enabled":     "true",
							"certificate": "/nonexistent/cert.pem",
							"key":         "/nonexistent/key.pem",
						},
					},
					ContinueOnError: false,
				}
			},
			expectStartError: true,
			expectStatus:     componentstatus.StatusPermanentError,
		},
		{
			name: "invalid TLS certificate - continueOnError true",
			setupConfig: func(t *testing.T) *Config {
				return &Config{
					BeatAuthConfig: map[string]any{
						"ssl": map[string]any{
							"enabled":     "true",
							"certificate": "/nonexistent/cert.pem",
							"key":         "/nonexistent/key.pem",
						},
					},
					ContinueOnError: true,
				}
			},
			expectStartError:     false,
			expectStatus:         componentstatus.StatusPermanentError,
			expectHTTPClientType: "errorRoundTripperProvider",
			testRoundTripError:   true,
		},
		{
			name: "successful client creation with minimal config",
			setupConfig: func(t *testing.T) *Config {
				return &Config{
					BeatAuthConfig: map[string]any{},
				}
			},
			expectStartError:     false,
			expectStatus:         componentstatus.StatusOK,
			expectHTTPClientType: "httpClientProvider",
		},
		{
			name: "RoundTripper called before Start",
			setupConfig: func(t *testing.T) *Config {
				return &Config{
					BeatAuthConfig: map[string]any{},
				}
			},
			skipStart:                true,
			testRoundTripperPreStart: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			settings := componenttest.NewNopTelemetrySettings()
			cfg := tc.setupConfig(t)

			auth, err := newAuthenticator(cfg, settings)
			require.NoError(t, err)

			if tc.testRoundTripperPreStart {
				rt, err := auth.RoundTripper(nil)
				require.Error(t, err)
				require.Nil(t, rt)
				require.Contains(t, err.Error(), "authenticator not started")
				return
			}

			if tc.skipStart {
				return
			}

			var reportedStatuses []componentstatus.Status
			host := &mockHost{
				extensions: extensionsMap{component.NewID(metadata.Type): auth},
				reportStatusFunc: func(ev *componentstatus.Event) {
					reportedStatuses = append(reportedStatuses, ev.Status())
				},
			}

			err = auth.Start(context.Background(), host)
			if tc.expectStartError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Validate component status
			require.Len(t, reportedStatuses, 1)
			require.Equal(t, tc.expectStatus, reportedStatuses[0])

			// Validate provider type if specified
			if tc.expectHTTPClientType != "" {
				require.NotNil(t, auth.rtProvider)

				switch tc.expectHTTPClientType {
				case "httpClientProvider":
					_, ok := (auth.rtProvider).(*httpClientProvider)
					require.True(t, ok, "Provider should be an httpClientProvider")
				case "errorRoundTripperProvider":
					_, ok := (auth.rtProvider).(*errorRoundTripperProvider)
					require.True(t, ok, "Provider should be an errorRoundTripperProvider")
				}

				rt, err := auth.RoundTripper(nil)
				require.NoError(t, err)
				require.NotNil(t, rt)

				if tc.expectHTTPClientType == "errorRoundTripperProvider" {
					_, ok := rt.(*errorRoundTripper)
					require.True(t, ok, "RoundTripper should be an errorRoundTripper")
				}
			}

			// Test RoundTrip error if specified
			if tc.testRoundTripError {
				rt, err := auth.RoundTripper(nil)
				require.NoError(t, err)

				req, err := http.NewRequest("GET", "http://example.com", nil)
				require.NoError(t, err)
				resp, err := rt.RoundTrip(req)
				require.Error(t, err)
				require.Nil(t, resp)
				require.Contains(t, err.Error(), "failed")
			}

			// Test HTTP request if specified
			if tc.testHTTPRequest {
				require.NotNil(t, tc.tlsCerts, "tlsCerts must be provided for testHTTPRequest")

				serverURL := startTestServer(t, []tls.Certificate{*tc.tlsCerts})

				httpClientConfig := confighttp.NewDefaultClientConfig()
				httpClientConfig.Auth = configoptional.Some(configauth.Config{
					AuthenticatorID: component.NewID(metadata.Type),
				})

				client, err := httpClientConfig.ToClient(context.Background(), host, settings)
				require.NoError(t, err)
				require.NotNil(t, client)

				resp, err := client.Get(serverURL)
				require.NoError(t, err)
				_ = resp.Body.Close()
			}
		})
	}
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

type mockHost struct {
	extensions       extensionsMap
	reportStatusFunc func(*componentstatus.Event)
}

func (m *mockHost) GetExtensions() map[component.ID]component.Component {
	return m.extensions
}

func (m *mockHost) Report(event *componentstatus.Event) {
	if m.reportStatusFunc != nil {
		m.reportStatusFunc(event)
	}
}
