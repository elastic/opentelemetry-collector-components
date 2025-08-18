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

package beatsauthextension

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configauth"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"

	"github.com/elastic/elastic-agent-libs/transport/tlscommon"
	"github.com/elastic/elastic-agent-libs/transport/tlscommontest"
	"github.com/elastic/opentelemetry-collector-components/extension/beatsauthextension/internal/metadata"
)

// It tests whether VerifyConnection is set on tls.Config
func TestVerifyConnection(t *testing.T) {
	testCerts := tlscommontest.GenTestCerts(t)
	fingerprint := tlscommontest.GetCertFingerprint(testCerts["ca"])

	settings := componenttest.NewNopTelemetrySettings()
	httpClientConfig := confighttp.NewDefaultClientConfig()
	httpClientConfig.Auth = configoptional.Some(configauth.Config{
		AuthenticatorID: component.NewID(metadata.Type),
	})

	testcases := map[string]struct {
		verificationMode     tlscommon.TLSVerificationMode
		peerCerts            []*x509.Certificate
		serverName           string
		expectedCallback     bool
		expectingError       bool
		CATrustedFingerprint string
		CASHA256             []string
	}{
		"CATrustedFingerprint and verification mode:VerifyFull": {
			verificationMode:     tlscommon.VerifyFull,
			peerCerts:            []*x509.Certificate{testCerts["correct"], testCerts["ca"]},
			serverName:           "localhost",
			expectedCallback:     true,
			CATrustedFingerprint: fingerprint,
		},
		"CATrustedFingerprint and verification mode:VerifyFull and incorrect servername": {
			verificationMode:     tlscommon.VerifyFull,
			peerCerts:            []*x509.Certificate{testCerts["correct"], testCerts["ca"]},
			serverName:           "random",
			expectedCallback:     true,
			expectingError:       true,
			CATrustedFingerprint: fingerprint,
		},
		"CATrustedFingerprint and verification mode:VerifyCertificate": {
			verificationMode:     tlscommon.VerifyCertificate,
			peerCerts:            []*x509.Certificate{testCerts["correct"], testCerts["ca"]},
			serverName:           "random", // does not perform hostname verification
			expectedCallback:     true,
			CATrustedFingerprint: fingerprint,
		},
		"CATrustedFingerprint and verification mode:VerifyStrict": {
			verificationMode:     tlscommon.VerifyStrict,
			peerCerts:            []*x509.Certificate{testCerts["correct"], testCerts["ca"]},
			serverName:           "localhost",
			expectedCallback:     true,
			CATrustedFingerprint: fingerprint,
			CASHA256:             []string{tlscommon.Fingerprint(testCerts["correct"])},
		},
		"CATrustedFingerprint and verification mode:VerifyNone": {
			verificationMode: tlscommon.VerifyNone,
			peerCerts:        []*x509.Certificate{testCerts["correct"], testCerts["ca"]},
			serverName:       "random",
			expectedCallback: false,
		},
		"invalid CATrustedFingerprint and verification mode:VerifyFull returns error": {
			verificationMode:     tlscommon.VerifyFull,
			peerCerts:            []*x509.Certificate{testCerts["correct"], testCerts["ca"]},
			serverName:           "localhost",
			expectedCallback:     true,
			CATrustedFingerprint: "INVALID HEX ENCODING",
			expectingError:       true,
		},
		"invalid CATrustedFingerprint and verification mode:VerifyCertificate returns error": {
			verificationMode:     tlscommon.VerifyCertificate,
			peerCerts:            []*x509.Certificate{testCerts["correct"], testCerts["ca"]},
			serverName:           "localhost",
			expectedCallback:     true,
			CATrustedFingerprint: "INVALID HEX ENCODING",
			expectingError:       true,
		},
		"invalid CATrustedFingerprint and verification mode:VerifyStrict returns error": {
			verificationMode:     tlscommon.VerifyStrict,
			peerCerts:            []*x509.Certificate{testCerts["correct"], testCerts["ca"]},
			serverName:           "localhost",
			expectedCallback:     true,
			CATrustedFingerprint: "INVALID HEX ENCODING",
			expectingError:       true,
			CASHA256:             []string{tlscommon.Fingerprint(testCerts["correct"])},
		},
	}

	for name, test := range testcases {
		t.Run(name, func(t *testing.T) {
			cfg := &Config{
				TLS: &TLSConfig{
					VerificationMode:     test.verificationMode.String(),
					CATrustedFingerprint: test.CATrustedFingerprint,
					CASha256:             test.CASHA256,
				},
			}

			auth, err := newAuthenticator(cfg, settings)
			require.NoError(t, err)

			host := extensionsMap{component.NewID(metadata.Type): auth}
			// starts the auth extension and sets tlscommon.tlsConfig
			err = auth.Start(context.Background(), host)
			require.NoError(t, err)

			// this verifies there was no error in calling the auth.RoundTripper
			client, err := httpClientConfig.ToClient(context.Background(), host, settings)
			require.NoError(t, err)
			require.NotNil(t, client)

			// verifies if a callback was expected
			verifier := auth.tlsConfig.BuildModuleClientConfig(test.serverName).VerifyConnection
			if test.expectedCallback {
				require.NotNil(t, verifier, "VerifyConnection returned a nil verifier")
			} else {
				require.Nil(t, verifier)
				return
			}

			err = verifier(tls.ConnectionState{
				PeerCertificates: test.peerCerts,
				ServerName:       test.serverName,
				VerifiedChains:   [][]*x509.Certificate{test.peerCerts},
			})
			if test.expectingError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// This test suite is from `tlscommon` package in elastic-agent-libs
// https://github.com/elastic/elastic-agent-libs/blob/531c75610fb3fa147cdde354b8ec5433e1b82dc3/transport/tlscommon/tls_config_test.go#L495
// It is modified to work with beatsauthextension
func TestVerificationMode(t *testing.T) {
	settings := componenttest.NewNopTelemetrySettings()
	httpClientConfig := confighttp.NewDefaultClientConfig()
	httpClientConfig.Auth = configoptional.Some(configauth.Config{
		AuthenticatorID: component.NewID(metadata.Type),
	})

	testcases := map[string]struct {
		verificationMode tlscommon.TLSVerificationMode
		expectingError   bool

		// hostname is used to make connection
		hostname string

		// ignoreCerts do not add the Root CA to the trust chain
		ignoreCerts bool

		// commonName used in the Certificate
		commonName string

		// dnsNames is used as the SNA DNSNames
		dnsNames []string

		// ips is used as the SNA IPAddresses
		ips []net.IP
	}{
		"VerifyFull validates domain": {
			verificationMode: tlscommon.VerifyFull,
			hostname:         "localhost",
			dnsNames:         []string{"localhost"},
		},
		"VerifyFull validates IPv4": {
			verificationMode: tlscommon.VerifyFull,
			hostname:         "127.0.0.1",
			ips:              []net.IP{net.IPv4(127, 0, 0, 1)},
		},
		"VerifyFull validates IPv6": {
			verificationMode: tlscommon.VerifyFull,
			hostname:         "::1",
			ips:              []net.IP{net.ParseIP("::1")},
		},
		"VerifyFull domain mismatch returns error": {
			verificationMode: tlscommon.VerifyFull,
			hostname:         "localhost",
			dnsNames:         []string{"example.com"},
			expectingError:   true,
		},
		"VerifyFull IPv4 mismatch returns error": {
			verificationMode: tlscommon.VerifyFull,
			hostname:         "127.0.0.1",
			ips:              []net.IP{net.IPv4(10, 0, 0, 1)},
			expectingError:   true,
		},
		"VerifyFull IPv6 mismatch returns error": {
			verificationMode: tlscommon.VerifyFull,
			hostname:         "::1",
			ips:              []net.IP{net.ParseIP("faca:b0de:baba::ca")},
			expectingError:   true,
		},
		"VerifyFull does not return error when SNA is empty and legacy Common Name is used": {
			verificationMode: tlscommon.VerifyFull,
			hostname:         "localhost",
			commonName:       "localhost",
			expectingError:   false,
		},
		"VerifyFull does not return error when SNA is empty and legacy Common Name is used with IP address": {
			verificationMode: tlscommon.VerifyFull,
			hostname:         "127.0.0.1",
			commonName:       "127.0.0.1",
			expectingError:   false,
		},

		"VerifyStrict validates domain": {
			verificationMode: tlscommon.VerifyStrict,
			hostname:         "localhost",
			dnsNames:         []string{"localhost"},
		},
		"VerifyStrict validates IPv4": {
			verificationMode: tlscommon.VerifyStrict,
			hostname:         "127.0.0.1",
			ips:              []net.IP{net.IPv4(127, 0, 0, 1)},
		},
		"VerifyStrict validates IPv6": {
			verificationMode: tlscommon.VerifyStrict,
			hostname:         "::1",
			ips:              []net.IP{net.ParseIP("::1")},
		},
		"VerifyStrict domain mismatch returns error": {
			verificationMode: tlscommon.VerifyStrict,
			hostname:         "127.0.0.1",
			dnsNames:         []string{"example.com"},
			expectingError:   true,
		},
		"VerifyStrict IPv4 mismatch returns error": {
			verificationMode: tlscommon.VerifyStrict,
			hostname:         "127.0.0.1",
			ips:              []net.IP{net.IPv4(10, 0, 0, 1)},
			expectingError:   true,
		},
		"VerifyStrict IPv6 mismatch returns error": {
			verificationMode: tlscommon.VerifyStrict,
			hostname:         "::1",
			ips:              []net.IP{net.ParseIP("faca:b0de:baba::ca")},
			expectingError:   true,
		},
		"VerifyStrict returns error when SNA is empty and legacy Common Name is used": {
			verificationMode: tlscommon.VerifyStrict,
			hostname:         "localhost",
			commonName:       "localhost",
			expectingError:   true,
		},
		"VerifyStrict returns error when SNA is empty and legacy Common Name is used with IP address": {
			verificationMode: tlscommon.VerifyStrict,
			hostname:         "127.0.0.1",
			commonName:       "127.0.0.1",
			expectingError:   true,
		},
		"VerifyStrict returns error when SNA is empty": {
			verificationMode: tlscommon.VerifyStrict,
			hostname:         "localhost",
			expectingError:   true,
		},

		"VerifyCertificate does not validate domain": {
			verificationMode: tlscommon.VerifyCertificate,
			hostname:         "localhost",
			dnsNames:         []string{"example.com"},
		},
		"VerifyCertificate does not validate IPv4": {
			verificationMode: tlscommon.VerifyCertificate,
			hostname:         "127.0.0.1",
			dnsNames:         []string{"example.com"}, // I believe it cannot be empty
		},
		"VerifyCertificate does not validate IPv6": {
			verificationMode: tlscommon.VerifyCertificate,
			hostname:         "127.0.0.1",
			ips:              []net.IP{net.ParseIP("faca:b0de:baba::ca")},
		},

		"VerifyNone accepts untrusted certificates": {
			verificationMode: tlscommon.VerifyNone,
			hostname:         "127.0.0.1",
			ignoreCerts:      true,
		},
	}
	caCert, err := tlscommontest.GenCA()
	if err != nil {
		t.Fatalf("could not generate root CA certificate: %s", err)
	}

	for name, test := range testcases {
		t.Run(name, func(t *testing.T) {

			// create certificates for server setup
			certs, err := tlscommontest.GenSignedCert(caCert, x509.KeyUsageCertSign, false, test.commonName, test.dnsNames, test.ips, false)
			if err != nil {
				t.Fatalf("could not generate certificates: %s", err)
			}
			// start server with generated cer
			serverURL := startTestServer(t, []tls.Certificate{certs})

			// config to pass to authenticator
			cfg := &Config{
				TLS: &TLSConfig{
					VerificationMode: test.verificationMode.String(),
				},
			}

			// create an instance of authenticator
			auth, err := newAuthenticator(cfg, settings)
			require.NoError(t, err)

			host := extensionsMap{component.NewID(metadata.Type): auth}
			// starts the auth extension
			err = auth.Start(context.Background(), host)
			require.NoError(t, err)

			httpClientConfig.TLS.ServerName = test.hostname
			httpClientConfig.TLS.CAPem = configopaque.String(string(
				pem.EncodeToMemory(&pem.Block{
					Type:  "CERTIFICATE",
					Bytes: caCert.Leaf.Raw,
				})))

			if test.ignoreCerts {
				httpClientConfig.TLS.ServerName = ""
				httpClientConfig.TLS.CAPem = ""
			}

			client, err := httpClientConfig.ToClient(context.Background(), host, settings)
			require.NoError(t, err)

			resp, err := client.Get(serverURL) //nolint:noctx // It is a test
			if err == nil {
				resp.Body.Close()
			}

			if test.expectingError {
				if err != nil {
					// We got the expected error, no need to check the status code
					return
				} else {
					t.Fatalf("expected error, got: %v", err)
				}
			}

			if err != nil {
				t.Fatalf("did not expect an error: %v", err)
			}

			if resp.StatusCode != 200 {
				t.Fatalf("expecting 200 got: %d", resp.StatusCode)
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
