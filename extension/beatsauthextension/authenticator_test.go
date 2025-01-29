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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configauth"
	"go.opentelemetry.io/collector/config/confighttp"

	"github.com/elastic/elastic-agent-libs/transport/tlscommon"
	"github.com/elastic/opentelemetry-collector-components/extension/beatsauthextension/internal/metadata"
)

// It tests whether VerifyConnection is set on tls.Config
func TestVerifyConnection(t *testing.T) {
	testCerts := tlscommon.GenTestCerts(t)
	fingerprint := tlscommon.GetCertFingerprint(testCerts["ca"])
	settings := componenttest.NewNopTelemetrySettings()

	httpClientConfig := confighttp.NewDefaultClientConfig()
	httpClientConfig.Auth = &configauth.Authentication{
		AuthenticatorID: component.NewID(metadata.Type),
	}

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
		"CATrustedFingerprint and verification mode:VerifyCertificate": {
			verificationMode:     tlscommon.VerifyCertificate,
			peerCerts:            []*x509.Certificate{testCerts["correct"], testCerts["ca"]},
			serverName:           "localhost",
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
			serverName:       "localhost",
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
			verifier := auth.tlsConfig.ToConfig().VerifyConnection
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

type extensionsMap map[component.ID]component.Component

func (m extensionsMap) GetExtensions() map[component.ID]component.Component {
	return m
}
