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

package awss3exporter

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTokenTestServer(t *testing.T, calls *atomic.Int64, expiresAt func() int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "local", r.Header.Get("X-Elastic-Region"))
		// Identity now travels via the mTLS client certificate rather than a
		// client-supplied header: the exporter must never set XFCC itself.
		assert.Empty(t, r.Header.Get("X-Forwarded-Client-Cert"))
		body, _ := io.ReadAll(r.Body)
		assert.JSONEq(t, `{"aud":"sts.amazonaws.com"}`, string(body))

		n := calls.Load()
		fmt.Fprintf(w, `{"token":"token-%d","expires_at":%d}`, n, expiresAt())
	}))
}

func newTestTokenSource(srv *httptest.Server, now func() time.Time) *tokenSource {
	return &tokenSource{
		client: srv.Client(),
		cfg: TokenEndpointConfig{
			URL:         srv.URL,
			Region:      "local",
			Audience:    "sts.amazonaws.com",
			RefreshSkew: time.Minute,
		},
		now: now,
	}
}

func TestTokenSourceCachesUntilExpiry(t *testing.T) {
	var calls atomic.Int64
	base := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	now := base
	// token valid for one hour from the fixed base time
	srv := newTokenTestServer(t, &calls, func() int64 { return base.Add(time.Hour).Unix() })
	defer srv.Close()

	src := newTestTokenSource(srv, func() time.Time { return now })

	tok, err := src.GetIdentityToken()
	require.NoError(t, err)
	assert.Equal(t, "token-1", string(tok))

	// Still well within validity: served from cache, no new fetch.
	now = base.Add(30 * time.Minute)
	tok, err = src.GetIdentityToken()
	require.NoError(t, err)
	assert.Equal(t, "token-1", string(tok))
	assert.Equal(t, int64(1), calls.Load())
}

func TestTokenSourceRefetchesWithinSkew(t *testing.T) {
	var calls atomic.Int64
	base := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	now := base
	srv := newTokenTestServer(t, &calls, func() int64 { return base.Add(time.Hour).Unix() })
	defer srv.Close()

	src := newTestTokenSource(srv, func() time.Time { return now })

	_, err := src.GetIdentityToken()
	require.NoError(t, err)
	assert.Equal(t, int64(1), calls.Load())

	// Within RefreshSkew (1m) of the 1h expiry -> refetch.
	now = base.Add(time.Hour - 30*time.Second)
	tok, err := src.GetIdentityToken()
	require.NoError(t, err)
	assert.Equal(t, "token-2", string(tok))
	assert.Equal(t, int64(2), calls.Load())
}

func TestTokenSourceInvalidateForcesRefetch(t *testing.T) {
	var calls atomic.Int64
	base := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	srv := newTokenTestServer(t, &calls, func() int64 { return base.Add(time.Hour).Unix() })
	defer srv.Close()

	src := newTestTokenSource(srv, func() time.Time { return base })

	_, err := src.GetIdentityToken()
	require.NoError(t, err)
	assert.Equal(t, int64(1), calls.Load())

	// Simulate STS rejecting the token; next retrieval must fetch a fresh one
	// even though it has not yet expired.
	src.invalidate()
	tok, err := src.GetIdentityToken()
	require.NoError(t, err)
	assert.Equal(t, "token-2", string(tok))
	assert.Equal(t, int64(2), calls.Load())
}

func TestTokenSourceErrorOnNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "denied", http.StatusForbidden)
	}))
	defer srv.Close()

	src := newTestTokenSource(srv, time.Now)
	_, err := src.GetIdentityToken()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 403")
}

func TestTokenSourceProviderReusesPerTenant(t *testing.T) {
	p := newTokenSourceProvider(TokenEndpointConfig{URL: "http://localhost:8443/token"}, time.Hour, time.Now)

	a, err := p.get(target{targetType: targetTypeServerless, targetID: "proj123"}, "org1")
	require.NoError(t, err)
	b, err := p.get(target{targetType: targetTypeServerless, targetID: "proj123"}, "org1")
	require.NoError(t, err)
	c, err := p.get(target{targetType: targetTypeServerless, targetID: "proj999"}, "org1")
	require.NoError(t, err)

	assert.Same(t, a, b)
	assert.NotSame(t, a, c)
}

func TestTokenSourceProviderRejectsUnknownTargetType(t *testing.T) {
	p := newTokenSourceProvider(TokenEndpointConfig{URL: "http://localhost:8443/token"}, time.Hour, time.Now)

	_, err := p.get(target{targetType: "unknown", targetID: "id"}, "org1")
	require.Error(t, err)
}

// TestTokenSourceUsesClientCertificate exercises the mTLS wiring end to end:
// the provider must configure the tenant's HTTP client with a
// GetClientCertificate callback that hands the TLS server the identity
// certificate carrying the expected SAN.
func TestTokenSourceUsesClientCertificate(t *testing.T) {
	p := newTokenSourceProvider(TokenEndpointConfig{URL: "http://localhost:8443/token"}, time.Hour, time.Now)

	src, err := p.get(target{targetType: targetTypeHosted, targetID: "dep123"}, "org1")
	require.NoError(t, err)

	getCert := src.client.Transport.(*http.Transport).TLSClientConfig.GetClientCertificate
	require.NotNil(t, getCert)

	cert, err := getCert(nil)
	require.NoError(t, err)
	require.Len(t, cert.Certificate, 1)
	assert.Equal(t, "dep123.deployment.org1.account", cert.Leaf.DNSNames[0])
}
