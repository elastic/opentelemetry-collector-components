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

package awss3exporter // import "github.com/elastic/opentelemetry-collector-components/exporter/awss3exporter"

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// tokenResponse is the payload returned by the local token endpoint. expires_at
// is a Unix timestamp (seconds) marking when the token stops being valid.
type tokenResponse struct {
	Token     string `json:"token"`
	ExpiresAt int64  `json:"expires_at"`
}

// tokenSource fetches and caches a web identity (OIDC/JWT) token for a single
// tenant, identified by its target (project/deployment) and org IDs. It
// implements the AWS SDK's stscreds.IdentityTokenRetriever interface so it
// can back an AssumeRoleWithWebIdentity credentials provider.
//
// Identity is established via mTLS rather than a client-supplied header: the
// tenant's identity is encoded as a DNS SAN on a self-signed certificate
// (identityCertSource) presented during the TLS handshake with the token
// endpoint. In production the token endpoint is the ECP proxy, which
// terminates that mTLS connection, verifies the certificate, and derives the
// X-Forwarded-Client-Cert header itself from the SAN — the exporter never
// builds that header directly, so it cannot spoof an identity the proxy
// didn't itself observe on the wire.
//
// The cached token is reused until it is within RefreshSkew of expiry, at which
// point a new one is fetched. Callers can also force a refetch via invalidate,
// which the credentials layer uses when STS rejects the token.
type tokenSource struct {
	client *http.Client
	cfg    TokenEndpointConfig
	now    func() time.Time

	mu        sync.Mutex
	token     []byte
	expiresAt time.Time
}

// GetIdentityToken returns a valid web identity token, fetching a fresh one if
// the cache is empty or the cached token is expiring. It satisfies
// stscreds.IdentityTokenRetriever.
func (s *tokenSource) GetIdentityToken() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.token) > 0 && s.now().Before(s.expiresAt.Add(-s.cfg.RefreshSkew)) {
		return s.token, nil
	}
	if err := s.fetchLocked(); err != nil {
		return nil, err
	}
	return s.token, nil
}

// invalidate drops the cached token so the next GetIdentityToken fetches a new
// one. Used when the credentials provider observes an invalid/expired token.
func (s *tokenSource) invalidate() {
	s.mu.Lock()
	s.token = nil
	s.mu.Unlock()
}

// fetchLocked retrieves a new token from the endpoint. The caller must hold s.mu.
// Identity is conveyed to the endpoint via the mTLS client certificate
// configured on s.client, not via a request header.
func (s *tokenSource) fetchLocked() error {
	body := fmt.Sprintf(`{"aud":%q}`, s.cfg.Audience)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, s.cfg.URL, strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.Region != "" {
		req.Header.Set("X-Elastic-Region", s.cfg.Region)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to request web identity token: %w", err)
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("failed to read token response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token endpoint returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(payload)))
	}

	var tr tokenResponse
	if err := json.Unmarshal(payload, &tr); err != nil {
		return fmt.Errorf("failed to decode token response: %w", err)
	}
	if tr.Token == "" {
		return fmt.Errorf("token endpoint returned an empty token")
	}

	s.token = []byte(tr.Token)
	s.expiresAt = time.Unix(tr.ExpiresAt, 0)
	return nil
}

// tokenKey identifies the tenant a token is scoped to.
type tokenKey struct {
	targetType targetType
	targetID   string
	orgID      string
}

// tokenSourceProvider hands out per-tenant tokenSources, so that a single
// token (and a single generated identity certificate) is fetched/created and
// reused across all S3 destinations belonging to the same tenant.
type tokenSourceProvider struct {
	cfg     TokenEndpointConfig
	certTTL time.Duration
	now     func() time.Time
	// baseTransport is cloned per tenant to seed the transport's non-TLS
	// defaults (proxy-from-environment, dial timeouts, etc). Overridable in
	// tests to point at an httptest.Server without a real cert chain.
	baseTransport *http.Transport

	mu      sync.Mutex
	sources map[tokenKey]*tokenSource
}

func newTokenSourceProvider(cfg TokenEndpointConfig, certTTL time.Duration, now func() time.Time) *tokenSourceProvider {
	return &tokenSourceProvider{
		cfg:     cfg,
		certTTL: certTTL,
		now:     now,
		sources: map[tokenKey]*tokenSource{},
	}
}

// get returns the tokenSource for the given tenant, creating one (along with
// its dedicated mTLS-enabled HTTP client) on first use.
func (p *tokenSourceProvider) get(t target, orgID string) (*tokenSource, error) {
	key := tokenKey{targetType: t.targetType, targetID: t.targetID, orgID: orgID}

	p.mu.Lock()
	defer p.mu.Unlock()

	if s, ok := p.sources[key]; ok {
		return s, nil
	}

	san, err := clientCertSAN(t, orgID)
	if err != nil {
		return nil, fmt.Errorf("failed to build identity certificate SAN: %w", err)
	}
	certSource := newIdentityCertSource(san, p.now, p.certTTL)

	transport := p.newTransport()
	transport.TLSClientConfig = &tls.Config{
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return certSource.get()
		},
	}

	s := &tokenSource{
		client: &http.Client{Timeout: p.cfg.Timeout, Transport: transport},
		cfg:    p.cfg,
		now:    p.now,
	}
	p.sources[key] = s
	return s, nil
}

// newTransport returns a fresh *http.Transport for a tokenSource to attach
// its per-tenant TLSClientConfig to. Cloning http.DefaultTransport keeps the
// standard proxy-from-environment / timeout defaults without sharing a TLS
// config (and therefore a client certificate) across tenants.
func (p *tokenSourceProvider) newTransport() *http.Transport {
	if p.baseTransport != nil {
		return p.baseTransport.Clone()
	}
	return http.DefaultTransport.(*http.Transport).Clone()
}
