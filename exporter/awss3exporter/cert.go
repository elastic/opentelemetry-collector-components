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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"sync"
	"time"
)

// identityCertSource lazily creates and caches a self-signed mTLS client
// certificate whose only DNS Subject Alternative Name is the caller's
// identity SAN (see clientCertSAN). This certificate is presented on the TLS
// connection used to reach the token endpoint; the ECP proxy terminates
// mTLS, verifies the certificate, and builds the X-Forwarded-Client-Cert
// header itself from the SAN it parses out of it. The exporter therefore
// never constructs or sends an XFCC header directly.
//
// The certificate is never chained to any CA — its sole purpose is to carry
// the identity SAN through the TLS handshake — so its validity window only
// bounds how long a single generated key/cert pair is reused, not any trust
// decision.
type identityCertSource struct {
	san string
	now func() time.Time
	ttl time.Duration

	mu       sync.Mutex
	cert     *tls.Certificate
	notAfter time.Time
}

func newIdentityCertSource(san string, now func() time.Time, ttl time.Duration) *identityCertSource {
	return &identityCertSource{san: san, now: now, ttl: ttl}
}

// get returns a cached certificate, generating a fresh one if the cache is
// empty or the cached certificate is within refreshSkew of its expiry.
func (s *identityCertSource) get() (*tls.Certificate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cert != nil && s.now().Before(s.notAfter.Add(-clientCertRefreshSkew)) {
		return s.cert, nil
	}

	cert, notAfter, err := generateSelfSignedClientCert(s.san, s.now(), s.ttl)
	if err != nil {
		return nil, err
	}
	s.cert = cert
	s.notAfter = notAfter
	return s.cert, nil
}

// clientCertRefreshSkew regenerates the identity certificate this long
// before it expires, mirroring the token source's RefreshSkew so a
// long-lived exporter process never presents an expired client certificate.
const clientCertRefreshSkew = 1 * time.Hour

// generateSelfSignedClientCert creates a fresh ECDSA P-256 key pair and a
// self-signed leaf certificate carrying dnsSAN as its only DNS SAN. It is
// never presented to, or signed by, any certificate authority: its only job
// is to carry the SAN through the mTLS handshake with the ECP proxy.
func generateSelfSignedClientCert(dnsSAN string, now time.Time, ttl time.Duration) (*tls.Certificate, time.Time, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to generate identity key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to generate certificate serial number: %w", err)
	}

	// Tolerate modest clock skew between the exporter and the proxy.
	notBefore := now.Add(-5 * time.Minute)
	notAfter := now.Add(ttl)

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: dnsSAN},
		DNSNames:     []string{dnsSAN},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to create identity certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to parse generated identity certificate: %w", err)
	}

	return &tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
		Leaf:        leaf,
	}, notAfter, nil
}
