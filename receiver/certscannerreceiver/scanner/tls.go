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

package scanner // import "github.com/elastic/opentelemetry-collector-components/receiver/certscannerreceiver/scanner"

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// CertificateInfo holds extracted certificate metadata.
type CertificateInfo struct {
	SubjectCN         string
	SubjectDN         string
	IssuerCN          string
	IssuerDN          string
	SerialNumber      string
	NotBefore         time.Time
	NotAfter          time.Time
	FingerprintSHA256 string
	DNSNames          []string
	IPAddresses       []string
	EmailAddresses    []string
}

// ScanResult contains all information from a successful TLS scan.
type ScanResult struct {
	Port             int
	Address          string
	TLSVersion       uint16
	TLSVersionName   string
	CipherSuite      uint16
	CipherSuiteName  string
	ServerName       string
	LeafCertificate  *CertificateInfo
	ChainDepth       int
	CertificateChain []*CertificateInfo
	ScanTime time.Time

	// Process information (populated by caller from port discovery)
	ProcessPID        int
	ProcessName       string
	ProcessExecutable string
}

// TLSScanner performs TLS handshakes and extracts certificate info.
type TLSScanner struct {
	timeout time.Duration
}

// NewTLSScanner creates a new TLS scanner with the given timeout.
func NewTLSScanner(timeout time.Duration) *TLSScanner {
	return &TLSScanner{timeout: timeout}
}

// ScanPort attempts a TLS handshake on the given address:port and extracts certificate info.
func (s *TLSScanner) ScanPort(ctx context.Context, addr string, port int) (*ScanResult, error) {
	address := net.JoinHostPort(addr, strconv.Itoa(port))

	// Enforce s.timeout for the entire connection setup (TCP dial + TLS handshake)
	ctxTimeout, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	dialer := &net.Dialer{}
	rawConn, err := dialer.DialContext(ctxTimeout, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("TCP dial failed: %w", err)
	}

	// TLS config - skip verification to get cert even if untrusted
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS10,
	}

	conn := tls.Client(rawConn, tlsConfig)
	if err := conn.HandshakeContext(ctxTimeout); err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("TLS handshake failed: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Get connection state
	state := conn.ConnectionState()

	if len(state.PeerCertificates) == 0 {
		return nil, fmt.Errorf("no certificates presented by server")
	}

	result := &ScanResult{
		Port:            port,
		Address:         addr,
		TLSVersion:      state.Version,
		TLSVersionName:  tlsVersionName(state.Version),
		CipherSuite:     state.CipherSuite,
		CipherSuiteName: tls.CipherSuiteName(state.CipherSuite),
		ServerName:      state.ServerName,
		ScanTime:        time.Now().UTC(),
	}

	// Extract leaf certificate (first in chain)
	leaf := state.PeerCertificates[0]
	result.LeafCertificate = extractCertInfo(leaf)
	result.ChainDepth = len(state.PeerCertificates)

	// Extract full certificate chain
	result.CertificateChain = make([]*CertificateInfo, len(state.PeerCertificates))
	for i, cert := range state.PeerCertificates {
		result.CertificateChain[i] = extractCertInfo(cert)
	}

	return result, nil
}

// extractCertInfo extracts metadata from an X.509 certificate.
func extractCertInfo(cert *x509.Certificate) *CertificateInfo {
	info := &CertificateInfo{
		SubjectCN:      cert.Subject.CommonName,
		SubjectDN:      cert.Subject.String(),
		IssuerCN:       cert.Issuer.CommonName,
		IssuerDN:       cert.Issuer.String(),
		SerialNumber:   formatSerialNumber(cert.SerialNumber.Bytes()),
		NotBefore:      cert.NotBefore.UTC(),
		NotAfter:       cert.NotAfter.UTC(),
		DNSNames:       cert.DNSNames,
		EmailAddresses: cert.EmailAddresses,
	}

	// Compute SHA256 fingerprint
	fingerprint := sha256.Sum256(cert.Raw)
	info.FingerprintSHA256 = formatFingerprint(fingerprint[:])

	// Extract IP SANs
	info.IPAddresses = make([]string, 0, len(cert.IPAddresses))
	for _, ip := range cert.IPAddresses {
		info.IPAddresses = append(info.IPAddresses, ip.String())
	}

	return info
}

// formatSerialNumber formats a certificate serial number as colon-separated hex.
func formatSerialNumber(serial []byte) string {
	if len(serial) == 0 {
		return ""
	}
	parts := make([]string, len(serial))
	for i, b := range serial {
		parts[i] = fmt.Sprintf("%02X", b)
	}
	return strings.Join(parts, ":")
}

// formatFingerprint formats a fingerprint as colon-separated hex.
func formatFingerprint(fp []byte) string {
	parts := make([]string, len(fp))
	for i, b := range fp {
		parts[i] = fmt.Sprintf("%02X", b)
	}
	return strings.Join(parts, ":")
}

// tlsVersionName returns a human-readable name for a TLS version.
func tlsVersionName(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "TLS 1.0"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS13:
		return "TLS 1.3"
	default:
		return fmt.Sprintf("Unknown (0x%04x)", version)
	}
}
