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

package scanner

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"
)

func TestTLSScanner_ScanPort(t *testing.T) {
	// Create a test TLS server with a self-signed certificate
	cert, key := generateTestCert(t)

	tlsCert, err := tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatalf("failed to create TLS cert: %v", err)
	}

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
	})
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	// Get the port that was assigned
	port := listener.Addr().(*net.TCPAddr).Port

	// Accept connections in background and complete handshake
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// For TLS, we need to actually perform the handshake
			// The Accept already does this for tls.Listener, but we need
			// to keep the connection open long enough for the client
			tlsConn := conn.(*tls.Conn)
			_ = tlsConn.Handshake()
			// Keep connection open briefly so client can read
			time.Sleep(100 * time.Millisecond)
			conn.Close()
		}
	}()

	// Test the scanner
	scanner := NewTLSScanner(5 * time.Second)
	ctx := context.Background()

	result, err := scanner.ScanPort(ctx, "127.0.0.1", port)
	if err != nil {
		t.Fatalf("ScanPort failed: %v", err)
	}

	// Verify result
	if result.Port != port {
		t.Errorf("expected port %d, got %d", port, result.Port)
	}

	if result.Address != "127.0.0.1" {
		t.Errorf("expected address 127.0.0.1, got %s", result.Address)
	}

	if result.LeafCertificate == nil {
		t.Fatal("expected leaf certificate, got nil")
	}

	if result.LeafCertificate.SubjectCN != "test.example.com" {
		t.Errorf("expected subject CN 'test.example.com', got %q", result.LeafCertificate.SubjectCN)
	}

	if result.ChainDepth != 1 {
		t.Errorf("expected chain depth 1, got %d", result.ChainDepth)
	}

	if len(result.LeafCertificate.DNSNames) != 2 {
		t.Errorf("expected 2 DNS SANs, got %d", len(result.LeafCertificate.DNSNames))
	}
}

func TestTLSScanner_ScanPort_ConnectionRefused(t *testing.T) {
	scanner := NewTLSScanner(1 * time.Second)
	ctx := context.Background()

	// Try to connect to a port that's not listening
	_, err := scanner.ScanPort(ctx, "127.0.0.1", 59999)
	if err == nil {
		t.Error("expected error for connection refused, got nil")
	}
}

func TestTLSScanner_ScanPort_Timeout(t *testing.T) {
	// Create a listener that accepts but doesn't complete TLS handshake
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	port := listener.Addr().(*net.TCPAddr).Port

	// Accept but don't do anything (simulates hang)
	go func() {
		conn, _ := listener.Accept()
		if conn != nil {
			// Hold connection open but don't respond
			time.Sleep(5 * time.Second)
			conn.Close()
		}
	}()

	scanner := NewTLSScanner(500 * time.Millisecond)
	ctx := context.Background()

	_, err = scanner.ScanPort(ctx, "127.0.0.1", port)
	if err == nil {
		t.Error("expected timeout error, got nil")
	}
}

func TestTlsVersionName(t *testing.T) {
	tests := []struct {
		version uint16
		want    string
	}{
		{tls.VersionTLS10, "TLS 1.0"},
		{tls.VersionTLS11, "TLS 1.1"},
		{tls.VersionTLS12, "TLS 1.2"},
		{tls.VersionTLS13, "TLS 1.3"},
		{0x0000, "Unknown (0x0000)"},
	}

	for _, tt := range tests {
		got := tlsVersionName(tt.version)
		if got != tt.want {
			t.Errorf("tlsVersionName(0x%04x) = %q, want %q", tt.version, got, tt.want)
		}
	}
}

func TestFormatSerialNumber(t *testing.T) {
	tests := []struct {
		input []byte
		want  string
	}{
		{[]byte{0x01, 0x02, 0x03}, "01:02:03"},
		{[]byte{0xAB, 0xCD, 0xEF}, "AB:CD:EF"},
		{[]byte{}, ""},
		{[]byte{0x00}, "00"},
	}

	for _, tt := range tests {
		got := formatSerialNumber(tt.input)
		if got != tt.want {
			t.Errorf("formatSerialNumber(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// generateTestCert creates a self-signed certificate for testing
func generateTestCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "test.example.com",
			Organization: []string{"Test Org"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"test.example.com", "localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	return certPEM, keyPEM
}
