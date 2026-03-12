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

package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/elastic/opentelemetry-collector-components/receiver/certscannerreceiver/scanner"
)

func newTestScanResult() *scanner.ScanResult {
	return &scanner.ScanResult{
		Port:            443,
		Address:         "127.0.0.1",
		TLSVersion:      0x0304,
		TLSVersionName:  "TLS 1.3",
		CipherSuite:     0x1302,
		CipherSuiteName: "TLS_AES_256_GCM_SHA384",
		ServerName:      "example.com",
		LeafCertificate: &scanner.CertificateInfo{
			SubjectCN:         "*.example.com",
			SubjectDN:         "CN=*.example.com,O=Example Inc,C=US",
			IssuerCN:          "DigiCert CA",
			IssuerDN:          "CN=DigiCert CA,O=DigiCert,C=US",
			SerialNumber:      "01:02:03",
			NotBefore:         time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			NotAfter:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			FingerprintSHA256: "AA:BB:CC",
			DNSNames:          []string{"*.example.com", "example.com"},
			IPAddresses:       []string{"1.2.3.4"},
			EmailAddresses:    []string{"admin@example.com"},
		},
		ChainDepth: 2,
		ScanTime:   time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
		ProcessPID:        1234,
		ProcessName:       "nginx",
		ProcessExecutable: "/usr/sbin/nginx",
		CertificateChain: []*scanner.CertificateInfo{
			{
				SubjectCN:         "*.example.com",
				SubjectDN:         "CN=*.example.com,O=Example Inc,C=US",
				IssuerCN:          "DigiCert CA",
				IssuerDN:          "CN=DigiCert CA,O=DigiCert,C=US",
				SerialNumber:      "01:02:03",
				NotBefore:         time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				NotAfter:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				FingerprintSHA256: "AA:BB:CC",
			},
			{
				SubjectCN:         "DigiCert CA",
				SubjectDN:         "CN=DigiCert CA,O=DigiCert,C=US",
				IssuerCN:          "DigiCert Root",
				IssuerDN:          "CN=DigiCert Root,O=DigiCert,C=US",
				SerialNumber:      "04:05:06",
				NotBefore:         time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				NotAfter:          time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
				FingerprintSHA256: "DD:EE:FF",
			},
		},
	}
}

func TestConvertToLogs(t *testing.T) {
	result := newTestScanResult()
	logs := ConvertToLogs(result, "test-host")

	require.Equal(t, 1, logs.ResourceLogs().Len())
	rl := logs.ResourceLogs().At(0)

	// Check resource attributes
	resAttrs := rl.Resource().Attributes()
	assertAttrStr(t, resAttrs, "observer.type", "certscanner")
	assertAttrStr(t, resAttrs, "observer.hostname", "test-host")

	// Check scope
	require.Equal(t, 1, rl.ScopeLogs().Len())
	sl := rl.ScopeLogs().At(0)
	assert.Equal(t, "certscanner", sl.Scope().Name())
	assert.Equal(t, "1.0.0", sl.Scope().Version())

	// Check log record
	require.Equal(t, 1, sl.LogRecords().Len())
	lr := sl.LogRecords().At(0)
	assert.Equal(t, "TLS certificate discovered", lr.Body().Str())
	assert.Equal(t, "INFO", lr.SeverityText())

	attrs := lr.Attributes()

	// Network fields
	assertAttrStr(t, attrs, "network.transport", "tcp")
	assertAttrInt(t, attrs, "destination.port", 443)
	assertAttrStr(t, attrs, "destination.address", "127.0.0.1")

	// Process fields
	assertAttrInt(t, attrs, "process.pid", 1234)
	assertAttrStr(t, attrs, "process.name", "nginx")
	assertAttrStr(t, attrs, "process.executable", "/usr/sbin/nginx")

	// TLS fields
	assertAttrStr(t, attrs, "tls.version", "TLS 1.3")
	assertAttrStr(t, attrs, "tls.cipher", "TLS_AES_256_GCM_SHA384")
	assertAttrStr(t, attrs, "tls.client.server_name", "example.com")

	// Certificate fields
	assertAttrStr(t, attrs, "tls.server.x509.subject.common_name", "*.example.com")
	assertAttrStr(t, attrs, "tls.server.x509.issuer.common_name", "DigiCert CA")
	assertAttrStr(t, attrs, "tls.server.x509.serial_number", "01:02:03")
	assertAttrStr(t, attrs, "tls.server.hash.sha256", "AA:BB:CC")
	assertAttrInt(t, attrs, "tls.server.certificate.chain_depth", 2)

	// SANs - all types merged into a single flat array per ECS
	sanVal, ok := attrs.Get("tls.server.x509.alternative_names")
	require.True(t, ok, "expected alternative_names attribute")
	assert.Equal(t, 4, sanVal.Slice().Len()) // 2 DNS + 1 IP + 1 email

	// Certificate chain (intermediate certs only, index 1+)
	chainVal, ok := attrs.Get("tls.server.certificate_chain")
	require.True(t, ok, "expected certificate_chain attribute")
	assert.Equal(t, 1, chainVal.Slice().Len())
	chainEntry := chainVal.Slice().At(0).Map()
	cn, _ := chainEntry.Get("subject.common_name")
	assert.Equal(t, "DigiCert CA", cn.Str())
}

func TestConvertToLogs_NoProcess(t *testing.T) {
	result := newTestScanResult()
	result.ProcessPID = 0
	result.ProcessName = ""
	result.ProcessExecutable = ""

	logs := ConvertToLogs(result, "test-host")
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	attrs := lr.Attributes()

	_, ok := attrs.Get("process.pid")
	assert.False(t, ok, "process.pid should not be set when PID is 0")
	_, ok = attrs.Get("process.name")
	assert.False(t, ok, "process.name should not be set when empty")
}

// helpers

func assertAttrStr(t *testing.T, attrs pcommon.Map, key, want string) {
	t.Helper()
	v, ok := attrs.Get(key)
	require.True(t, ok, "missing attribute %q", key)
	assert.Equal(t, want, v.Str(), "attribute %q", key)
}

func assertAttrInt(t *testing.T, attrs pcommon.Map, key string, want int64) {
	t.Helper()
	v, ok := attrs.Get(key)
	require.True(t, ok, "missing attribute %q", key)
	assert.Equal(t, want, v.Int(), "attribute %q", key)
}
