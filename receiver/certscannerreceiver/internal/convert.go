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

package internal // import "github.com/elastic/opentelemetry-collector-components/receiver/certscannerreceiver/internal"

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/elastic/opentelemetry-collector-components/receiver/certscannerreceiver/scanner"
)

const (
	scopeName    = "certscanner"
	scopeVersion = "1.0.0"
)

// ConvertToLogs creates ECS-aligned log records from a scan result.
func ConvertToLogs(result *scanner.ScanResult, hostname string) plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()

	// Set resource attributes (observer info)
	resAttrs := resourceLogs.Resource().Attributes()
	resAttrs.PutStr("observer.type", "certscanner")
	resAttrs.PutStr("observer.hostname", hostname)

	// Create scope logs
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().SetName(scopeName)
	scopeLogs.Scope().SetVersion(scopeVersion)

	// Create the log record
	logRecord := scopeLogs.LogRecords().AppendEmpty()
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(result.ScanTime))
	logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now().UTC()))
	logRecord.SetSeverityNumber(plog.SeverityNumberInfo)
	logRecord.SetSeverityText("INFO")

	// Set body message
	logRecord.Body().SetStr("TLS certificate discovered")

	attrs := logRecord.Attributes()

	// Network fields (ECS)
	attrs.PutStr("network.transport", "tcp")
	attrs.PutInt("destination.port", int64(result.Port))
	attrs.PutStr("destination.address", result.Address)

	// Process fields (ECS) - for correlation with system integration metrics
	if result.ProcessPID > 0 {
		attrs.PutInt("process.pid", int64(result.ProcessPID))
	}
	if result.ProcessName != "" {
		attrs.PutStr("process.name", result.ProcessName)
	}
	if result.ProcessExecutable != "" {
		attrs.PutStr("process.executable", result.ProcessExecutable)
	}

	// TLS connection fields (ECS)
	attrs.PutStr("tls.version", result.TLSVersionName)
	attrs.PutStr("tls.cipher", result.CipherSuiteName)
	if result.ServerName != "" {
		attrs.PutStr("tls.client.server_name", result.ServerName)
	}

	// Certificate fields (ECS tls.server.x509.*)
	if cert := result.LeafCertificate; cert != nil {
		// Subject info
		if cert.SubjectCN != "" {
			attrs.PutStr("tls.server.x509.subject.common_name", cert.SubjectCN)
		}
		attrs.PutStr("tls.server.x509.subject.distinguished_name", cert.SubjectDN)

		// Issuer info
		if cert.IssuerCN != "" {
			attrs.PutStr("tls.server.x509.issuer.common_name", cert.IssuerCN)
		}
		attrs.PutStr("tls.server.x509.issuer.distinguished_name", cert.IssuerDN)

		// Certificate identifiers
		attrs.PutStr("tls.server.x509.serial_number", cert.SerialNumber)
		attrs.PutStr("tls.server.hash.sha256", cert.FingerprintSHA256)

		// Validity dates
		attrs.PutStr("tls.server.x509.not_before", cert.NotBefore.Format(time.RFC3339))
		attrs.PutStr("tls.server.x509.not_after", cert.NotAfter.Format(time.RFC3339))

		// Chain info
		attrs.PutInt("tls.server.certificate.chain_depth", int64(result.ChainDepth))

		// Subject Alternative Names (SANs) — stored by X.509 SAN type
		if len(cert.DNSNames) > 0 {
			dnsSlice := attrs.PutEmptySlice("tls.server.x509.alternative_names")
			for _, dns := range cert.DNSNames {
				dnsSlice.AppendEmpty().SetStr(dns)
			}
		}
		if len(cert.IPAddresses) > 0 {
			ipSlice := attrs.PutEmptySlice("tls.server.x509.alternative_ip")
			for _, ip := range cert.IPAddresses {
				ipSlice.AppendEmpty().SetStr(ip)
			}
		}
		if len(cert.EmailAddresses) > 0 {
			emailSlice := attrs.PutEmptySlice("tls.server.x509.alternative_email")
			for _, email := range cert.EmailAddresses {
				emailSlice.AppendEmpty().SetStr(email)
			}
		}
	}

	// Add certificate chain info as nested structure
	if len(result.CertificateChain) > 1 {
		chainSlice := attrs.PutEmptySlice("tls.server.certificate_chain")
		// Skip index 0 (leaf cert, already captured above)
		for i := 1; i < len(result.CertificateChain); i++ {
			chainCert := result.CertificateChain[i]
			certMap := chainSlice.AppendEmpty().SetEmptyMap()
			certMap.PutStr("subject.common_name", chainCert.SubjectCN)
			certMap.PutStr("subject.distinguished_name", chainCert.SubjectDN)
			certMap.PutStr("issuer.common_name", chainCert.IssuerCN)
			certMap.PutStr("issuer.distinguished_name", chainCert.IssuerDN)
			certMap.PutStr("serial_number", chainCert.SerialNumber)
			certMap.PutStr("not_before", chainCert.NotBefore.Format(time.RFC3339))
			certMap.PutStr("not_after", chainCert.NotAfter.Format(time.RFC3339))
			certMap.PutStr("fingerprint.sha256", chainCert.FingerprintSHA256)
		}
	}

	return logs
}

