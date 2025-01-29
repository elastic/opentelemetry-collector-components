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
	"fmt"

	"github.com/elastic/elastic-agent-libs/transport/tlscommon"
	"go.opentelemetry.io/collector/component"
)

type Config struct {
	TLS *TLSConfig `mapstructure:"tls"`
}

var tlsVerificationModes = map[string]tlscommon.TLSVerificationMode{
	"full":        tlscommon.VerifyFull,
	"strict":      tlscommon.VerifyStrict,
	"none":        tlscommon.VerifyNone,
	"certificate": tlscommon.VerifyCertificate,
}

type TLSConfig struct {
	VerificationMode     string   `mapstructure:"verification_mode"`
	CATrustedFingerprint string   `mapstructure:"ca_trusted_fingerprint"`
	CASha256             []string `mapstructure:"ca_sha256"`
	ServerName           string   `mapstructure:"server_name"`
}

func createDefaultConfig() component.Config {
	return &Config{
		&TLSConfig{
			VerificationMode: "full",
		},
	}
}

func (cfg *Config) Validate() error {
	if _, ok := tlsVerificationModes[cfg.TLS.VerificationMode]; !ok {
		return fmt.Errorf("unsupported verification mode: %s", cfg.TLS.VerificationMode)
	}
	return nil
}
