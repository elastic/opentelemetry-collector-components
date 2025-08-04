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
	"context"
	"fmt"
	"net/http"

	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent-libs/transport/tlscommon"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensionauth"
	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"
)

var _ extensionauth.HTTPClient = (*authenticator)(nil)
var _ extensionauth.GRPCClient = (*authenticator)(nil)
var _ extension.Extension = (*authenticator)(nil)

type authenticator struct {
	cfg       *Config
	telemetry component.TelemetrySettings
	tlsConfig *tlscommon.TLSConfig // set by Start
	logger    *zap.Logger
}

func newAuthenticator(cfg *Config, telemetry component.TelemetrySettings) (*authenticator, error) {
	return &authenticator{cfg: cfg, telemetry: telemetry, logger: telemetry.Logger}, nil
}

func (a *authenticator) Start(ctx context.Context, host component.Host) error {

	// configures logp package
	_, _ = logp.ConfigureWithCoreLocal(logp.DefaultConfig(logp.ContainerEnvironment), a.logger.Core())

	if a.cfg.TLS != nil {
		tlsConfig, err := tlscommon.LoadTLSConfig(&tlscommon.Config{
			VerificationMode:     tlsVerificationModes[a.cfg.TLS.VerificationMode],
			CATrustedFingerprint: a.cfg.TLS.CATrustedFingerprint,
			CASha256:             a.cfg.TLS.CASha256,
		})
		if err != nil {
			return err
		}
		a.tlsConfig = tlsConfig
	}
	return nil
}

func (a *authenticator) Shutdown(ctx context.Context) error {
	return nil
}

func (a *authenticator) RoundTripper(base http.RoundTripper) (http.RoundTripper, error) {
	// At the time of writing, client.Transport is guaranteed to always have type *http.Transport.
	// If this assumption is ever broken, we would need to create and use our own transport, and
	// ignore the one passed in.
	httpTransport, ok := base.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("http.Roundripper is not of type *http.Transport")
	}
	if err := a.configureTransport(httpTransport); err != nil {
		return nil, err
	}
	return httpTransport, nil
}

func (a *authenticator) configureTransport(transport *http.Transport) error {
	if a.tlsConfig != nil {
		// copy incoming root CA into our tls config
		// because ca_trusted_fingerprint will be appended to incoming CA
		a.tlsConfig.RootCAs = transport.TLSClientConfig.RootCAs

		beatTLSConfig := a.tlsConfig.BuildModuleClientConfig(transport.TLSClientConfig.ServerName)

		transport.TLSClientConfig.VerifyConnection = beatTLSConfig.VerifyConnection
		transport.TLSClientConfig.InsecureSkipVerify = beatTLSConfig.InsecureSkipVerify
		transport.TLSClientConfig.RootCAs = a.tlsConfig.RootCAs

	}

	return nil
}

func (a *authenticator) PerRPCCredentials() (credentials.PerRPCCredentials, error) {
	// Elasticsearch doesn't support gRPC, this function won't be called
	return nil, nil
}
