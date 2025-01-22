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
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/auth"
	"google.golang.org/grpc/credentials"
)

var _ auth.Client = (*authenticator)(nil)

type authenticator struct {
	cfg       *Config
	telemetry component.TelemetrySettings
	client    *http.Client // set by Start
}

func newAuthenticator(cfg *Config, telemetry component.TelemetrySettings) (*authenticator, error) {
	return &authenticator{cfg: cfg, telemetry: telemetry}, nil
}

func (a *authenticator) Start(ctx context.Context, host component.Host) error {
	client, err := a.cfg.ClientConfig.ToClient(ctx, host, component.TelemetrySettings{
		// Don't instrument this client's transport, as the exporter's
		// transport is instrumented before delegating to this one.
	})
	if err != nil {
		return err
	}
	a.client = client
	return nil
}

func (a *authenticator) Shutdown(ctx context.Context) error {
	return nil
}

func (a *authenticator) RoundTripper(base http.RoundTripper) (http.RoundTripper, error) {
	return a, nil
}

func (a *authenticator) PerRPCCredentials() (credentials.PerRPCCredentials, error) {
	// Elasticsearch doesn't support gRPC, this function won't be called
	return nil, nil
}

func (a *authenticator) RoundTrip(req *http.Request) (*http.Response, error) {
	return a.client.Transport.RoundTrip(req)
}
