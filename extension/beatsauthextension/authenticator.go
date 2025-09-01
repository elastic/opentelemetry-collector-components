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

	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent-libs/transport/httpcommon"
	"go.elastic.co/apm/module/apmelasticsearch/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensionauth"
	"google.golang.org/grpc/credentials"
)

var _ extensionauth.HTTPClient = (*authenticator)(nil)
var _ extensionauth.GRPCClient = (*authenticator)(nil)
var _ extension.Extension = (*authenticator)(nil)
var defaultOutput = "elasticsearch"

type authenticator struct {
	telemetry    component.TelemetrySettings
	httpSettings httpcommon.HTTPTransportSettings
	output       string // users can also pass output key, this is required to switch between different http transport options
	logger       *logp.Logger
	client       *http.Client
}

func newAuthenticator(cfg *Config, telemetry component.TelemetrySettings) (*authenticator, error) {
	logger, err := logp.NewZapLogger(telemetry.Logger)
	if err != nil {
		return nil, err
	}

	parsedCfg, err := config.NewConfigFrom(cfg)
	beatAuthConfig := httpcommon.DefaultHTTPTransportSettings()
	err = parsedCfg.Unpack(&beatAuthConfig)
	if err != nil {
		return nil, fmt.Errorf("failed unpacking config: %w", err)
	}

	if value, ok := cfg.BeatAuthconfig["output"]; ok && value != defaultOutput {
		return nil, fmt.Errorf("%s output is not supported: %w", value, err)
	}

	return &authenticator{httpSettings: beatAuthConfig, telemetry: telemetry, logger: logger, output: defaultOutput}, nil
}

func (a *authenticator) Start(ctx context.Context, host component.Host) error {
	var err error
	a.client, err = a.httpSettings.Client(a.getHTTPOptions()...)
	if err != nil {
		return fmt.Errorf("could not create http client: %w", err)
	}
	return nil
}

func (a *authenticator) Shutdown(ctx context.Context) error {
	return nil
}

func (a *authenticator) RoundTripper(base http.RoundTripper) (http.RoundTripper, error) {
	return a.client.Transport, nil
}

// getHTTPOptions returns a list of http transport options based on configured output
// default output if not configured is elasticsearch
func (a *authenticator) getHTTPOptions() []httpcommon.TransportOption {
	switch a.output {
	case defaultOutput:
		// these options are derived from beats codebase
		// Ref: https://github.com/khushijain21/beats/blob/main/libbeat/esleg/eslegclient/connection.go#L163-L171
		return []httpcommon.TransportOption{
			httpcommon.WithLogger(a.logger),
			// httpcommon.WithIOStats(s.Observer), 		// we don't have access to observer
			httpcommon.WithKeepaliveSettings{IdleConnTimeout: a.httpSettings.IdleConnTimeout},
			httpcommon.WithModRoundtripper(func(rt http.RoundTripper) http.RoundTripper {
				return apmelasticsearch.WrapRoundTripper(rt)
			}),
			// httpcommon.WithHeaderRoundTripper(map[string]string{"User-Agent": s.UserAgent}), // we don't have access to useragent
		}
	}

	return nil
}

func (a *authenticator) PerRPCCredentials() (credentials.PerRPCCredentials, error) {
	// Elasticsearch doesn't support gRPC, this function won't be called
	return nil, nil
}
