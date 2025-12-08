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

package clientaddrmiddlewareextension // import "github.com/elastic/opentelemetry-collector-components/extension/clientaddrmiddlewareextension"

import (
	"context"
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"google.golang.org/grpc"
)

type clientAddrMiddleware struct {
	cfg *Config
}

func newClientAddrMiddleware(cfg *Config, set extension.Settings) (*clientAddrMiddleware, error) {
	return &clientAddrMiddleware{cfg: cfg}, nil
}

func (c *clientAddrMiddleware) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (c *clientAddrMiddleware) Shutdown(ctx context.Context) error {
	return nil
}

func (c *clientAddrMiddleware) GetHTTPHandler(base http.Handler) (http.Handler, error) {
	return base, nil
}

func (c *clientAddrMiddleware) GetGRPCServerOptions() ([]grpc.ServerOption, error) {
	return []grpc.ServerOption{}, nil
}

func (c *clientAddrMiddleware) GetHTTPRoundTripper(tripper http.RoundTripper) (http.RoundTripper, error) {
	return tripper, nil
}

func (c *clientAddrMiddleware) GetGRPCClientOptions() ([]grpc.DialOption, error) {
	return []grpc.DialOption{}, nil
}
