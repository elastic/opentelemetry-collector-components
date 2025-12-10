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

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/elastic/opentelemetry-collector-components/extension/clientaddrmiddlewareextension/internal/netutil"
)

type clientAddrMiddleware struct {
	cfg *Config
}

func newClientAddrMiddleware(cfg *Config, set extension.Settings) (*clientAddrMiddleware, error) {
	return &clientAddrMiddleware{cfg: cfg}, nil
}

// Start starts the middleware extension.
func (c *clientAddrMiddleware) Start(ctx context.Context, host component.Host) error {
	return nil
}

// Shutdown shuts down the middleware extension.
func (c *clientAddrMiddleware) Shutdown(ctx context.Context) error {
	return nil
}

// ctxWithClientAddr returns a new context with updated client info that contains
// a valid ip address found in the headers.
// If no client address is found, the original context is returned.
func ctxWithClientAddr(ctx context.Context, headers map[string][]string) context.Context {
	if ip := netutil.ClientAddrFromHeaders(headers); ip != nil {
		cl := client.FromContext(ctx)
		cl.Addr = ip
		return client.NewContext(ctx, cl)
	}
	return ctx
}

// GetHTTPHandler HTTP server middleware that returns an HTTP handler that updates the ctx client.Info.Address
// using request headers.
func (c *clientAddrMiddleware) GetHTTPHandler(base http.Handler) (http.Handler, error) {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := ctxWithClientAddr(r.Context(), r.Header)
		base.ServeHTTP(w, r.WithContext(ctx))
	}), nil
}

// GetGRPCServerOptions GRPC server middleware that returns gRPC server options that updates the ctx client.Info.Address
// using request metadata.
func (c *clientAddrMiddleware) GetGRPCServerOptions() ([]grpc.ServerOption, error) {
	opt := grpc.ChainUnaryInterceptor(
		func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			md, ok := metadata.FromIncomingContext(ctx)
			if ok {
				ctx = ctxWithClientAddr(ctx, md)
			}
			return handler(ctx, req)
		},
	)
	return []grpc.ServerOption{opt}, nil
}

type clientAddrRoundTripper struct {
	base http.RoundTripper
}

func (c *clientAddrRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx := ctxWithClientAddr(r.Context(), r.Header)
	return c.base.RoundTrip(r.WithContext(ctx))
}

// GetHTTPRoundTripper HTTP client middleware that returns a HTTP round tripper that updates the ctx client.Info.Address
// using request headers.
func (c *clientAddrMiddleware) GetHTTPRoundTripper(base http.RoundTripper) (http.RoundTripper, error) {
	return &clientAddrRoundTripper{base: base}, nil
}

// GetGRPCClientOptions GRPC client middleware that returns gRPC client dial options that updates the ctx client.Info.Address
// using request metadata.
func (c *clientAddrMiddleware) GetGRPCClientOptions() ([]grpc.DialOption, error) {
	opt := grpc.WithChainUnaryInterceptor(
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			md, ok := metadata.FromOutgoingContext(ctx)
			if ok {
				ctx = ctxWithClientAddr(ctx, md)
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		},
	)
	return []grpc.DialOption{opt}, nil
}
