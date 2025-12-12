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

package clientaddrmiddlewareextension

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/extension/extensionmiddleware"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcmetadata "google.golang.org/grpc/metadata"

	"github.com/elastic/opentelemetry-collector-components/extension/clientaddrmiddlewareextension/internal/metadata"
)

type testCtxKey struct{}

const textCtxValue = "other ctx data"

func testCtxWithValues(addr net.Addr) context.Context {
	ctx := context.WithValue(context.Background(), testCtxKey{}, textCtxValue)
	if addr == nil {
		return ctx
	}

	info := client.Info{
		Addr: addr,
	}
	return client.NewContext(ctx, info)
}

// validateOtherCtxValues ensure that other ctx values still exist
func validateOtherCtxValues(t *testing.T, ctx context.Context) {
	val := ctx.Value(testCtxKey{})
	require.Equal(t, val, textCtxValue)
}

func TestHTTPServerMiddleware(t *testing.T) {
	// Create the extension
	f := NewFactory()
	ext, err := f.Create(
		context.Background(), extensiontest.NewNopSettings(metadata.Type),
		&Config{},
	)
	require.NoError(t, err)
	middleware := ext.(extensionmiddleware.HTTPServer)

	// Create a test handler that captures the context
	var capturedCtx context.Context
	baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedCtx = r.Context()
	})

	// Wrap the handler with middleware
	finalHandler, err := middleware.GetHTTPHandler(baseHandler)
	require.NoError(t, err)

	// Create the server
	srv := &http.Server{
		Handler: finalHandler,
	}

	testCases := []struct {
		name               string
		initialAddr        net.Addr
		headers            http.Header
		expectedClientAddr *net.IPAddr
	}{
		{
			name:        "client address updated",
			initialAddr: &net.IPAddr{IP: net.ParseIP("127.0.0.1")},
			headers: http.Header{
				"X-Forwarded-For": []string{"192.168.1.100"},
			},
			expectedClientAddr: &net.IPAddr{IP: net.ParseIP("192.168.1.100")},
		},
		{
			name:        "client address added",
			initialAddr: nil,
			headers: http.Header{
				"X-Forwarded-For": []string{"192.168.1.100"},
			},
			expectedClientAddr: &net.IPAddr{IP: net.ParseIP("192.168.1.100")},
		},
		{
			name:        "no valid address in metadata keys",
			initialAddr: &net.IPAddr{IP: net.ParseIP("127.0.0.1")},
			headers: http.Header{
				"X-Forwarded-For": []string{"invalid.ip.address"},
				"X-Real-Ip":       []string{"another.invalid.ip.address"},
			},
			expectedClientAddr: &net.IPAddr{IP: net.ParseIP("127.0.0.1")},
		},
		{
			name:               "empty metadata keys",
			initialAddr:        &net.IPAddr{IP: net.ParseIP("127.0.0.1")},
			headers:            http.Header{},
			expectedClientAddr: &net.IPAddr{IP: net.ParseIP("127.0.0.1")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test request
			req := httptest.NewRequestWithContext(testCtxWithValues(tc.initialAddr), http.MethodGet, "/test", http.NoBody)
			req.Header = tc.headers

			// Create a response recorder
			rec := httptest.NewRecorder()

			// Serve the request
			srv.Handler.ServeHTTP(rec, req)

			// Get the response
			resp := rec.Result()
			defer resp.Body.Close()

			// Verify the client address
			cl := client.FromContext(capturedCtx)
			require.NotEmpty(t, cl.Addr, "client address should not be empty")
			assert.Equal(t, tc.expectedClientAddr.IP.String(), cl.Addr.String())

			validateOtherCtxValues(t, capturedCtx)
		})
	}
}

type grpcTraceServer struct {
	ptraceotlp.UnimplementedGRPCServer
	f func(context.Context, ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error)
}

func (m *grpcTraceServer) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	return m.f(ctx, req)
}

func TestGRPCServerMiddleware(t *testing.T) {
	// Create and register the extension
	f := NewFactory()
	ext, err := f.Create(
		context.Background(), extensiontest.NewNopSettings(metadata.Type),
		&Config{},
	)
	require.NoError(t, err)
	middleware := ext.(extensionmiddleware.GRPCServer)

	// Get the gRPC server options from the middleware
	// These options include the unary interceptor that extracts client address from metadata
	opts, err := middleware.GetGRPCServerOptions()
	require.NoError(t, err)

	// Create a test server with the middleware options
	srv := grpc.NewServer(opts...)

	// Register a test service that captures the context
	var capturedCtx context.Context
	ptraceotlp.RegisterGRPCServer(srv, &grpcTraceServer{
		f: func(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
			capturedCtx = ctx
			return ptraceotlp.NewExportResponse(), nil
		},
	})

	// Create a listener for the test server
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lis.Close()

	// Start the server
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = srv.Serve(lis)
	}()
	defer func() {
		srv.GracefulStop()
		wg.Wait()
	}()

	// Create a client connection to the test server
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	// Create a trace client using the connection
	traceClient := ptraceotlp.NewGRPCClient(conn)

	testCases := []struct {
		name               string
		initialAddr        net.Addr
		headers            http.Header
		expectedClientAddr *net.IPAddr
	}{
		{
			name:        "client address updated",
			initialAddr: &net.IPAddr{IP: net.ParseIP("127.0.0.1")},
			headers: http.Header{
				"X-Forwarded-For": []string{"192.168.1.100"},
			},
			expectedClientAddr: &net.IPAddr{IP: net.ParseIP("192.168.1.100")},
		},
		{
			name:        "client address added",
			initialAddr: nil,
			headers: http.Header{
				"X-Forwarded-For": []string{"192.168.1.100"},
			},
			expectedClientAddr: &net.IPAddr{IP: net.ParseIP("192.168.1.100")},
		},
		{
			name:        "no valid address in metadata keys",
			initialAddr: &net.IPAddr{IP: net.ParseIP("127.0.0.1")},
			headers: http.Header{
				"X-Forwarded-For": []string{"invalid.ip.address"},
				"X-Real-Ip":       []string{"another.invalid.ip.address"},
			},
			// Server-side context doesn't have initial client info, so when no valid address
			// is found in headers, the address will be nil
			expectedClientAddr: nil,
		},
		{
			name:        "empty metadata keys",
			initialAddr: &net.IPAddr{IP: net.ParseIP("127.0.0.1")},
			headers:     http.Header{},
			// Server-side context doesn't have initial client info, so when no headers
			// are present, the address will be nil
			expectedClientAddr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test request
			ctx := testCtxWithValues(tc.initialAddr)
			for k, v := range tc.headers {
				ctx = grpcmetadata.AppendToOutgoingContext(ctx, strings.ToLower(k), v[0])
			}

			// Add other metadata to the context
			ctx = grpcmetadata.AppendToOutgoingContext(ctx, "other-data", textCtxValue)

			// Send the request and capture response metadata
			_, err := traceClient.Export(ctx, ptraceotlp.NewExportRequest())
			require.NoError(t, err)

			// Verify the client address from the server-side context (where middleware updates it)
			cl := client.FromContext(capturedCtx)
			if tc.expectedClientAddr == nil {
				require.Nil(t, cl.Addr, "the client address should be nil")
				return
			}
			require.NotNil(t, cl.Addr, "the client address should not be nil")
			require.Equal(t, tc.expectedClientAddr.IP.String(), cl.Addr.String())

			// Verify that other metadata was preserved
			md, ok := grpcmetadata.FromOutgoingContext(ctx)
			require.True(t, ok)
			otherData, ok := md["other-data"]
			require.True(t, ok)
			require.Equal(t, otherData, []string{textCtxValue})
		})
	}
}
