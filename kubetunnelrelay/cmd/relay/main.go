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

// Command relay is the public rendezvous service for remote kubectl-read access.
// In-cluster probes (EDOT collectors running the kubetunnelprobe extension) dial
// the gRPC endpoint and hold a tunnel open; the external AI agent uses the HTTP
// endpoint to discover probes (GET /targets) and run read-only commands against a
// chosen one (POST /targets/{probe_id}/read).
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/elastic/opentelemetry-collector-components/internal/kubetunnel/tunnelpb"
	"github.com/elastic/opentelemetry-collector-components/kubetunnelrelay/internal/relay"
)

func main() {
	grpcAddr := flag.String("grpc-addr", ":4317", "address for the gRPC tunnel endpoint that probes dial")
	httpAddr := flag.String("http-addr", ":8080", "address for the agent-facing HTTP API")
	requestTimeout := flag.Duration("request-timeout", 30*time.Second, "how long to wait for a probe to answer a read")
	authTokensFlag := flag.String("auth-tokens", os.Getenv("RELAY_AUTH_TOKENS"), "comma-separated accepted bearer tokens (empty disables auth)")
	tlsCert := flag.String("tls-cert", "", "optional TLS certificate file (applies to both gRPC and HTTP)")
	tlsKey := flag.String("tls-key", "", "optional TLS key file")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	var authTokens []string
	for _, t := range strings.Split(*authTokensFlag, ",") {
		if t = strings.TrimSpace(t); t != "" {
			authTokens = append(authTokens, t)
		}
	}
	if len(authTokens) == 0 {
		logger.Warn("authentication is disabled (no --auth-tokens); do not use as-is in production")
	}

	srv := relay.New(authTokens, *requestTimeout, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// gRPC tunnel server.
	var grpcOpts []grpc.ServerOption
	if *tlsCert != "" && *tlsKey != "" {
		creds, err := credentials.NewServerTLSFromFile(*tlsCert, *tlsKey)
		if err != nil {
			logger.Error("failed to load TLS credentials", "err", err)
			os.Exit(1)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
	}
	grpcServer := grpc.NewServer(grpcOpts...)
	tunnelpb.RegisterTunnelServer(grpcServer, srv)

	grpcLn, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		logger.Error("failed to listen for gRPC", "addr", *grpcAddr, "err", err)
		os.Exit(1)
	}

	httpServer := &http.Server{
		Addr:              *httpAddr,
		Handler:           srv.HTTPHandler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("gRPC tunnel listening", "addr", *grpcAddr)
		errCh <- grpcServer.Serve(grpcLn)
	}()
	go func() {
		logger.Info("HTTP API listening", "addr", *httpAddr)
		if *tlsCert != "" && *tlsKey != "" {
			errCh <- httpServer.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			errCh <- httpServer.ListenAndServe()
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errCh:
		logger.Error("server exited", "err", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	grpcServer.GracefulStop()
}
