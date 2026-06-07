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

// Package relay implements the public rendezvous service: a gRPC tunnel server
// that in-cluster probes dial out to, plus an HTTP API the external AI agent
// calls to discover probes and run read-only commands against them.
package relay

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kubetunnel "github.com/elastic/opentelemetry-collector-components/internal/kubetunnel"
	"github.com/elastic/opentelemetry-collector-components/internal/kubetunnel/tunnelpb"
)

// Server is the relay. It is safe for concurrent use.
type Server struct {
	tunnelpb.UnimplementedTunnelServer

	registry       *kubetunnel.Registry
	requestTimeout time.Duration
	logger         *slog.Logger

	// authTokens, when non-empty, is the set of accepted bearer tokens for both
	// the gRPC (Register) and HTTP legs. Empty means auth is disabled (PoC).
	authTokens map[string]struct{}
}

// New builds a relay. A zero requestTimeout falls back to 30s.
func New(authTokens []string, requestTimeout time.Duration, logger *slog.Logger) *Server {
	if requestTimeout <= 0 {
		requestTimeout = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	tokens := make(map[string]struct{}, len(authTokens))
	for _, t := range authTokens {
		if t = strings.TrimSpace(t); t != "" {
			tokens[t] = struct{}{}
		}
	}
	return &Server{
		registry:       kubetunnel.NewRegistry(),
		requestTimeout: requestTimeout,
		logger:         logger,
		authTokens:     tokens,
	}
}

func (s *Server) authorized(token string) bool {
	if len(s.authTokens) == 0 {
		return true
	}
	_, ok := s.authTokens[strings.TrimSpace(token)]
	return ok
}

// Connect is the gRPC tunnel endpoint. A probe dials it, sends a Register frame,
// then the relay parks the stream and pushes read requests down it.
func (s *Server) Connect(stream tunnelpb.Tunnel_ConnectServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.Unavailable, "stream closed before registration")
	}
	reg := first.GetRegister()
	if reg == nil {
		return status.Error(codes.InvalidArgument, "first frame must carry Register")
	}
	if !s.authorized(reg.GetToken()) {
		return status.Error(codes.Unauthenticated, "invalid token")
	}
	if reg.GetProbeId() == "" {
		return status.Error(codes.InvalidArgument, "probe_id is required")
	}

	info := kubetunnel.ProbeInfo{
		ProbeID:          reg.GetProbeId(),
		ClusterID:        reg.GetClusterId(),
		NodeName:         reg.GetNodeName(),
		PodName:          reg.GetPodName(),
		Namespace:        reg.GetNamespace(),
		CollectorVersion: reg.GetCollectorVersion(),
		K8sVersion:       reg.GetK8SVersion(),
		Labels:           reg.GetLabels(),
		ConnectedAt:      time.Now().UTC(),
	}
	tunnel := kubetunnel.NewTunnel(stream, info)
	s.registry.Add(tunnel)
	s.logger.Info("probe registered", "probe_id", info.ProbeID, "cluster_id", info.ClusterID, "node", info.NodeName)
	defer func() {
		s.registry.Remove(info.ProbeID, tunnel)
		s.logger.Info("probe disconnected", "probe_id", info.ProbeID)
	}()

	// Park the stream: deliver every result back to its waiting HTTP request.
	for {
		frame, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if frame.GetRequestId() == "" {
			continue
		}
		var result kubetunnel.ReadResult
		if err := json.Unmarshal(frame.GetResultJson(), &result); err != nil {
			result = kubetunnel.ReadResult{Error: "relay: could not decode probe result: " + err.Error()}
		}
		tunnel.Deliver(frame.GetRequestId(), &result)
	}
}

// HTTPHandler returns the agent-facing HTTP API.
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /targets", s.handleListTargets)
	// Trailing wildcard so probe ids containing '/' (e.g. "namespace/pod") route
	// correctly. POSTing a ReadRequest to a target performs the read.
	mux.HandleFunc("POST /targets/{probe_id...}", s.handleRead)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// handleListTargets is the discovery endpoint: it lists connected probes so the
// AI agent can pick the right target. Optional cluster_id / node_name filters.
func (s *Server) handleListTargets(w http.ResponseWriter, r *http.Request) {
	if !s.httpAuthorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	cluster := r.URL.Query().Get("cluster_id")
	node := r.URL.Query().Get("node_name")
	all := s.registry.List()
	out := all[:0]
	for _, p := range all {
		if cluster != "" && p.ClusterID != cluster {
			continue
		}
		if node != "" && p.NodeName != node {
			continue
		}
		out = append(out, p)
	}
	writeJSON(w, http.StatusOK, out)
}

// handleRead forwards a read command to a specific probe and returns its result.
func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	if !s.httpAuthorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	probeID := r.PathValue("probe_id")
	tunnel, ok := s.registry.Get(probeID)
	if !ok {
		http.Error(w, "no probe registered with id "+probeID, http.StatusNotFound)
		return
	}

	var req kubetunnel.ReadRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "invalid ReadRequest: "+err.Error(), http.StatusBadRequest)
		return
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		http.Error(w, "could not encode request", http.StatusInternalServerError)
		return
	}

	requestID := uuid.NewString()
	resultCh := tunnel.Call(requestID)
	defer tunnel.EndCall(requestID)

	if err := tunnel.Send(requestID, reqJSON); err != nil {
		http.Error(w, "probe disconnected: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	select {
	case res := <-resultCh:
		writeJSON(w, http.StatusOK, res)
	case <-time.After(s.requestTimeout):
		http.Error(w, "timed out waiting for probe response", http.StatusGatewayTimeout)
	case <-r.Context().Done():
		http.Error(w, "client cancelled", http.StatusRequestTimeout)
	}
}

func (s *Server) httpAuthorized(r *http.Request) bool {
	if len(s.authTokens) == 0 {
		return true
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return s.authorized(token)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
