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

package kubetunnel // import "github.com/elastic/opentelemetry-collector-components/internal/kubetunnel"

import (
	"sort"
	"sync"

	"github.com/elastic/opentelemetry-collector-components/internal/kubetunnel/tunnelpb"
)

// frameSender is the subset of the gRPC server stream the relay needs to push a
// request down a tunnel. tunnelpb.Tunnel_ConnectServer satisfies it.
type frameSender interface {
	Send(*tunnelpb.ServerFrame) error
}

// Tunnel represents one connected collector ("probe"). It multiplexes concurrent
// read requests over a single gRPC stream, correlating them by request id.
type Tunnel struct {
	Info ProbeInfo

	stream frameSender

	// sendMu serializes stream.Send: gRPC streams are not safe for concurrent
	// sends from multiple request goroutines.
	sendMu sync.Mutex

	mu      sync.Mutex
	pending map[string]chan *ReadResult
}

// NewTunnel wraps a gRPC server stream and its registration metadata.
func NewTunnel(stream frameSender, info ProbeInfo) *Tunnel {
	return &Tunnel{
		stream:  stream,
		Info:    info,
		pending: make(map[string]chan *ReadResult),
	}
}

// Call registers a pending request and returns the channel that Deliver will
// signal once the result arrives. The channel is buffered so Deliver never
// blocks even if the caller has already given up. Pair every Call with EndCall.
func (t *Tunnel) Call(requestID string) <-chan *ReadResult {
	ch := make(chan *ReadResult, 1)
	t.mu.Lock()
	t.pending[requestID] = ch
	t.mu.Unlock()
	return ch
}

// EndCall removes a pending request. Safe to call multiple times.
func (t *Tunnel) EndCall(requestID string) {
	t.mu.Lock()
	delete(t.pending, requestID)
	t.mu.Unlock()
}

// Send pushes a request down the tunnel to the collector.
func (t *Tunnel) Send(requestID string, requestJSON []byte) error {
	t.sendMu.Lock()
	defer t.sendMu.Unlock()
	return t.stream.Send(&tunnelpb.ServerFrame{
		RequestId:   requestID,
		RequestJson: requestJSON,
	})
}

// Deliver routes a result coming back up the tunnel to the waiting Call. Unknown
// or already-finished request ids are dropped.
func (t *Tunnel) Deliver(requestID string, result *ReadResult) {
	t.mu.Lock()
	ch, ok := t.pending[requestID]
	t.mu.Unlock()
	if ok {
		ch <- result
	}
}

// Registry tracks every connected probe, keyed by its unique probe id.
type Registry struct {
	mu     sync.RWMutex
	probes map[string]*Tunnel
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{probes: make(map[string]*Tunnel)}
}

// Add registers a tunnel under its probe id, replacing any previous tunnel with
// the same id (e.g. after a reconnect).
func (r *Registry) Add(t *Tunnel) {
	r.mu.Lock()
	r.probes[t.Info.ProbeID] = t
	r.mu.Unlock()
}

// Remove deregisters the tunnel with the given probe id, but only if it is still
// the currently-registered one (so a stale reconnect cleanup can't evict a fresh
// connection).
func (r *Registry) Remove(probeID string, t *Tunnel) {
	r.mu.Lock()
	if cur, ok := r.probes[probeID]; ok && cur == t {
		delete(r.probes, probeID)
	}
	r.mu.Unlock()
}

// Get returns the tunnel for a probe id.
func (r *Registry) Get(probeID string) (*Tunnel, bool) {
	r.mu.RLock()
	t, ok := r.probes[probeID]
	r.mu.RUnlock()
	return t, ok
}

// List returns the metadata of all connected probes (the discoverable targets),
// sorted by probe id for a stable discovery response.
func (r *Registry) List() []ProbeInfo {
	r.mu.RLock()
	out := make([]ProbeInfo, 0, len(r.probes))
	for _, t := range r.probes {
		out = append(out, t.Info)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ProbeID < out[j].ProbeID })
	return out
}
