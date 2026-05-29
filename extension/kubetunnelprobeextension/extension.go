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

package kubetunnelprobeextension // import "github.com/elastic/opentelemetry-collector-components/extension/kubetunnelprobeextension"

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"

	kubetunnel "github.com/elastic/opentelemetry-collector-components/internal/kubetunnel"
	"github.com/elastic/opentelemetry-collector-components/internal/kubetunnel/tunnelpb"
)

type probeExtension struct {
	cfg      *Config
	settings extension.Settings
	logger   *zap.Logger

	host     component.Host
	executor *executor

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

var _ component.Component = (*probeExtension)(nil)

func newProbeExtension(cfg *Config, set extension.Settings) *probeExtension {
	return &probeExtension{cfg: cfg, settings: set, logger: set.Logger}
}

func (e *probeExtension) Start(_ context.Context, host component.Host) error {
	e.host = host

	exec, err := newExecutor(e.cfg.Kubeconfig, e.cfg.AllowedNamespaces)
	if err != nil {
		return fmt.Errorf("initializing kubernetes client: %w", err)
	}
	e.executor = exec

	// Use a background context so the tunnel outlives the Start call; Shutdown
	// cancels it.
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel

	e.wg.Add(1)
	go e.run(ctx)
	return nil
}

func (e *probeExtension) Shutdown(context.Context) error {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	return nil
}

// run keeps a tunnel to the relay open, redialing with backoff whenever it drops.
func (e *probeExtension) run(ctx context.Context) {
	defer e.wg.Done()
	backoff := e.cfg.ReconnectBackoff
	if backoff <= 0 {
		backoff = 5 * time.Second
	}
	for {
		if ctx.Err() != nil {
			return
		}
		if err := e.connectOnce(ctx); err != nil && ctx.Err() == nil {
			e.logger.Warn("tunnel to relay ended, will reconnect", zap.Error(err), zap.Duration("backoff", backoff))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

// connectOnce dials the relay, registers, and serves read requests until the
// stream ends.
func (e *probeExtension) connectOnce(ctx context.Context) error {
	conn, err := e.cfg.Relay.ToClientConn(ctx, e.host.GetExtensions(), e.settings.TelemetrySettings)
	if err != nil {
		return fmt.Errorf("dialing relay: %w", err)
	}
	defer func() { _ = conn.Close() }()

	stream, err := tunnelpb.NewTunnelClient(conn).Connect(ctx)
	if err != nil {
		return fmt.Errorf("opening tunnel: %w", err)
	}

	// sendMu serializes Send across the registration frame and all concurrent
	// result frames; gRPC client streams are not safe for concurrent Send.
	var sendMu sync.Mutex
	register := &tunnelpb.ClientFrame{Register: e.registration()}
	sendMu.Lock()
	err = stream.Send(register)
	sendMu.Unlock()
	if err != nil {
		return fmt.Errorf("sending registration: %w", err)
	}
	e.logger.Info("registered with relay",
		zap.String("relay", e.cfg.Relay.Endpoint),
		zap.String("probe_id", register.Register.ProbeId),
		zap.String("cluster_id", e.cfg.ClusterID))

	for {
		frame, err := stream.Recv()
		if err != nil {
			return err
		}
		if frame.GetRequestId() == "" {
			continue
		}
		go e.handle(ctx, stream, &sendMu, frame.GetRequestId(), frame.GetRequestJson())
	}
}

func (e *probeExtension) handle(ctx context.Context, stream tunnelpb.Tunnel_ConnectClient, sendMu *sync.Mutex, requestID string, requestJSON []byte) {
	var req kubetunnel.ReadRequest
	var result kubetunnel.ReadResult
	if err := json.Unmarshal(requestJSON, &req); err != nil {
		result = kubetunnel.ReadResult{Error: "probe: could not decode request: " + err.Error()}
	} else {
		result = e.executor.Execute(ctx, req)
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		resultJSON, _ = json.Marshal(kubetunnel.ReadResult{Error: "probe: could not encode result: " + err.Error()})
	}

	sendMu.Lock()
	defer sendMu.Unlock()
	if err := stream.Send(&tunnelpb.ClientFrame{RequestId: requestID, ResultJson: resultJSON}); err != nil {
		e.logger.Warn("failed to send result", zap.String("request_id", requestID), zap.Error(err))
	}
}

// registration builds the metadata frame describing this probe.
func (e *probeExtension) registration() *tunnelpb.Register {
	return &tunnelpb.Register{
		Token:            string(e.cfg.AuthToken),
		ProbeId:          e.probeID(),
		ClusterId:        e.cfg.ClusterID,
		NodeName:         e.cfg.NodeName,
		PodName:          e.cfg.PodName,
		Namespace:        e.cfg.Namespace,
		CollectorVersion: e.settings.BuildInfo.Version,
		K8SVersion:       e.executor.serverVersion(),
	}
}

// probeID returns the configured id, or a sensible default derived from the pod
// identity (falling back to the hostname).
func (e *probeExtension) probeID() string {
	if e.cfg.ProbeID != "" {
		return e.cfg.ProbeID
	}
	if e.cfg.PodName != "" {
		if e.cfg.Namespace != "" {
			return e.cfg.Namespace + "/" + e.cfg.PodName
		}
		return e.cfg.PodName
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "probe"
}
