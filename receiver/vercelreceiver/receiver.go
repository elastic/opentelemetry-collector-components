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

package vercelreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/vercelreceiver"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/internal/vercel"
)

var (
	_ receiver.Logs    = (*vercelReceiver)(nil)
	_ receiver.Metrics = (*vercelReceiver)(nil)
)

type vercelReceiver struct {
	cfg      *Config
	settings receiver.Settings
	logger   *zap.Logger

	encoding        vercel.EncodingExtension
	logsConsumer    consumer.Logs
	metricsConsumer consumer.Metrics

	server     *http.Server
	listener   net.Listener
	shutdownWG sync.WaitGroup
}

func newReceiver(cfg *Config, logsConsumer consumer.Logs, metricsConsumer consumer.Metrics, set receiver.Settings) *vercelReceiver {
	return &vercelReceiver{
		cfg:             cfg,
		logsConsumer:    logsConsumer,
		metricsConsumer: metricsConsumer,
		logger:          set.Logger,
		settings:        set,
	}
}

func (r *vercelReceiver) registerLogsConsumer(logsConsumer consumer.Logs) {
	r.logsConsumer = logsConsumer
}

func (r *vercelReceiver) registerMetricsConsumer(metricsConsumer consumer.Metrics) {
	r.metricsConsumer = metricsConsumer
}

func (r *vercelReceiver) Start(ctx context.Context, host component.Host) error {
	mux := http.NewServeMux()
	mux.HandleFunc(http.MethodPost+" "+r.cfg.Route, r.handlePayload)

	ext, ok := host.GetExtensions()[r.cfg.Encoding.Extension]
	if !ok {
		return fmt.Errorf("unknown extension %q", r.cfg.Encoding.Extension)
	}
	var okEncoding bool
	r.encoding, okEncoding = ext.(vercel.EncodingExtension)
	if !okEncoding {
		return fmt.Errorf("extension %q is not a Vercel encoding extension", r.cfg.Encoding.Extension)
	}

	var err error
	r.server, err = r.cfg.ToServer(ctx, host.GetExtensions(), r.settings.TelemetrySettings, mux)
	if err != nil {
		return err
	}

	r.listener, err = r.cfg.ToListener(ctx)
	if err != nil {
		return err
	}

	r.shutdownWG.Go(func() {
		if err := r.server.Serve(r.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			r.logger.Error("HTTP server error", zap.Error(err))
		}
	})

	r.logger.Info("vercel receiver started", zap.String("endpoint", r.listener.Addr().String()))
	return nil
}

func (r *vercelReceiver) Shutdown(ctx context.Context) error {
	if r.server == nil {
		return nil
	}
	err := r.server.Shutdown(ctx)
	r.shutdownWG.Wait()
	return err
}

func (r *vercelReceiver) handlePayload(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	// The extension owns the decoding mechanism; the receiver owns the flush/
	// batch policy. Decoding lives behind the encoding extension, which is where
	// streaming and memory-bounded reads belong so large bodies never overwhelm
	// the receiver. The receiver passes req.Body (not buffered bytes) plus its
	// configured flush options, so batch sizing is tunable without changing how
	// the extension decodes.
	parser, err := r.encoding.NewVercelParser(req.Body, r.decoderOptions()...)
	if err != nil {
		r.logger.Error("failed to create vercel payload parser", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for {
		payload, err := parser.Next()
		if errors.Is(err, io.EOF) {
			w.WriteHeader(http.StatusOK)
			return
		}
		if err != nil {
			r.logger.Error("failed to parse vercel payload", zap.Error(err))
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !r.consumePayload(w, req, payload) {
			return
		}
	}
}

// decoderOptions translates the receiver's flush config into decoder options.
// The extension owns decoding; the receiver owns how large each decoded batch
// is, so batch sizing can be tuned (e.g. during load testing) via config.
func (r *vercelReceiver) decoderOptions() []encoding.DecoderOption {
	return []encoding.DecoderOption{
		encoding.WithFlushItems(r.cfg.Encoding.Flush.Items),
		encoding.WithFlushBytes(r.cfg.Encoding.Flush.Bytes),
	}
}

func (r *vercelReceiver) consumePayload(w http.ResponseWriter, req *http.Request, payload vercel.Payload) bool {
	switch payload.Signal {
	case vercel.SignalLogs:
		if r.logsConsumer == nil {
			http.Error(w, "vercel logs payload received without logs consumer", http.StatusInternalServerError)
			return false
		}
		if payload.Logs.LogRecordCount() > 0 {
			if err := r.logsConsumer.ConsumeLogs(req.Context(), payload.Logs); err != nil {
				r.logger.Error("failed to consume vercel logs", zap.Error(err))
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return false
			}
		}
	case vercel.SignalMetrics:
		if r.metricsConsumer == nil {
			http.Error(w, "vercel metrics payload received without metrics consumer", http.StatusInternalServerError)
			return false
		}
		if payload.Metrics.MetricCount() > 0 {
			if err := r.metricsConsumer.ConsumeMetrics(req.Context(), payload.Metrics); err != nil {
				r.logger.Error("failed to consume vercel metrics", zap.Error(err))
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return false
			}
		}
	default:
		http.Error(w, "unsupported vercel signal", http.StatusBadRequest)
		return false
	}
	return true
}
