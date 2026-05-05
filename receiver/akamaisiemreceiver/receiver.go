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

package akamaisiemreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver"

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/akamaiclient"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/auth"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/cursor"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/poller"
)

// akamaiReceiver polls the Akamai SIEM API and emits logs as plog.Logs with
// raw JSON in a body map keyed "message", along with data_stream.* fields and
// the elastic.mapping.mode: bodymap scope attribute. The Elasticsearch exporter
// serializes the body map directly into the document.
type akamaiReceiver struct {
	cfg      *Config
	settings receiver.Settings
	log      *zap.Logger
	consumer consumer.Logs

	cancel context.CancelFunc
	wg     sync.WaitGroup
	tracer trace.Tracer // nil-safe
}

func newAkamaiReceiver(cfg *Config, settings receiver.Settings, cons consumer.Logs) (*akamaiReceiver, error) {
	return &akamaiReceiver{
		cfg:      cfg,
		settings: settings,
		log:      settings.Logger,
		consumer: cons,
	}, nil
}

// Start implements receiver.Logs.
func (r *akamaiReceiver) Start(ctx context.Context, host component.Host) error {
	// Create cursor store for state persistence via storage extension.
	var cursorStore *cursor.CursorStore
	if r.cfg.StorageID != nil {
		storageClient, err := getStorageClient(ctx, host, r.cfg.StorageID, r.settings.ID)
		if err != nil {
			return fmt.Errorf("failed to get storage client: %w", err)
		}
		cursorStore = cursor.NewCursorStore(storageClient)
	}

	// Load persisted cursor.
	var cur cursor.Cursor
	if cursorStore != nil {
		var err error
		cur, err = cursorStore.Load(ctx)
		if err != nil {
			r.log.Warn("failed to load cursor, starting fresh", zap.Error(err))
			cur = cursor.Cursor{}
		} else if cur.ChainFrom != 0 {
			r.log.Info("loaded persisted cursor",
				zap.Int64("chain_from", cur.ChainFrom),
				zap.Int64("chain_to", cur.ChainTo),
				zap.Bool("caught_up", cur.CaughtUp),
				zap.String("last_offset", cur.LastOffset),
			)
		}
	}

	// Create HTTP client from confighttp.ClientConfig (handles TLS, proxy, timeout).
	httpClient, err := r.cfg.HTTP.ToClient(ctx, host.GetExtensions(), r.settings.TelemetrySettings)
	if err != nil {
		return fmt.Errorf("failed to create HTTP client: %w", err)
	}

	// Wrap transport with EdgeGrid signing. ToClient may return a client with a
	// nil Transport (meaning use http.DefaultTransport); guard against that so
	// auth.Transport.RoundTrip never dereferences a nil Base.
	signer := auth.NewEdgeGridSigner(
		string(r.cfg.Authentication.ClientToken),
		string(r.cfg.Authentication.ClientSecret),
		string(r.cfg.Authentication.AccessToken),
	)
	base := httpClient.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	httpClient.Transport = &auth.Transport{
		Base:   base,
		Signer: signer,
	}

	client, err := akamaiclient.NewClient(
		httpClient,
		r.cfg.HTTP.Endpoint,
		r.cfg.ConfigIDs,
		r.log,
	)
	if err != nil {
		return err
	}

	pollerCfg := poller.PollerConfig{
		EventLimit:              r.cfg.EventLimit,
		InitialLookback:         r.cfg.InitialLookback,
		OffsetTTL:               r.cfg.OffsetTTL,
		MaxRecoveryAttempts:     r.cfg.MaxRecoveryAttempts,
		InvalidTimestampRetries: r.cfg.InvalidTimestampRetries,
		BatchSize:               r.cfg.BatchSize,
		StreamBufferSize:        r.cfg.StreamBufferSize,
	}

	// Build telemetry instruments.
	tb, err := metadata.NewTelemetryBuilder(r.settings.TelemetrySettings)
	if err != nil {
		r.log.Warn("failed to create telemetry builder, metrics disabled", zap.Error(err))
	}
	var tel *poller.Telemetry
	if tb != nil {
		r.tracer = metadata.Tracer(r.settings.TelemetrySettings)
		tel = &poller.Telemetry{
			Tracer:             r.tracer,
			Requests:           tb.AkamaiSiemRequests,
			RequestErrors:      tb.AkamaiSiemRequestErrors,
			EventsReceived:     tb.AkamaiSiemEventsReceived,
			EventsEmitted:      tb.AkamaiSiemEventsEmitted,
			OffsetExpired:      tb.AkamaiSiemOffsetExpired,
			OffsetTTLDrops:     tb.AkamaiSiemOffsetTTLDrops,
			RecoveryAttempts:   tb.AkamaiSiemRecoveryAttempts,
			InvalidTSRetries:   tb.AkamaiSiemInvalidTimestampRetries,
			RequestDuration:    tb.AkamaiSiemRequestDuration,
			PollDuration:       tb.AkamaiSiemPollDuration,
			EventsPerSecond:    tb.AkamaiSiemEventsPerSecond,
			PagesProcessed:     tb.AkamaiSiemPagesProcessed,
			CursorPersists:     tb.AkamaiSiemCursorPersists,
			BytesReceived:      tb.AkamaiSiemBytesReceived,
			PageProcessingTime: tb.AkamaiSiemPageProcessingTime,

			EventsPerPage: tb.AkamaiSiemEventsPerPage,
		}
	}

	poll := poller.NewPoller(client, cursorStore, cur, pollerCfg, r.emitEvents, r.log, tel)

	pollCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer client.Close()
		if cursorStore != nil {
			defer func() { _ = cursorStore.Close(context.Background()) }()
		}
		r.pollLoop(pollCtx, poll)
	}()

	storageInfo := "disabled"
	if r.cfg.StorageID != nil {
		storageInfo = r.cfg.StorageID.String()
	}
	r.log.Info("akamai SIEM receiver started",
		zap.String("endpoint", r.cfg.HTTP.Endpoint),
		zap.String("config_ids", r.cfg.ConfigIDs),
		zap.Duration("poll_interval", r.cfg.PollInterval),
		zap.Int("event_limit", r.cfg.EventLimit),
		zap.String("storage", storageInfo),
	)

	return nil
}

// Shutdown implements receiver.Logs. It respects the context deadline so a
// hung poll goroutine does not block the collector's shutdown indefinitely.
func (r *akamaiReceiver) Shutdown(ctx context.Context) error {
	r.log.Info("akamai SIEM receiver shutting down")
	if r.cancel != nil {
		r.cancel()
	}
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		r.log.Info("akamai SIEM receiver stopped")
		return nil
	case <-ctx.Done():
		r.log.Warn("akamai SIEM receiver shutdown context expired", zap.Error(ctx.Err()))
		return ctx.Err()
	}
}

func (r *akamaiReceiver) pollLoop(ctx context.Context, poll *poller.Poller) {
	// Run first poll immediately.
	if err := poll.Poll(ctx); err != nil {
		if ctx.Err() != nil {
			return
		}
		r.log.Error("poll failed", zap.Error(err))
	}

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := poll.Poll(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				r.log.Error("poll failed", zap.Error(err))
			}
		}
	}
}

// emitEvents converts raw JSON event strings to plog.Logs and forwards them
// to the configured consumer.
//
// Each log record carries the raw Akamai JSON in a body map keyed "message",
// alongside data_stream.{type,dataset,namespace} body keys for Kibana filters.
// data_stream.* is also written to the resource attributes so the Elasticsearch
// exporter's dynamic routing can target the correct data stream. The
// elastic.mapping.mode: bodymap scope attribute tells the ES exporter to
// serialize the body map fields directly into the indexed document.
func (r *akamaiReceiver) emitEvents(ctx context.Context, events []string) error {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()

	// Resource attributes — used by the ES exporter's dynamic routing for
	// data-stream targeting. Bodymap mode does NOT serialize resource attrs
	// into the document, so these data_stream.* values are also written into
	// the body map below for Kibana filters.
	rattr := rl.Resource().Attributes()
	rattr.PutStr("data_stream.type", r.cfg.DataStream.Type)
	rattr.PutStr("data_stream.dataset", r.cfg.DataStream.Dataset)
	rattr.PutStr("data_stream.namespace", r.cfg.DataStream.Namespace)

	// Scope attribute — tells the Elasticsearch exporter to use bodymap mode,
	// which serializes the body map fields directly into the document.
	sl.Scope().Attributes().PutStr("elastic.mapping.mode", "bodymap")

	now := pcommon.NewTimestampFromTime(time.Now())

	if r.tracer != nil {
		var span trace.Span
		ctx, span = r.tracer.Start(ctx, "akamai_siem.EmitEvents",
			trace.WithAttributes(attribute.Int("event_count", len(events))),
		)
		defer span.End()
	}

	for _, rawJSON := range events {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(now)
		lr.SetObservedTimestamp(now)
		body := lr.Body().SetEmptyMap()
		body.PutStr("message", rawJSON)
		// Body data_stream.* — bodymap mode serializes only body content into
		// the indexed document, so these are needed for Kibana filters
		// (data_stream.dataset:akamai.siem etc.) to match.
		body.PutStr("data_stream.type", r.cfg.DataStream.Type)
		body.PutStr("data_stream.dataset", r.cfg.DataStream.Dataset)
		body.PutStr("data_stream.namespace", r.cfg.DataStream.Namespace)
	}

	return r.consumer.ConsumeLogs(ctx, logs)
}

// getStorageClient retrieves a storage.Client from the configured storage extension.
func getStorageClient(ctx context.Context, host component.Host, id *component.ID, componentID component.ID) (storage.Client, error) {
	if id == nil {
		return nil, fmt.Errorf("storage extension ID is nil")
	}
	ext, ok := host.GetExtensions()[*id]
	if !ok {
		return nil, fmt.Errorf("storage extension %q not found", id)
	}
	se, ok := ext.(storage.Extension)
	if !ok {
		return nil, fmt.Errorf("extension %q is not a storage extension", id)
	}
	return se.GetClient(ctx, component.KindReceiver, componentID, "")
}
