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
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/akamaiclient"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/auth"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/cursor"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/mapper"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/poller"
)

// akamaiReceiver polls the Akamai SIEM API and emits logs. The output format
// (raw JSON body map or parsed OTel semantic convention attributes) is
// controlled by cfg.OutputFormat.
type akamaiReceiver struct {
	cfg      *Config
	settings receiver.Settings
	log      *zap.Logger
	consumer consumer.Logs

	cancel          context.CancelFunc
	wg              sync.WaitGroup
	mappingDuration metric.Float64Histogram // nil-safe
	mappingErrors   metric.Int64Counter     // nil-safe
	tracer          trace.Tracer            // nil-safe
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
		cur, err = cursorStore.Load(context.Background())
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
		r.cfg.Endpoint,
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
		r.mappingDuration = tb.AkamaiSiemMappingDuration
		r.mappingErrors = tb.AkamaiSiemMappingErrors
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
		zap.String("endpoint", r.cfg.Endpoint),
		zap.String("config_ids", r.cfg.ConfigIDs),
		zap.Duration("poll_interval", r.cfg.PollInterval),
		zap.String("output_format", r.cfg.OutputFormat),
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

// emitEvents converts raw JSON strings to plog.Logs and sends them to the
// configured consumer based on output_format.
func (r *akamaiReceiver) emitEvents(ctx context.Context, events []string) error {
	switch r.cfg.OutputFormat {
	case "otel":
		return r.emitOTel(ctx, events)
	default:
		return r.emitRaw(ctx, events)
	}
}

// emitRaw sends events as raw JSON body maps. Each log record body is a map
// with key "message" containing the raw Akamai JSON string. The downstream
// pipeline should set elastic.mapping.mode: bodymap via a transform processor
// so the ES exporter serializes the map fields directly into the document.
func (r *akamaiReceiver) emitRaw(ctx context.Context, events []string) error {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	now := pcommon.NewTimestampFromTime(time.Now())

	// Start MapEvents span if tracing is enabled.
	if r.tracer != nil {
		var span trace.Span
		ctx, span = r.tracer.Start(ctx, "akamai_siem.MapEvents",
			trace.WithAttributes(
				attribute.String("output_format", "raw"),
				attribute.Int("event_count", len(events)),
			),
		)
		defer span.End()
	}

	mapStart := time.Now()

	for _, rawJSON := range events {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(now)
		lr.SetObservedTimestamp(now)
		body := lr.Body().SetEmptyMap()
		body.PutStr("message", rawJSON)
	}

	if r.mappingDuration != nil {
		r.mappingDuration.Record(ctx, time.Since(mapStart).Seconds())
	}

	return r.consumer.ConsumeLogs(ctx, logs)
}

// emitOTel parses events and maps them into OTel semantic convention attributes.
func (r *akamaiReceiver) emitOTel(ctx context.Context, events []string) error {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	now := pcommon.NewTimestampFromTime(time.Now())

	// Start MapEvents span if tracing is enabled.
	if r.tracer != nil {
		var span trace.Span
		ctx, span = r.tracer.Start(ctx, "akamai_siem.MapEvents",
			trace.WithAttributes(
				attribute.String("output_format", "otel"),
				attribute.Int("event_count", len(events)),
			),
		)
		defer span.End()
	}

	mapStart := time.Now()

	var mapErrors int
	for _, rawJSON := range events {
		tmp := plog.NewLogRecord()
		tmp.SetObservedTimestamp(now)
		if err := mapper.MapToOTelLog(rawJSON, tmp); err != nil {
			mapErrors++
			if r.mappingErrors != nil {
				r.mappingErrors.Add(ctx, 1)
			}
			r.log.Error("failed to map event to OTel, skipping event",
				zap.Error(err),
				zap.Int("map_errors", mapErrors),
				zap.String("output_format", "otel"),
			)
			continue
		}
		tmp.MoveTo(sl.LogRecords().AppendEmpty())
	}

	if mapErrors > 0 {
		r.log.Warn("events skipped due to field mapping failures",
			zap.Int("skipped", mapErrors),
			zap.Int("total", len(events)),
			zap.Int("emitted", len(events)-mapErrors),
			zap.String("output_format", "otel"),
		)
		if r.tracer != nil {
			span := trace.SpanFromContext(ctx)
			span.SetStatus(codes.Error, fmt.Sprintf("%d/%d events failed mapping", mapErrors, len(events)))
			span.SetAttributes(attribute.Int("mapping_errors", mapErrors))
		}
	}

	if r.mappingDuration != nil {
		r.mappingDuration.Record(ctx, time.Since(mapStart).Seconds())
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
