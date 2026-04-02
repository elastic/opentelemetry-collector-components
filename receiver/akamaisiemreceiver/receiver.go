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
	"sync"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/extension/xextension/storage"

	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/akamaiclient"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/auth"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/cursor"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/mapper"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/poller"
)

// consumerEntry wraps a pipeline consumer. In dual mode, one entry produces raw JSON
// bodies (raw output_format) and the other produces structured OTel attributes (otel output_format).
type consumerEntry struct {
	consumer consumer.Logs
}

// akamaiReceiver polls the Akamai SIEM API and emits logs. Supports single mode
// (one consumer, ECS or OTel) and dual mode (two consumers sharing one poller).
// In dual mode, the sharedcomponent.SharedMap ensures a single API connection.
type akamaiReceiver struct {
	cfg      *Config // primary config (poller settings, endpoint, auth)
	settings receiver.Settings
	log      *zap.Logger

	// Consumers — one or both may be set. When both are set, emitEvents
	// fans out to both in parallel goroutines (dual export mode).
	mu           sync.Mutex
	rawConsumer  *consumerEntry
	otelConsumer *consumerEntry

	cancel          context.CancelFunc
	wg              sync.WaitGroup
	mappingDuration metric.Float64Histogram // nil-safe
	mappingErrors   metric.Int64Counter     // nil-safe
	tracer          trace.Tracer            // nil-safe
}

func newAkamaiReceiver(cfg *Config, settings receiver.Settings) (*akamaiReceiver, error) {
	return &akamaiReceiver{
		cfg:      cfg,
		settings: settings,
		log:      settings.Logger,
	}, nil
}

// setRawConsumer registers a raw-mode consumer on the shared receiver.
func (r *akamaiReceiver) setRawConsumer(cons consumer.Logs) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rawConsumer = &consumerEntry{consumer: cons}
}

// setOTelConsumer registers an OTel-mode consumer on the shared receiver.
func (r *akamaiReceiver) setOTelConsumer(cons consumer.Logs) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.otelConsumer = &consumerEntry{consumer: cons}
}

// isDualMode returns true when both raw and OTel consumers are registered.
func (r *akamaiReceiver) isDualMode() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rawConsumer != nil && r.otelConsumer != nil
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

	// Wrap transport with EdgeGrid signing.
	signer := auth.NewEdgeGridSigner(
		string(r.cfg.Authentication.ClientToken),
		string(r.cfg.Authentication.ClientSecret),
		string(r.cfg.Authentication.AccessToken),
	)
	httpClient.Transport = &auth.Transport{
		Base:   httpClient.Transport,
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

			EventsPerPage:      tb.AkamaiSiemEventsPerPage,
		}
	}

	poll := poller.NewPoller(client, cursorStore, cur, pollerCfg, r.emitEvents, r.log, tel)

	pollCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	mode := "single"
	if r.isDualMode() {
		mode = "dual"
	}

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
		zap.String("mode", mode),
		zap.Int("event_limit", r.cfg.EventLimit),
		zap.String("storage", storageInfo),
	)

	return nil
}

// Shutdown implements receiver.Logs.
func (r *akamaiReceiver) Shutdown(_ context.Context) error {
	r.log.Info("akamai SIEM receiver shutting down")
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	r.log.Info("akamai SIEM receiver stopped")
	return nil
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
// OTel pipeline(s). In dual mode, both raw and OTel formatting run in parallel
// and the cursor advances only when both succeed (slowest-wins).
func (r *akamaiReceiver) emitEvents(ctx context.Context, events []string) error {
	r.mu.Lock()
	raw := r.rawConsumer
	otel := r.otelConsumer
	r.mu.Unlock()

	// Single consumer mode — no goroutine overhead.
	if raw != nil && otel == nil {
		return r.emitRaw(ctx, events, raw)
	}
	if otel != nil && raw == nil {
		return r.emitOTel(ctx, events, otel)
	}
	if raw == nil && otel == nil {
		return fmt.Errorf("no consumers registered")
	}

	// Dual mode — parallel formatting + emission, slowest-wins.
	var wg sync.WaitGroup
	var rawErr, otelErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		rawErr = r.emitRaw(ctx, events, raw)
	}()
	go func() {
		defer wg.Done()
		otelErr = r.emitOTel(ctx, events, otel)
	}()
	wg.Wait()

	if rawErr != nil {
		return fmt.Errorf("raw consumer: %w", rawErr)
	}
	if otelErr != nil {
		return fmt.Errorf("OTel consumer: %w", otelErr)
	}
	return nil
}

// emitRaw sends events as raw JSON body strings. The ES ingest pipeline handles all
// field parsing and ECS enrichment downstream. Injects x-elastic-mapping-mode: ecs
// into the OTel client context so the elasticsearchexporter serializes
// LogRecord.Body → "message" field (not "body.text") without requiring explicit
// mapping.mode config on the exporter.
func (r *akamaiReceiver) emitRaw(ctx context.Context, events []string, entry *consumerEntry) error {
	ctx = withMappingMode(ctx, "ecs")

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
		lr.Body().SetStr(rawJSON)
	}

	if r.mappingDuration != nil {
		r.mappingDuration.Record(ctx, time.Since(mapStart).Seconds())
	}

	return entry.consumer.ConsumeLogs(ctx, logs)
}

// emitOTel parses events and maps them into OTel semantic convention attributes.
func (r *akamaiReceiver) emitOTel(ctx context.Context, events []string, entry *consumerEntry) error {
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

	return entry.consumer.ConsumeLogs(ctx, logs)
}

// withMappingMode injects x-elastic-mapping-mode into the OTel client context
// metadata. The downstream elasticsearchexporter reads this to serialize
// LogRecord.Body into the "message" field (ECS) rather than "body.text" (OTel default),
// without requiring explicit mapping.mode config on the exporter.
// This follows the same pattern as elasticapmintakereceiver.
func withMappingMode(ctx context.Context, mode string) context.Context {
	info := client.FromContext(ctx)
	newMeta := make(map[string][]string)
	for k := range info.Metadata.Keys() {
		newMeta[k] = info.Metadata.Get(k)
	}
	newMeta["x-elastic-mapping-mode"] = []string{mode}
	return client.NewContext(ctx, client.Info{
		Addr:     info.Addr,
		Auth:     info.Auth,
		Metadata: client.NewMetadata(newMeta),
	})
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
