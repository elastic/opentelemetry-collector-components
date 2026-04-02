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

package poller // import "github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/poller"

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/akamaiclient"
	"github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/cursor"
)

// Telemetry provides optional metric and tracing instruments for the poller.
// All fields are optional — nil values are silently skipped.
type Telemetry struct {
	Tracer             trace.Tracer
	Requests           metric.Int64Counter
	RequestErrors      metric.Int64Counter
	EventsReceived     metric.Int64Counter
	EventsEmitted      metric.Int64Counter
	OffsetExpired      metric.Int64Counter
	OffsetTTLDrops     metric.Int64Counter
	RecoveryAttempts   metric.Int64Counter
	InvalidTSRetries   metric.Int64Counter
	RequestDuration    metric.Float64Histogram
	PollDuration       metric.Float64Histogram
	EventsPerSecond    metric.Float64Histogram
	PagesProcessed     metric.Int64Counter
	CursorPersists     metric.Int64Counter
	BytesReceived      metric.Int64Counter
	PageProcessingTime metric.Float64Histogram
	EventsPerPage      metric.Int64Histogram
}

func (t *Telemetry) addCounter(ctx context.Context, c metric.Int64Counter, v int64) {
	if t != nil && c != nil {
		c.Add(ctx, v)
	}
}

func (t *Telemetry) recordFloat(ctx context.Context, h metric.Float64Histogram, v float64) {
	if t != nil && h != nil {
		h.Record(ctx, v)
	}
}

func (t *Telemetry) recordInt(ctx context.Context, h metric.Int64Histogram, v int64) {
	if t != nil && h != nil {
		h.Record(ctx, v)
	}
}

// startSpan starts a trace span if a tracer is configured. Returns ctx and a
// nil-safe end function. When no tracer is set, this is a no-op.
func (t *Telemetry) startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	if t == nil || t.Tracer == nil {
		return ctx, func(error) {}
	}
	ctx, span := t.Tracer.Start(ctx, name, trace.WithAttributes(attrs...))
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

const (
	chainOverlap    = 10 * time.Second
	maxLookback     = 12 * time.Hour
	apiSafetyBuffer = 60 // seconds subtracted from "now" for the `to` parameter
)

// PollerConfig holds the parameters that drive the polling state machine.
type PollerConfig struct {
	EventLimit              int
	InitialLookback         time.Duration
	OffsetTTL               time.Duration
	MaxRecoveryAttempts     int
	InvalidTimestampRetries int
	BatchSize               int // events per ConsumeLogs call (default 1000)
	StreamBufferSize        int // bounded channel capacity (default 4)
}

// EventEmitter is called by the poller to emit a batch of raw JSON events.
// The implementation is responsible for converting them to plog.Logs and
// calling ConsumeLogs.
type EventEmitter func(ctx context.Context, events []string) error

// Poller implements the three-branch chain state machine for the Akamai SIEM API.
type Poller struct {
	client      *akamaiclient.Client
	cursor      cursor.Cursor
	cursorStore *cursor.CursorStore
	cfg         PollerConfig
	emit        EventEmitter
	log         *zap.Logger
	telemetry   *Telemetry
}

// NewPoller creates a new poller.
func NewPoller(client *akamaiclient.Client, cursorStore *cursor.CursorStore, cur cursor.Cursor, cfg PollerConfig, emit EventEmitter, log *zap.Logger, telemetry *Telemetry) *Poller {
	return &Poller{
		client:      client,
		cursor:      cur,
		cursorStore: cursorStore,
		cfg:         cfg,
		emit:        emit,
		log:         log.Named("poller"),
		telemetry:   telemetry,
	}
}

// Poll performs a single polling iteration, fetching pages until the chain
// is drained (events < event_limit).
func (p *Poller) Poll(ctx context.Context) error {
	ctx, endSpan := p.telemetry.startSpan(ctx, "akamai_siem.Poll",
		attribute.Bool("cursor.caught_up", p.cursor.CaughtUp),
		attribute.String("cursor.last_offset", p.cursor.LastOffset),
	)
	start := time.Now()
	p.log.Debug("starting poll iteration",
		zap.Int64("chain_from", p.cursor.ChainFrom),
		zap.Int64("chain_to", p.cursor.ChainTo),
		zap.Bool("caught_up", p.cursor.CaughtUp),
		zap.String("last_offset", p.cursor.LastOffset),
	)

	params := p.buildFetchParams()
	pageCount := 0
	pollEventsTotal := 0
	recoveryAttempts := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fetchCtx, endFetchSpan := p.telemetry.startSpan(ctx, "akamai_siem.FetchPage",
			attribute.String("mode", params.FetchMode()),
			attribute.Int("limit", params.Limit),
		)
		body, fetchErr := p.fetchWithTimestampRetry(fetchCtx, params)
		if fetchErr != nil {
			endFetchSpan(fetchErr)
			p.telemetry.addCounter(ctx, p.telemetry.RequestErrors, 1)
			if !p.handleFetchError(ctx, fetchErr, &params) {
				return nil
			}
			p.telemetry.addCounter(ctx, p.telemetry.RecoveryAttempts, 1)
			recoveryAttempts++
			if p.cfg.MaxRecoveryAttempts > 0 && recoveryAttempts >= p.cfg.MaxRecoveryAttempts {
				p.log.Error("max recovery attempts reached, ending poll cycle",
					zap.Int("recovery_attempts", recoveryAttempts),
					zap.Error(fetchErr),
				)
				return nil
			}
			continue
		}
		endFetchSpan(nil)
		recoveryAttempts = 0
		pageCount++

		eventCount, pageCtx, processErr := p.processPage(ctx, body, pageCount, params)
		_ = body.Close()
		if processErr != nil {
			return nil
		}
		pollEventsTotal += eventCount

		if eventCount == 0 {
			p.cursor.CaughtUp = true
			p.log.Debug("no events received, chain drained")
			p.persistCursor(ctx)
			break
		}

		// Update cursor with page offset.
		if pageCtx.Offset != "" {
			p.cursor.LastOffset = pageCtx.Offset
			p.cursor.OffsetObtainedAt = time.Now()
		}

		// Drain detection.
		p.cursor.CaughtUp = eventCount < p.cfg.EventLimit
		p.persistCursor(ctx)

		if p.cursor.CaughtUp {
			p.log.Debug("chain drained",
				zap.Int("events", eventCount),
				zap.Int("limit", p.cfg.EventLimit),
			)
			break
		}

		if pageCtx.Offset == "" {
			p.log.Error("missing next offset in paginated response; ending cycle")
			break
		}

		// Continue draining with next page.
		params.Offset = pageCtx.Offset
		params.From = 0
		params.To = 0
	}

	elapsed := time.Since(start)
	p.telemetry.recordFloat(ctx, p.telemetry.PollDuration, elapsed.Seconds())

	// EPS: events per second for this poll cycle.
	if elapsed.Seconds() > 0 && pollEventsTotal > 0 {
		eps := float64(pollEventsTotal) / elapsed.Seconds()
		p.telemetry.recordFloat(ctx, p.telemetry.EventsPerSecond, eps)
	}

	p.log.Debug("poll iteration complete",
		zap.Duration("duration", elapsed),
		zap.Int("pages", pageCount),
		zap.Int("events", pollEventsTotal),
		zap.Bool("caught_up", p.cursor.CaughtUp),
	)
	endSpan(nil)
	return nil
}

func (p *Poller) persistCursor(ctx context.Context) {
	if p.cursorStore == nil {
		return
	}
	_, endSpan := p.telemetry.startSpan(ctx, "akamai_siem.PersistCursor",
		attribute.String("last_offset", p.cursor.LastOffset),
		attribute.Bool("caught_up", p.cursor.CaughtUp),
	)
	err := p.cursorStore.Save(ctx, p.cursor)
	endSpan(err)
	if err != nil {
		p.log.Error("failed to persist cursor", zap.Error(err))
	} else {
		p.telemetry.addCounter(ctx, p.telemetry.CursorPersists, 1)
		p.log.Debug("cursor persisted",
			zap.Int64("chain_from", p.cursor.ChainFrom),
			zap.Int64("chain_to", p.cursor.ChainTo),
			zap.String("last_offset", p.cursor.LastOffset),
			zap.Bool("caught_up", p.cursor.CaughtUp),
		)
	}
}

// processPage streams events from body through a bounded channel. A consumer
// goroutine reads from the channel, batches events, and calls the EventEmitter
// per batch. This bounds memory to (streamBufferSize + batchSize) events
// regardless of total page size.
//
// Cursor is NOT persisted here — the caller handles that after processPage
// returns, matching the Beats pattern where cursor persist happens after all
// events in the page are confirmed.
func (p *Poller) processPage(ctx context.Context, body interface{ Read([]byte) (int, error) }, page int, params akamaiclient.FetchParams) (int, akamaiclient.OffsetContext, error) {
	_, endSpan := p.telemetry.startSpan(ctx, "akamai_siem.ProcessPage",
		attribute.Int("page", page),
	)
	pageStart := time.Now()

	bufSize := p.cfg.StreamBufferSize
	if bufSize <= 0 {
		bufSize = 4
	}
	batchSize := p.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	eventCh := make(chan string, bufSize)

	// Scanner goroutine: streams NDJSON lines into bounded channel.
	// Closes eventCh when done so the consumer's range loop exits.
	var pageCtx akamaiclient.OffsetContext
	var streamCount int
	var streamErr error
	go func() {
		defer close(eventCh)
		pageCtx, streamCount, streamErr = akamaiclient.StreamEvents(ctx, body, eventCh)
	}()

	// Consumer: reads from channel, batches, calls emit per batch.
	// Runs on the calling goroutine — range exits when scanner closes eventCh.
	var emitErr error
	var emittedCount atomic.Int64
	batch := make([]string, 0, batchSize)

	for event := range eventCh {
		batch = append(batch, event)
		if len(batch) >= batchSize {
			if err := p.emit(ctx, batch); err != nil {
				emitErr = err
				for range eventCh {
				}
				break
			}
			emittedCount.Add(int64(len(batch)))
			batch = make([]string, 0, batchSize)
		}
	}
	if emitErr == nil && len(batch) > 0 {
		if err := p.emit(ctx, batch); err != nil {
			emitErr = err
		} else {
			emittedCount.Add(int64(len(batch)))
		}
	}

	totalEmitted := int(emittedCount.Load())

	// Handle errors.
	if streamErr != nil {
		endSpan(streamErr)
		p.log.Error("failed to stream events",
			zap.Error(streamErr),
			zap.Int("page", page),
			zap.String("mode", params.FetchMode()),
		)
		return 0, akamaiclient.OffsetContext{}, streamErr
	}
	if emitErr != nil {
		endSpan(emitErr)
		p.log.Error("failed to emit events",
			zap.Error(emitErr),
			zap.Int("events_emitted", totalEmitted),
			zap.Int("events_received", streamCount),
			zap.Int("page", page),
		)
		return 0, akamaiclient.OffsetContext{}, emitErr
	}

	// Telemetry.
	p.telemetry.addCounter(ctx, p.telemetry.EventsReceived, int64(streamCount))
	p.telemetry.addCounter(ctx, p.telemetry.PagesProcessed, 1)
	if cl := p.client.LastContentLength(); cl > 0 {
		p.telemetry.addCounter(ctx, p.telemetry.BytesReceived, cl)
	}
	p.telemetry.addCounter(ctx, p.telemetry.EventsEmitted, int64(totalEmitted))
	p.telemetry.recordInt(ctx, p.telemetry.EventsPerPage, int64(streamCount))
	pageElapsed := time.Since(pageStart)
	p.telemetry.recordFloat(ctx, p.telemetry.PageProcessingTime, pageElapsed.Seconds())
	endSpan(nil)

	p.log.Debug("page processed",
		zap.Int("page", page),
		zap.Int("events_received", streamCount),
		zap.Int("events_emitted", totalEmitted),
		zap.Duration("processing_time", pageElapsed),
		zap.String("offset", pageCtx.Offset),
	)

	return streamCount, pageCtx, nil
}

// buildFetchParams implements the three-branch chain state machine.
func (p *Poller) buildFetchParams() akamaiclient.FetchParams {
	now := time.Now().Unix()
	params := akamaiclient.FetchParams{Limit: p.cfg.EventLimit}

	switch {
	case !p.cursor.CaughtUp && p.cursor.LastOffset != "" && !p.cursor.IsOffsetStale(p.cfg.OffsetTTL):
		// Branch 1: Chain in progress, offset valid — continue draining.
		params.Offset = p.cursor.LastOffset
		p.log.Debug("offset-based fetch (chain draining)",
			zap.String("offset", params.Offset),
		)

	case !p.cursor.CaughtUp && p.cursor.ChainFrom != 0:
		// Branch 2: Chain in progress but offset gone/stale — replay chain window.
		if p.cursor.IsOffsetStale(p.cfg.OffsetTTL) {
			p.log.Warn("offset stale, replaying chain window",
				zap.Duration("offset_age", time.Since(p.cursor.OffsetObtainedAt)),
				zap.Duration("ttl", p.cfg.OffsetTTL),
			)
			p.telemetry.addCounter(context.Background(), p.telemetry.OffsetTTLDrops, 1)
		}
		p.cursor.ClearOffset()

		from := p.cursor.ChainFrom - int64(chainOverlap.Seconds())
		earliest := now - int64(maxLookback.Seconds())
		if from < earliest {
			p.log.Warn("chain_from clamped to max lookback",
				zap.Int64("original_from", p.cursor.ChainFrom),
				zap.Int64("clamped_from", earliest),
			)
			from = earliest
		}
		params.From = from
		params.To = p.cursor.ChainTo
		p.log.Debug("time-based fetch (chain replay)",
			zap.Int64("from", params.From),
			zap.Int64("to", params.To),
		)

	default:
		// Branch 3: Caught up or first run — start a new chain.
		var from int64
		if p.cursor.ChainTo != 0 {
			from = p.cursor.ChainTo - int64(chainOverlap.Seconds())
		} else {
			from = now - int64(p.cfg.InitialLookback.Seconds())
		}
		earliest := now - int64(maxLookback.Seconds())
		if from < earliest {
			from = earliest
		}
		to := now - apiSafetyBuffer

		p.cursor.ChainFrom = from
		p.cursor.ChainTo = to
		p.cursor.CaughtUp = false
		p.cursor.ClearOffset()

		params.From = from
		params.To = to
		p.log.Debug("time-based fetch (new chain)",
			zap.Int64("from", params.From),
			zap.Int64("to", params.To),
		)
	}

	return params
}

func (p *Poller) fetchWithTimestampRetry(ctx context.Context, params akamaiclient.FetchParams) (*readCloserWrapper, error) {
	maxRetries := p.cfg.InvalidTimestampRetries
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			p.telemetry.addCounter(ctx, p.telemetry.InvalidTSRetries, 1)
			p.log.Debug("retrying request after invalid timestamp",
				zap.Int("attempt", attempt),
			)
		}

		p.telemetry.addCounter(ctx, p.telemetry.Requests, 1)
		reqStart := time.Now()
		body, err := p.client.FetchResponse(ctx, params)
		p.telemetry.recordFloat(ctx, p.telemetry.RequestDuration, time.Since(reqStart).Seconds())
		if err == nil {
			return &readCloserWrapper{body}, nil
		}
		lastErr = err

		var apiErr *akamaiclient.APIError
		if errors.As(err, &apiErr) && apiErr.IsInvalidTimestamp() && attempt < maxRetries {
			continue
		}
		return nil, err
	}
	return nil, lastErr
}

// readCloserWrapper wraps io.ReadCloser to satisfy the interface returned
// by fetchWithTimestampRetry.
type readCloserWrapper struct {
	rc interface {
		Read(p []byte) (n int, err error)
		Close() error
	}
}

func (w *readCloserWrapper) Read(p []byte) (int, error) { return w.rc.Read(p) }
func (w *readCloserWrapper) Close() error               { return w.rc.Close() }

// handleFetchError processes API errors. Returns true if recoverable.
func (p *Poller) handleFetchError(ctx context.Context, err error, params *akamaiclient.FetchParams) bool {
	var apiErr *akamaiclient.APIError
	if !errors.As(err, &apiErr) {
		p.log.Error("failed to fetch events", zap.Error(err))
		return false
	}

	switch {
	case apiErr.IsOffsetOutOfRange():
		p.log.Warn("416 offset expired; clearing offset for chain replay",
			zap.String("last_offset", p.cursor.LastOffset),
		)
		p.telemetry.addCounter(ctx, p.telemetry.OffsetExpired, 1)
		p.cursor.ClearOffset()
		*params = p.buildFetchParams()
		return true

	case apiErr.IsInvalidTimestamp():
		p.log.Warn("invalid timestamp after retries; clearing offset for chain replay")
		p.cursor.ClearOffset()
		*params = p.buildFetchParams()
		return true

	case apiErr.IsFromTooOld():
		p.log.Warn("from timestamp too old, replaying with clamp")
		*params = p.buildFetchParams()
		return true

	case apiErr.StatusCode == 400:
		p.log.Error("non-recoverable 400 response",
			zap.Int("status_code", apiErr.StatusCode),
			zap.String("detail", apiErr.Detail),
		)
		return false

	default:
		p.log.Error("failed to fetch events",
			zap.Int("status_code", apiErr.StatusCode),
			zap.String("detail", apiErr.Detail),
		)
		return false
	}
}
