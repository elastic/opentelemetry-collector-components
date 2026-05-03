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

package entityanalyticsreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/entityanalyticsreceiver"

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"

	"github.com/elastic/entcollect"
	"github.com/elastic/opentelemetry-collector-components/receiver/entityanalyticsreceiver/internal/metadata"
)

var _ component.Component = (*entityAnalyticsReceiver)(nil)

type entityAnalyticsReceiver struct {
	cfg          *Config
	consumer     consumer.Logs
	logger       *zap.Logger
	buildVersion string
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

func newReceiver(params receiver.Settings, cfg *Config, cons consumer.Logs) *entityAnalyticsReceiver {
	return &entityAnalyticsReceiver{
		cfg:          cfg,
		consumer:     cons,
		logger:       params.Logger,
		buildVersion: params.BuildInfo.Version,
	}
}

// Start resolves the storage extension, opens a named store for the
// configured provider, and spawns the sync loop goroutine.
//
// Consistency: after a successful sync the receiver commits provider
// state (buffer.Commit) based on ConsumeLogs returning nil. That
// means the pipeline accepted the batch, not necessarily that events
// are durably written to Elasticsearch. With the default agentless
// pipeline (synchronous ES exporter, no batch processor) acceptance
// and durable write coincide. Async exporters or batch processors
// decouple the two: state may advance before export completes,
// creating a replay window bounded by document idempotency
// (deterministic _id) and the full sync interval.
func (r *entityAnalyticsReceiver) Start(_ context.Context, host component.Host) error {
	registry, err := resolveRegistry(host, r.cfg.StorageID)
	if err != nil {
		return fmt.Errorf("resolving storage registry: %w", err)
	}
	store, err := registry.Store("entity_analytics." + r.cfg.Provider)
	if err != nil {
		return fmt.Errorf("opening store: %w", err)
	}

	factory, ok := Get(r.cfg.Provider)
	if !ok {
		return fmt.Errorf("unknown provider %q", r.cfg.Provider)
	}
	provider, err := factory(nil)
	if err != nil {
		return fmt.Errorf("creating provider %q: %w", r.cfg.Provider, err)
	}

	slogger := slog.New(zapslog.NewHandler(r.logger.Core()))

	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.wg.Add(1)
	go r.run(ctx, store, provider, slogger)
	return nil
}

func (r *entityAnalyticsReceiver) Shutdown(_ context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	return nil
}

// run drives the sync loop with a single ticker at UpdateInterval.
// At each tick, a full sync is performed if the last successful full
// sync is older than SyncInterval (or has never happened); otherwise
// an incremental sync runs. This is self-healing: a failed full sync
// leaves lastFullSyncAt at its zero value, so every subsequent tick
// retries a full sync until one succeeds. Note that under persistent
// full-sync failure, retries occur at UpdateInterval cadence, not at
// SyncInterval.
func (r *entityAnalyticsReceiver) run(ctx context.Context, store entcollect.Store, provider entcollect.Provider, log *slog.Logger) {
	defer r.wg.Done()

	pub := newPublisher(r.consumer, r.cfg.Provider, metadata.ScopeName, r.buildVersion)

	var lastFullSyncAt time.Time
	if r.doSync(ctx, store, provider, pub, log, true) {
		lastFullSyncAt = time.Now()
	}

	ticker := time.NewTicker(r.cfg.UpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			full := lastFullSyncAt.IsZero() || time.Since(lastFullSyncAt) >= r.cfg.SyncInterval
			if r.doSync(ctx, store, provider, pub, log, full) && full {
				lastFullSyncAt = time.Now()
			}
		}
	}
}

// doSync runs one sync (full or incremental) and commits the buffer
// on success. It returns true if both the sync and the commit
// succeeded, false otherwise.
func (r *entityAnalyticsReceiver) doSync(ctx context.Context, store entcollect.Store, provider entcollect.Provider, pub entcollect.Publisher, log *slog.Logger, full bool) bool {
	kind := "full"
	if !full {
		kind = "incremental"
	}
	r.logger.Info("starting sync", zap.String("kind", kind))

	buf := entcollect.NewBuffer(store)
	var err error
	if full {
		err = provider.FullSync(ctx, buf, pub, log)
	} else {
		err = provider.IncrementalSync(ctx, buf, pub, log)
	}
	if err != nil {
		buf.Discard()
		r.logger.Error("sync failed", zap.String("kind", kind), zap.Error(err))
		return false
	}
	if err := buf.Commit(); err != nil {
		r.logger.Error("committing sync state", zap.String("kind", kind), zap.Error(err))
		return false
	}
	r.logger.Info("sync complete", zap.String("kind", kind))
	return true
}
