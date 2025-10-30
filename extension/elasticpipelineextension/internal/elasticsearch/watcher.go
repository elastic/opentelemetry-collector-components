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

package elasticsearch

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// ConfigChangeCallback is called when configuration changes are detected.
type ConfigChangeCallback func(ctx context.Context, documents []PipelineDocument) error

// Watcher monitors Elasticsearch for configuration changes.
type Watcher struct {
	fetcher      *Fetcher
	pollInterval time.Duration
	logger       *zap.Logger
	callback     ConfigChangeCallback
	lastConfigs  map[string]int64 // pipeline_id -> version
}

// NewWatcher creates a new configuration watcher.
func NewWatcher(fetcher *Fetcher, pollInterval time.Duration, logger *zap.Logger) *Watcher {
	return &Watcher{
		fetcher:      fetcher,
		pollInterval: pollInterval,
		logger:       logger,
		lastConfigs:  make(map[string]int64),
	}
}

// Start begins monitoring for configuration changes.
func (w *Watcher) Start(ctx context.Context, callback ConfigChangeCallback) error {
	w.callback = callback

	// Initial fetch
	if err := w.checkForChanges(ctx); err != nil {
		w.logger.Error("Initial configuration fetch failed", zap.Error(err))
		return err
	}

	// Start polling
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	w.logger.Info("Started configuration watcher",
		zap.Duration("poll_interval", w.pollInterval))

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Configuration watcher stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := w.checkForChanges(ctx); err != nil {
				w.logger.Error("Failed to check for configuration changes", zap.Error(err))
				// Continue polling despite errors
			}
		}
	}
}

// checkForChanges fetches configurations and detects changes.
func (w *Watcher) checkForChanges(ctx context.Context) error {
	documents, err := w.fetcher.FetchConfigurations(ctx)
	if err != nil {
		return err
	}

	changes := w.detectChanges(documents)
	if len(changes) > 0 {
		w.logger.Info("Detected configuration changes",
			zap.Int("changed_pipelines", len(changes)))

		if w.callback != nil {
			if err := w.callback(ctx, changes); err != nil {
				w.logger.Error("Configuration change callback failed", zap.Error(err))
				return err
			}
		}
	}

	w.updateLastConfigs(documents)
	return nil
}

// detectChanges compares current configurations with last known versions.
func (w *Watcher) detectChanges(documents []PipelineDocument) []PipelineDocument {
	var changes []PipelineDocument

	currentConfigs := make(map[string]int64)

	for _, doc := range documents {
		currentConfigs[doc.PipelineID] = doc.Metadata.Version

		lastVersion, exists := w.lastConfigs[doc.PipelineID]
		if !exists || lastVersion != doc.Metadata.Version {
			changes = append(changes, doc)
			w.logger.Debug("Pipeline configuration changed",
				zap.String("pipeline_id", doc.PipelineID),
				zap.Int64("old_version", lastVersion),
				zap.Int64("new_version", doc.Metadata.Version))
		}
	}

	// Check for removed pipelines
	for pipelineID := range w.lastConfigs {
		if _, exists := currentConfigs[pipelineID]; !exists {
			w.logger.Debug("Pipeline configuration removed",
				zap.String("pipeline_id", pipelineID))

			// Create a tombstone document to indicate removal
			changes = append(changes, PipelineDocument{
				PipelineID: pipelineID,
				Metadata: DocumentMetadata{
					Enabled: false,
					Version: -1, // Special version to indicate removal
				},
			})
		}
	}

	return changes
}

// updateLastConfigs updates the tracking of configuration versions.
func (w *Watcher) updateLastConfigs(documents []PipelineDocument) {
	newConfigs := make(map[string]int64)

	for _, doc := range documents {
		if doc.Metadata.Enabled {
			newConfigs[doc.PipelineID] = doc.Metadata.Version
		}
	}

	w.lastConfigs = newConfigs
}
