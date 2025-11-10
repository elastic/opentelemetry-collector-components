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

package elasticpipelineextension

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	esclient "github.com/elastic/opentelemetry-collector-components/extension/elasticpipelineextension/internal/elasticsearch"
	"github.com/elastic/opentelemetry-collector-components/extension/elasticpipelineextension/internal/pipeline"
)

// elasticPipelineExtension implements the main extension logic.
type elasticPipelineExtension struct {
	config            *Config
	telemetrySettings component.TelemetrySettings
	logger            *zap.Logger

	// Elasticsearch components
	esClient *esclient.Client
	fetcher  *esclient.Fetcher
	watcher  *esclient.Watcher

	// Pipeline management
	pipelineManager *pipeline.Manager

	// Processor configuration management
	processorConfigs  map[string]map[string]interface{}         // key -> config
	processorWatchers map[string][]func(map[string]interface{}) // key -> callbacks
	processorMu       sync.RWMutex

	// Health reporting
	healthReporter *healthReporter

	// Lifecycle management
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
	mu      sync.Mutex
}

// healthReporter handles reporting pipeline health back to Elasticsearch.
type healthReporter struct {
	client   *esclient.Client
	manager  *pipeline.Manager
	interval time.Duration
	logger   *zap.Logger
	enabled  bool
}

var _ extension.Extension = (*elasticPipelineExtension)(nil)

// newElasticPipelineExtension creates a new instance of the extension.
func newElasticPipelineExtension(config *Config, set extension.Settings) *elasticPipelineExtension {
	return &elasticPipelineExtension{
		config:            config,
		telemetrySettings: set.TelemetrySettings,
		logger:            set.TelemetrySettings.Logger,
		processorConfigs:  make(map[string]map[string]interface{}),
		processorWatchers: make(map[string][]func(map[string]interface{})),
	}
}

// Start implements extension.Extension.
func (e *elasticPipelineExtension) Start(ctx context.Context, host component.Host) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return fmt.Errorf("extension already started")
	}

	e.ctx, e.cancel = context.WithCancel(ctx)

	// Initialize Elasticsearch client
	if err := e.initializeElasticsearchClient(ctx, host); err != nil {
		return fmt.Errorf("failed to initialize Elasticsearch client: %w", err)
	}

	// Initialize pipeline manager
	e.pipelineManager = pipeline.NewManager(
		e.config.PipelineManagement.Namespace,
		e.config.PipelineManagement.MaxPipelines,
		e.config.PipelineManagement.MaxComponentsPerPipeline,
		e.config.PipelineManagement.ValidateConfigs,
		e.config.PipelineManagement.DryRunMode,
		e.config.PipelineManagement.StartupTimeout,
		e.config.PipelineManagement.ShutdownTimeout,
		e.logger,
	)
	e.pipelineManager.SetHost(host)

	// Initialize configuration fetcher
	var err error
	filters := make([]esclient.FilterConfig, len(e.config.Watcher.Filters))
	for i, f := range e.config.Watcher.Filters {
		filters[i] = esclient.FilterConfig{
			Field: f.Field,
			Value: f.Value,
		}
	}

	e.fetcher, err = esclient.NewFetcher(
		e.esClient,
		e.config.Watcher.CacheDuration,
		filters,
		e.logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create configuration fetcher: %w", err)
	}

	// Initialize configuration watcher
	e.watcher = esclient.NewWatcher(
		e.fetcher,
		e.config.Watcher.PollInterval,
		e.logger,
	)

	// Initialize health reporter if enabled
	if e.config.PipelineManagement.EnableHealthReporting {
		e.healthReporter = &healthReporter{
			client:   e.esClient,
			manager:  e.pipelineManager,
			interval: e.config.PipelineManagement.HealthReportInterval,
			logger:   e.logger,
			enabled:  true,
		}
	}

	// Start configuration watcher
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		if err := e.watcher.Start(e.ctx, e.onConfigurationChange); err != nil && err != context.Canceled {
			e.logger.Error("Configuration watcher failed", zap.Error(err))
		}
	}()

	// Start health reporter if enabled
	if e.healthReporter != nil && e.healthReporter.enabled {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			e.healthReporter.start(e.ctx)
		}()
	}

	e.started = true
	e.logger.Info("Elastic pipeline extension started successfully")
	return nil
}

// Shutdown implements extension.Extension.
func (e *elasticPipelineExtension) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.started {
		return nil
	}

	e.logger.Info("Shutting down elastic pipeline extension")

	// Cancel context to stop all goroutines
	if e.cancel != nil {
		e.cancel()
	}

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		e.logger.Info("All goroutines stopped")
	case <-ctx.Done():
		e.logger.Warn("Shutdown timeout, some goroutines may still be running")
	}

	e.started = false
	e.logger.Info("Elastic pipeline extension shut down successfully")
	return nil
}

// initializeElasticsearchClient creates and configures the Elasticsearch client.
func (e *elasticPipelineExtension) initializeElasticsearchClient(ctx context.Context, host component.Host) error {
	if e.config.Source.Elasticsearch == nil {
		return fmt.Errorf("elasticsearch configuration is required")
	}

	// Create Elasticsearch client configuration
	cfg := elasticsearch.Config{
		Addresses: []string{e.config.Source.Elasticsearch.Endpoint},
	}

	// Add Basic Auth if credentials are provided
	if e.config.Source.Elasticsearch.Username != "" {
		cfg.Username = e.config.Source.Elasticsearch.Username
		cfg.Password = e.config.Source.Elasticsearch.Password
	}

	// Create Elasticsearch client
	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}

	// Wrap with our pipeline-specific client
	e.esClient = esclient.NewClient(
		esClient,
		e.config.Source.Elasticsearch.Index,
		e.logger,
	)

	// Test the connection
	res, err := esClient.Ping()
	if err != nil {
		return fmt.Errorf("failed to ping Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch ping failed: %s", res.Status())
	}

	e.logger.Info("Successfully connected to Elasticsearch",
		zap.String("index", e.config.Source.Elasticsearch.Index))

	return nil
}

// onConfigurationChange handles configuration changes detected by the watcher.
func (e *elasticPipelineExtension) onConfigurationChange(ctx context.Context, documents []esclient.PipelineDocument) error {
	e.logger.Info("Processing configuration changes", zap.Int("document_count", len(documents)))

	// Extract processor configurations from all documents
	// These will be consumed by dynamic processors (e.g., elasticdynamictransform)
	// that implement the ProcessorConfigProvider interface
	e.extractProcessorConfigs(documents)

	// Note: We no longer apply pipeline configurations via pipelineManager.
	// The processor configurations extracted above are consumed directly by
	// dynamic processors that watch for config changes.

	return nil
}

// start begins the health reporting routine.
func (hr *healthReporter) start(ctx context.Context) {
	if !hr.enabled {
		return
	}

	ticker := time.NewTicker(hr.interval)
	defer ticker.Stop()

	hr.logger.Info("Started health reporter", zap.Duration("interval", hr.interval))

	for {
		select {
		case <-ctx.Done():
			hr.logger.Info("Health reporter stopped")
			return
		case <-ticker.C:
			hr.reportHealth(ctx)
		}
	}
}

// reportHealth reports the health status of all managed pipelines.
func (hr *healthReporter) reportHealth(ctx context.Context) {
	healthStatuses := hr.manager.GetAllPipelineHealth()

	for pipelineID, health := range healthStatuses {
		if err := hr.client.UpdatePipelineHealth(ctx, pipelineID, *health); err != nil {
			hr.logger.Error("Failed to report pipeline health",
				zap.String("pipeline_id", pipelineID),
				zap.Error(err))
		} else {
			hr.logger.Debug("Reported pipeline health",
				zap.String("pipeline_id", pipelineID),
				zap.String("status", health.Status))
		}
	}
}

// GetProcessorConfig implements ProcessorConfigProvider interface.
// Returns the configuration for a specific processor by key.
func (e *elasticPipelineExtension) GetProcessorConfig(ctx context.Context, key string) (map[string]interface{}, error) {
	e.processorMu.RLock()
	defer e.processorMu.RUnlock()

	config, ok := e.processorConfigs[key]
	if !ok {
		return nil, fmt.Errorf("processor config not found for key: %s", key)
	}

	e.logger.Debug("Retrieved processor config", zap.String("key", key))
	return config, nil
}

// WatchProcessorConfig implements ProcessorConfigProvider interface.
// Registers a callback to be notified when the processor config changes.
// Returns a cancel function to stop watching.
func (e *elasticPipelineExtension) WatchProcessorConfig(key string, callback func(config map[string]interface{})) (cancel func()) {
	e.processorMu.Lock()
	defer e.processorMu.Unlock()

	// Add callback to watchers
	e.processorWatchers[key] = append(e.processorWatchers[key], callback)

	e.logger.Debug("Registered processor config watcher", zap.String("key", key))

	// Return cancel function
	return func() {
		e.processorMu.Lock()
		defer e.processorMu.Unlock()

		watchers := e.processorWatchers[key]
		for i, cb := range watchers {
			// Compare function pointers (this is a best-effort approach)
			if &cb == &callback {
				e.processorWatchers[key] = append(watchers[:i], watchers[i+1:]...)
				e.logger.Debug("Unregistered processor config watcher", zap.String("key", key))
				break
			}
		}
	}
}

// extractProcessorConfigs extracts processor configurations from pipeline documents
// and stores them for retrieval by dynamic processors.
func (e *elasticPipelineExtension) extractProcessorConfigs(documents []esclient.PipelineDocument) {
	e.processorMu.Lock()
	defer e.processorMu.Unlock()

	// Build new processor configs map
	newConfigs := make(map[string]map[string]interface{})

	for _, doc := range documents {
		if doc.Config.Processors == nil {
			continue
		}

		// Store each processor config with its full key (e.g., "transform/myprocessor")
		for processorKey, processorConfig := range doc.Config.Processors {
			if configMap, ok := processorConfig.(map[string]interface{}); ok {
				newConfigs[processorKey] = configMap
				e.logger.Debug("Extracted processor config",
					zap.String("key", processorKey),
					zap.String("pipeline_id", doc.PipelineID))
			}
		}
	}

	// Detect changes and notify watchers
	for key, newConfig := range newConfigs {
		oldConfig, existed := e.processorConfigs[key]

		// Update config
		e.processorConfigs[key] = newConfig

		// Notify watchers if config changed or is new
		if !existed || !configsEqual(oldConfig, newConfig) {
			e.logger.Info("Processor config changed, notifying watchers",
				zap.String("key", key),
				zap.Int("watchers", len(e.processorWatchers[key])))

			for _, callback := range e.processorWatchers[key] {
				// Call in goroutine to avoid blocking
				go callback(newConfig)
			}
		}
	}

	// Remove configs that no longer exist
	for key := range e.processorConfigs {
		if _, exists := newConfigs[key]; !exists {
			delete(e.processorConfigs, key)
			e.logger.Info("Processor config removed", zap.String("key", key))
		}
	}
}

// configsEqual does a simple comparison of two config maps.
// This is a basic implementation - could be enhanced for deep comparison.
func configsEqual(a, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if bv, ok := b[k]; !ok || fmt.Sprintf("%v", v) != fmt.Sprintf("%v", bv) {
			return false
		}
	}

	return true
}
