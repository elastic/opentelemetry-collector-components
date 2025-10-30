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

	// Create Elasticsearch client using direct configuration
	// For now, we'll create a basic client - this should be enhanced to use configelasticsearch
	esClient, err := elasticsearch.NewDefaultClient()
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

	for _, doc := range documents {
		if err := e.pipelineManager.ApplyConfiguration(ctx, doc); err != nil {
			e.logger.Error("Failed to apply pipeline configuration",
				zap.String("pipeline_id", doc.PipelineID),
				zap.Error(err))
			// Continue processing other documents
		} else {
			e.logger.Info("Successfully applied pipeline configuration",
				zap.String("pipeline_id", doc.PipelineID),
				zap.Int64("version", doc.Metadata.Version))
		}
	}

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
