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

package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/extension/elasticpipelineextension/internal/elasticsearch"
)

// Manager handles dynamic pipeline lifecycle management.
type Manager struct {
	namespace                string
	maxPipelines             int
	maxComponentsPerPipeline int
	validateConfigs          bool
	dryRunMode               bool
	startupTimeout           time.Duration
	shutdownTimeout          time.Duration
	logger                   *zap.Logger

	mu               sync.RWMutex
	managedPipelines map[string]*ManagedPipeline // pipeline_id -> managed pipeline
	host             factoryGetter
}

// factoryGetter is an interface that the component.Host must implement
// to allow dynamic component creation.
type factoryGetter interface {
	component.Host
	GetFactory(component.Kind, component.Type) component.Factory
}

// SignalPipeline represents components for a specific signal type (logs, metrics, traces).
type SignalPipeline struct {
	SignalType   string // "logs", "metrics", "traces"
	PipelineName string // Full pipeline name like "logs/application"

	// Component lists for this signal
	Receivers  []component.Component
	Processors []component.Component
	Exporters  []component.Component

	// Consumer chains for this signal type
	LogsConsumer    consumer.Logs
	MetricsConsumer consumer.Metrics
	TracesConsumer  consumer.Traces
}

// ManagedPipeline represents a dynamically managed pipeline.
type ManagedPipeline struct {
	ID         string
	Config     elasticsearch.PipelineConfig
	Version    int64
	Status     PipelineStatus
	CreatedAt  time.Time
	UpdatedAt  time.Time
	LastError  string
	ErrorCount int64

	// Signal-specific pipelines (e.g., "logs/application" -> SignalPipeline)
	SignalPipelines map[string]*SignalPipeline
}

// PipelineStatus represents the status of a managed pipeline.
type PipelineStatus string

const (
	StatusPending  PipelineStatus = "pending"
	StatusRunning  PipelineStatus = "running"
	StatusFailed   PipelineStatus = "failed"
	StatusStopping PipelineStatus = "stopping"
	StatusStopped  PipelineStatus = "stopped"
)

// NewManager creates a new pipeline manager.
func NewManager(
	namespace string,
	maxPipelines int,
	maxComponentsPerPipeline int,
	validateConfigs bool,
	dryRunMode bool,
	startupTimeout time.Duration,
	shutdownTimeout time.Duration,
	logger *zap.Logger,
) *Manager {
	return &Manager{
		namespace:                namespace,
		maxPipelines:             maxPipelines,
		maxComponentsPerPipeline: maxComponentsPerPipeline,
		validateConfigs:          validateConfigs,
		dryRunMode:               dryRunMode,
		startupTimeout:           startupTimeout,
		shutdownTimeout:          shutdownTimeout,
		logger:                   logger,
		managedPipelines:         make(map[string]*ManagedPipeline),
	}
}

// SetHost sets the collector host for component management.
func (m *Manager) SetHost(host component.Host) {
	m.mu.Lock()
	defer m.mu.Unlock()

	h, ok := host.(factoryGetter)
	if !ok {
		m.logger.Error("Host does not implement required factory getter interface")
		return
	}
	m.host = h
}

// ApplyConfiguration applies a pipeline configuration.
func (m *Manager) ApplyConfiguration(ctx context.Context, doc elasticsearch.PipelineDocument) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if this is a removal (indicated by version -1)
	if doc.Metadata.Version == -1 {
		return m.removePipelineInternal(ctx, doc.PipelineID)
	}

	// Check limits
	if len(m.managedPipelines) >= m.maxPipelines {
		return fmt.Errorf("maximum number of pipelines (%d) reached", m.maxPipelines)
	}

	// Validate configuration if enabled
	if m.validateConfigs {
		if err := m.validatePipelineConfig(doc.Config); err != nil {
			return fmt.Errorf("configuration validation failed: %w", err)
		}
	}

	pipelineID := m.buildPipelineID(doc.PipelineID)

	// Check if pipeline already exists
	if existing, exists := m.managedPipelines[pipelineID]; exists {
		if existing.Version >= doc.Metadata.Version {
			m.logger.Info("Pipeline version is not newer, skipping update",
				zap.String("pipeline_id", pipelineID),
				zap.Int64("existing_version", existing.Version),
				zap.Int64("new_version", doc.Metadata.Version))
			return nil
		}

		// Update existing pipeline
		return m.updatePipelineInternal(ctx, pipelineID, doc)
	}

	// Create new pipeline
	return m.createPipelineInternal(ctx, pipelineID, doc)
}

// RemoveConfiguration removes a pipeline configuration.
func (m *Manager) RemoveConfiguration(ctx context.Context, pipelineID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.removePipelineInternal(ctx, pipelineID)
}

// ListManagedPipelines returns a list of managed pipeline IDs.
func (m *Manager) ListManagedPipelines() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pipelineIDs := make([]string, 0, len(m.managedPipelines))
	for id := range m.managedPipelines {
		pipelineIDs = append(pipelineIDs, id)
	}
	return pipelineIDs
}

// GetPipelineHealth returns the health status of a specific pipeline.
func (m *Manager) GetPipelineHealth(pipelineID string) (*elasticsearch.PipelineHealth, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fullID := m.buildPipelineID(pipelineID)
	managed, exists := m.managedPipelines[fullID]
	if !exists {
		return nil, fmt.Errorf("pipeline %s not found", pipelineID)
	}

	return &elasticsearch.PipelineHealth{
		Status:      string(managed.Status),
		LastApplied: managed.UpdatedAt,
		LastError:   managed.LastError,
		ErrorCount:  managed.ErrorCount,
	}, nil
}

// GetAllPipelineHealth returns health status for all managed pipelines.
func (m *Manager) GetAllPipelineHealth() map[string]*elasticsearch.PipelineHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()

	health := make(map[string]*elasticsearch.PipelineHealth)
	for id, managed := range m.managedPipelines {
		// Strip namespace from ID for external reporting
		originalID := m.stripNamespace(id)
		health[originalID] = &elasticsearch.PipelineHealth{
			Status:      string(managed.Status),
			LastApplied: managed.UpdatedAt,
			LastError:   managed.LastError,
			ErrorCount:  managed.ErrorCount,
		}
	}
	return health
}

// Internal methods (must be called with lock held)

func (m *Manager) createPipelineInternal(ctx context.Context, pipelineID string, doc elasticsearch.PipelineDocument) error {
	m.logger.Info("Creating new pipeline", zap.String("pipeline_id", pipelineID))

	if m.dryRunMode {
		m.logger.Info("Dry run mode: would create pipeline", zap.String("pipeline_id", pipelineID))
		return nil
	}

	if m.host == nil {
		return fmt.Errorf("component host not set")
	}

	managed := &ManagedPipeline{
		ID:              pipelineID,
		Config:          doc.Config,
		Version:         doc.Metadata.Version,
		Status:          StatusPending,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
		SignalPipelines: make(map[string]*SignalPipeline),
	}

	// ARCHITECTURE LIMITATION:
	// OpenTelemetry Collector does not support dynamically creating pipelines that reference
	// connectors from the static configuration. Connectors can only wire together pipelines
	// that are both defined in the static config.
	//
	// To support processor-only configs from Elasticsearch, we would need to either:
	// 1. Create a custom "dynamic transform" processor that loads configs from the extension
	// 2. Require full pipeline definitions in Elasticsearch (not just processors)
	// 3. Modify the OTel Collector core to support dynamic pipeline registration
	//
	// For now, this extension only supports full pipeline definitions from Elasticsearch.
	
	if len(doc.Config.Pipelines) == 0 {
		return fmt.Errorf("no pipelines defined in configuration - processor-only configs are not supported")
	}

	// Create signal pipelines from full pipeline definitions
	for pipelineName, pipelineDef := range doc.Config.Pipelines {
		signalType := extractSignalType(pipelineName)

		if !isValidSignalType(signalType) {
			m.logger.Warn("Invalid signal type in pipeline name, skipping",
				zap.String("pipeline_name", pipelineName),
				zap.String("signal_type", signalType))
			continue
		}

		m.logger.Info("Creating signal pipeline",
			zap.String("pipeline_name", pipelineName),
			zap.String("signal_type", signalType))

		signalPipeline, err := m.buildSignalPipeline(ctx, pipelineName, signalType, pipelineDef, doc.Config, pipelineID)
		if err != nil {
			m.shutdownSignalPipelines(ctx, managed.SignalPipelines)
			managed.Status = StatusFailed
			managed.LastError = err.Error()
			managed.ErrorCount++
			return fmt.Errorf("failed to create signal pipeline %s: %w", pipelineName, err)
		}

		managed.SignalPipelines[pipelineName] = signalPipeline
	}

	if len(managed.SignalPipelines) == 0 {
		return fmt.Errorf("no valid signal pipelines created")
	}

	// Start all signal pipelines
	if err := m.startSignalPipelines(ctx, managed.SignalPipelines); err != nil {
		m.shutdownSignalPipelines(ctx, managed.SignalPipelines)
		managed.Status = StatusFailed
		managed.LastError = err.Error()
		managed.ErrorCount++
		return fmt.Errorf("failed to start signal pipelines: %w", err)
	}

	managed.Status = StatusRunning
	m.managedPipelines[pipelineID] = managed

	m.logger.Info("Pipeline created successfully",
		zap.String("pipeline_id", pipelineID),
		zap.Int("signal_pipelines", len(managed.SignalPipelines)))
	return nil
}

func (m *Manager) updatePipelineInternal(ctx context.Context, pipelineID string, doc elasticsearch.PipelineDocument) error {
	m.logger.Info("Updating existing pipeline", zap.String("pipeline_id", pipelineID))

	if m.dryRunMode {
		m.logger.Info("Dry run mode: would update pipeline", zap.String("pipeline_id", pipelineID))
		return nil
	}

	managed := m.managedPipelines[pipelineID]

	// Gracefully shutdown existing signal pipelines
	if managed.SignalPipelines != nil {
		m.shutdownSignalPipelines(ctx, managed.SignalPipelines)
	}

	// Clear signal pipeline references
	managed.SignalPipelines = make(map[string]*SignalPipeline)

	// Update configuration
	managed.Config = doc.Config
	managed.Version = doc.Metadata.Version
	managed.UpdatedAt = time.Now()
	managed.Status = StatusPending

	// Recreate signal pipelines with new configuration
	for pipelineName, pipelineDef := range doc.Config.Pipelines {
		signalType := extractSignalType(pipelineName)

		if !isValidSignalType(signalType) {
			m.logger.Warn("Invalid signal type in pipeline name, skipping",
				zap.String("pipeline_name", pipelineName),
				zap.String("signal_type", signalType))
			continue
		}

		m.logger.Info("Recreating signal pipeline during update",
			zap.String("pipeline_name", pipelineName),
			zap.String("signal_type", signalType))

		signalPipeline, err := m.buildSignalPipeline(ctx, pipelineName, signalType, pipelineDef, doc.Config, pipelineID)
		if err != nil {
			m.shutdownSignalPipelines(ctx, managed.SignalPipelines)
			managed.Status = StatusFailed
			managed.LastError = err.Error()
			managed.ErrorCount++
			return fmt.Errorf("failed to recreate signal pipeline %s during update: %w", pipelineName, err)
		}

		managed.SignalPipelines[pipelineName] = signalPipeline
	}

	// Check if we recreated any pipelines
	if len(managed.SignalPipelines) == 0 {
		managed.Status = StatusFailed
		managed.LastError = "no valid signal pipelines created during update"
		managed.ErrorCount++
		return fmt.Errorf("no valid signal pipelines created during update")
	}

	// Start all signal pipelines
	if err := m.startSignalPipelines(ctx, managed.SignalPipelines); err != nil {
		m.shutdownSignalPipelines(ctx, managed.SignalPipelines)
		managed.Status = StatusFailed
		managed.LastError = err.Error()
		managed.ErrorCount++
		return fmt.Errorf("failed to start signal pipelines during update: %w", err)
	}

	managed.Status = StatusRunning
	managed.LastError = ""

	m.logger.Info("Pipeline updated successfully",
		zap.String("pipeline_id", pipelineID),
		zap.Int("signal_pipelines", len(managed.SignalPipelines)))
	return nil
}

func (m *Manager) removePipelineInternal(ctx context.Context, pipelineID string) error {
	fullID := m.buildPipelineID(pipelineID)

	managed, exists := m.managedPipelines[fullID]
	if !exists {
		m.logger.Info("Pipeline not found for removal", zap.String("pipeline_id", pipelineID))
		return nil // Not an error - pipeline might have been removed already
	}

	m.logger.Info("Removing pipeline", zap.String("pipeline_id", pipelineID))

	if m.dryRunMode {
		m.logger.Info("Dry run mode: would remove pipeline", zap.String("pipeline_id", pipelineID))
		return nil
	}

	// Gracefully shutdown all signal pipelines
	if managed.SignalPipelines != nil {
		m.shutdownSignalPipelines(ctx, managed.SignalPipelines)
	}

	managed.Status = StatusStopped
	delete(m.managedPipelines, fullID)

	m.logger.Info("Pipeline removed successfully", zap.String("pipeline_id", pipelineID))
	return nil
}

// Helper methods

func (m *Manager) validatePipelineConfig(config elasticsearch.PipelineConfig) error {
	componentCount := 0
	componentCount += len(config.Receivers)
	componentCount += len(config.Processors)
	componentCount += len(config.Exporters)
	componentCount += len(config.Connectors)

	if componentCount > m.maxComponentsPerPipeline {
		return fmt.Errorf("pipeline has %d components, maximum allowed is %d",
			componentCount, m.maxComponentsPerPipeline)
	}

	// Additional validation could include:
	// - Schema validation
	// - Component existence checks
	// - Security policy validation

	return nil
}

func (m *Manager) buildPipelineID(originalID string) string {
	return fmt.Sprintf("%s/%s", m.namespace, originalID)
}

func (m *Manager) stripNamespace(fullID string) string {
	prefix := m.namespace + "/"
	if len(fullID) > len(prefix) && fullID[:len(prefix)] == prefix {
		return fullID[len(prefix):]
	}
	return fullID
}
