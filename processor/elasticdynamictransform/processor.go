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

package elasticdynamictransform

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// ProcessorConfigProvider is the interface that extensions must implement
// to provide processor configurations dynamically
type ProcessorConfigProvider interface {
	// GetProcessorConfig returns the current configuration for a processor
	GetProcessorConfig(ctx context.Context, key string) (map[string]interface{}, error)

	// WatchProcessorConfig registers a callback for configuration updates
	// Returns a cancel function to stop watching
	WatchProcessorConfig(key string, callback func(config map[string]interface{})) (cancel func())
}

// dynamicTransformProcessor is a processor that dynamically loads transform configurations
type dynamicTransformProcessor struct {
	config       *Config
	settings     processor.Settings
	nextConsumer consumer.Logs
	provider     ProcessorConfigProvider

	mu               sync.RWMutex
	currentProcessor processor.Logs // The actual transform processor instance

	cancelWatch  func()
	shutdownChan chan struct{}
	logger       *zap.Logger
}

// newDynamicTransformProcessor creates a new dynamic transform processor
func newDynamicTransformProcessor(
	ctx context.Context,
	set processor.Settings,
	config *Config,
	nextConsumer consumer.Logs,
) (*dynamicTransformProcessor, error) {
	p := &dynamicTransformProcessor{
		config:       config,
		settings:     set,
		nextConsumer: nextConsumer,
		shutdownChan: make(chan struct{}),
		logger:       set.Logger,
	}

	return p, nil
}

// Start initializes the processor
func (p *dynamicTransformProcessor) Start(ctx context.Context, host component.Host) error {
	p.logger.Info("Starting dynamic transform processor",
		zap.String("processor_key", p.config.ProcessorKey),
		zap.String("extension", p.config.ExtensionID))

	// Find the extension
	var provider ProcessorConfigProvider
	for _, ext := range host.GetExtensions() {
		if prov, ok := ext.(ProcessorConfigProvider); ok {
			// TODO: Check if this is the correct extension by ID
			// For now, we take the first ProcessorConfigProvider we find
			provider = prov
			break
		}
	}

	if provider == nil {
		p.logger.Warn("Extension not found or does not implement ProcessorConfigProvider, using fallback mode",
			zap.String("extension", p.config.ExtensionID),
			zap.String("fallback_mode", string(p.config.FallbackMode)))
		// Don't fail - continue with fallback mode
		return nil
	}

	p.provider = provider

	// Try to load initial configuration
	loadCtx, cancel := context.WithTimeout(ctx, p.config.InitialWaitTimeout)
	defer cancel()

	if err := p.reloadConfig(loadCtx, host); err != nil {
		p.logger.Warn("Failed to load initial config, using fallback mode",
			zap.Error(err),
			zap.String("fallback_mode", string(p.config.FallbackMode)))
		// Don't fail - continue with fallback mode
	} else {
		p.logger.Info("Successfully loaded initial processor configuration",
			zap.String("processor_key", p.config.ProcessorKey))
	}

	// Start watching for config changes
	if p.provider != nil {
		p.cancelWatch = p.provider.WatchProcessorConfig(p.config.ProcessorKey, func(config map[string]interface{}) {
			p.logger.Info("Received config update notification",
				zap.String("processor_key", p.config.ProcessorKey))

			reloadCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			if err := p.reloadConfig(reloadCtx, host); err != nil {
				p.logger.Error("Failed to reload config on update", zap.Error(err))
			} else {
				p.logger.Info("Successfully reloaded processor configuration")
			}
		})
	}

	// Start periodic reload as backup
	go p.periodicReload(ctx, host)

	return nil
}

// Shutdown stops the processor
func (p *dynamicTransformProcessor) Shutdown(ctx context.Context) error {
	p.logger.Info("Shutting down dynamic transform processor")

	close(p.shutdownChan)

	if p.cancelWatch != nil {
		p.cancelWatch()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.currentProcessor != nil {
		return p.currentProcessor.Shutdown(ctx)
	}

	return nil
}

// Capabilities returns the capabilities of the processor
func (p *dynamicTransformProcessor) Capabilities() consumer.Capabilities {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.currentProcessor != nil {
		return p.currentProcessor.Capabilities()
	}

	// Default capabilities when no processor is loaded
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeLogs processes log records
func (p *dynamicTransformProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	p.mu.RLock()
	current := p.currentProcessor
	p.mu.RUnlock()

	if current != nil {
		return current.ConsumeLogs(ctx, ld)
	}

	// No processor loaded, apply fallback behavior
	switch p.config.FallbackMode {
	case FallbackModePassthrough:
		p.logger.Debug("Passthrough mode - forwarding logs unchanged (no processor config loaded)")
		return p.nextConsumer.ConsumeLogs(ctx, ld)

	case FallbackModeDrop:
		p.logger.Debug("Drop mode - dropping logs (no processor config loaded)")
		return nil

	case FallbackModeError:
		return fmt.Errorf("no processor configuration loaded for key: %s", p.config.ProcessorKey)

	default:
		// Default to passthrough
		return p.nextConsumer.ConsumeLogs(ctx, ld)
	}
}

// reloadConfig fetches the configuration and creates/updates the transform processor
func (p *dynamicTransformProcessor) reloadConfig(ctx context.Context, host component.Host) error {
	if p.provider == nil {
		return fmt.Errorf("no provider available")
	}

	// Fetch config from extension
	configMap, err := p.provider.GetProcessorConfig(ctx, p.config.ProcessorKey)
	if err != nil {
		return fmt.Errorf("failed to get processor config: %w", err)
	}

	p.logger.Debug("Fetched processor configuration from extension",
		zap.String("processor_key", p.config.ProcessorKey),
		zap.Any("config", configMap))

	// The config from Elasticsearch has the structure:
	// {
	//   "error_mode": "propagate",
	//   "log_statements": [...]
	// }
	// We need to unmarshal this into transformprocessor.Config
	conf := confmap.NewFromStringMap(configMap)
	var transformConfig transformprocessor.Config
	if err := conf.Unmarshal(&transformConfig); err != nil {
		return fmt.Errorf("failed to unmarshal transform config: %w", err)
	}

	p.logger.Debug("Unmarshaled transform config",
		zap.Int("log_statements_count", len(transformConfig.LogStatements)),
		zap.String("error_mode", string(transformConfig.ErrorMode)))

	// Clear the context field to enable context inference.
	// When context is empty, the transform processor infers it from the paths used,
	// allowing cleaner syntax without "log." prefix in statements.
	// This is simpler than manually adding "log." prefix to all paths.
	// for i := range transformConfig.LogStatements {
	// 	transformConfig.LogStatements[i].Context = ""
	// }

	// Log the actual statements for debugging
	for i, stmt := range transformConfig.LogStatements {
		p.logger.Debug("Log statement group",
			zap.Int("index", i),
			zap.String("context", string(stmt.Context)),
			zap.Strings("statements", stmt.Statements),
			zap.Strings("conditions", stmt.Conditions))
	}

	// Create transform processor factory
	factory := transformprocessor.NewFactory()

	// NOTE: We don't call transformConfig.Validate() here because the config's
	// logFunctions map is not yet initialized. The factory will handle validation
	// during CreateLogs() after setting up the OTTL function registry.

	// Create processor settings for the transform processor
	// We need to use a component ID with type "transform" to satisfy the factory's type check
	transformID := component.NewIDWithName(factory.Type(), p.config.ProcessorKey)
	transformSettings := processor.Settings{
		ID:                transformID,
		TelemetrySettings: p.settings.TelemetrySettings,
		BuildInfo:         p.settings.BuildInfo,
	}

	// Create the transform processor
	newProcessor, err := factory.CreateLogs(ctx, transformSettings, &transformConfig, p.nextConsumer)
	if err != nil {
		return fmt.Errorf("failed to create transform processor: %w", err)
	}

	// Start the new processor
	if err := newProcessor.Start(ctx, host); err != nil {
		return fmt.Errorf("failed to start transform processor: %w", err)
	}

	// Hot-swap the processor with thread safety
	p.mu.Lock()
	oldProcessor := p.currentProcessor
	p.currentProcessor = newProcessor
	p.mu.Unlock()

	p.logger.Info("Successfully created and swapped transform processor",
		zap.String("processor_key", p.config.ProcessorKey))

	// Shutdown old processor if it exists
	if oldProcessor != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := oldProcessor.Shutdown(shutdownCtx); err != nil {
			p.logger.Warn("Failed to shutdown old processor",
				zap.Error(err))
		} else {
			p.logger.Debug("Old processor shut down successfully")
		}
	}

	return nil
}

// periodicReload periodically checks for configuration updates
func (p *dynamicTransformProcessor) periodicReload(ctx context.Context, host component.Host) {
	ticker := time.NewTicker(p.config.ReloadInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if p.provider != nil {
				reloadCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := p.reloadConfig(reloadCtx, host); err != nil {
					p.logger.Debug("Periodic reload failed", zap.Error(err))
				}
				cancel()
			}

		case <-p.shutdownChan:
			p.logger.Debug("Periodic reload stopped")
			return

		case <-ctx.Done():
			p.logger.Debug("Periodic reload stopped due to context cancellation")
			return
		}
	}
}
