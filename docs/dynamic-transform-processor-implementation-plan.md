# Dynamic Transform Processor Implementation Plan

## Overview

Create a custom processor that dynamically loads transform configurations from the `elasticpipeline` extension at runtime, enabling true dynamic configuration without collector restarts.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Static Config (config.sampling.yaml)                    │
├─────────────────────────────────────────────────────────┤
│ processors:                                             │
│   elasticdynamictransform/stream_processing:            │
│     extension: elasticpipeline                          │
│     processor_key: "transform/stream_processing"        │
│     reload_interval: 30s                                │
│     fallback_mode: passthrough                          │
│                                                         │
│ pipelines:                                              │
│   logs/stream_processing:                               │
│     receivers: [routing/stream_ingress]                 │
│     processors: [elasticdynamictransform/stream_proc]   │
│     exporters: [routing/stream_egress]                  │
└─────────────────────────────────────────────────────────┘
                           │
                           │ uses
                           ▼
┌─────────────────────────────────────────────────────────┐
│ Dynamic Transform Processor                             │
├─────────────────────────────────────────────────────────┤
│ • Connects to elasticpipeline extension                 │
│ • Loads transform config from extension                 │
│ • Creates internal transform processor instance         │
│ • Delegates ConsumeLogs to internal processor           │
│ • Watches for config updates                            │
│ • Hot-swaps processor when config changes               │
└─────────────────────────────────────────────────────────┘
                           │
                           │ queries
                           ▼
┌─────────────────────────────────────────────────────────┐
│ ElasticPipeline Extension                               │
├─────────────────────────────────────────────────────────┤
│ • Fetches configs from Elasticsearch                    │
│ • Caches processor configs in memory                    │
│ • Provides GetProcessorConfig(key) API                  │
│ • Notifies subscribers on config changes                │
└─────────────────────────────────────────────────────────┘
                           │
                           │ fetches from
                           ▼
┌─────────────────────────────────────────────────────────┐
│ Elasticsearch (.otel-pipeline-config index)             │
├─────────────────────────────────────────────────────────┤
│ {                                                       │
│   "config": {                                           │
│     "processors": {                                     │
│       "transform/stream_processing": {                  │
│         "error_mode": "propagate",                      │
│         "log_statements": [...]                         │
│       }                                                 │
│     }                                                   │
│   }                                                     │
│ }                                                       │
└─────────────────────────────────────────────────────────┘
```

## Implementation Components

### 1. New Processor: `elasticdynamictransform`

Location: `processor/elasticdynamictransform/`

#### Files Structure:
```
processor/elasticdynamictransform/
├── config.go              # Processor configuration
├── config_test.go
├── factory.go             # Factory implementation
├── factory_test.go
├── processor.go           # Main processor logic
├── processor_test.go
├── doc.go                 # Package documentation
├── README.md
├── metadata.yaml
├── go.mod
└── Makefile
```

#### 1.1 Configuration (`config.go`)

```go
package elasticdynamictransform

import (
    "errors"
    "time"
)

// Config defines configuration for the dynamic transform processor
type Config struct {
    // ExtensionID is the component ID of the elasticpipeline extension
    // Example: "elasticpipeline" or "elasticpipeline/custom"
    ExtensionID string `mapstructure:"extension"`
    
    // ProcessorKey is the key to look up in the extension's processor configs
    // This should match the key in the Elasticsearch document
    // Example: "transform/stream_processing"
    ProcessorKey string `mapstructure:"processor_key"`
    
    // ReloadInterval is how often to check for config updates
    // Default: 30s
    ReloadInterval time.Duration `mapstructure:"reload_interval"`
    
    // FallbackMode defines behavior when config is unavailable
    // Options: "passthrough" (forward unchanged), "error" (return error), "drop" (drop data)
    // Default: "passthrough"
    FallbackMode string `mapstructure:"fallback_mode"`
    
    // InitialWaitTimeout is how long to wait for initial config load
    // If exceeded and no config available, uses fallback mode
    // Default: 5s
    InitialWaitTimeout time.Duration `mapstructure:"initial_wait_timeout"`
}

func (cfg *Config) Validate() error {
    if cfg.ExtensionID == "" {
        return errors.New("extension must be specified")
    }
    if cfg.ProcessorKey == "" {
        return errors.New("processor_key must be specified")
    }
    if cfg.FallbackMode != "" && 
       cfg.FallbackMode != "passthrough" && 
       cfg.FallbackMode != "error" && 
       cfg.FallbackMode != "drop" {
        return errors.New("fallback_mode must be one of: passthrough, error, drop")
    }
    return nil
}
```

#### 1.2 Factory (`factory.go`)

```go
package elasticdynamictransform

import (
    "context"
    "time"
    
    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/consumer"
    "go.opentelemetry.io/collector/processor"
)

const (
    Type             = "elasticdynamictransform"
    stability        = component.StabilityLevelAlpha
)

func NewFactory() processor.Factory {
    return processor.NewFactory(
        Type,
        createDefaultConfig,
        processor.WithLogs(createLogsProcessor, stability),
    )
}

func createDefaultConfig() component.Config {
    return &Config{
        ReloadInterval:     30 * time.Second,
        FallbackMode:       "passthrough",
        InitialWaitTimeout: 5 * time.Second,
    }
}

func createLogsProcessor(
    ctx context.Context,
    set processor.CreateSettings,
    cfg component.Config,
    nextConsumer consumer.Logs,
) (processor.Logs, error) {
    pCfg := cfg.(*Config)
    
    return newDynamicTransformProcessor(ctx, set, pCfg, nextConsumer)
}
```

#### 1.3 Processor (`processor.go`)

```go
package elasticdynamictransform

import (
    "context"
    "fmt"
    "sync"
    "time"
    
    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/consumer"
    "go.opentelemetry.io/collector/pdata/plog"
    "go.opentelemetry.io/collector/processor"
    "go.uber.org/zap"
    
    // Import transform processor
    "github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
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

type dynamicTransformProcessor struct {
    config       *Config
    settings     processor.CreateSettings
    nextConsumer consumer.Logs
    provider     ProcessorConfigProvider
    
    mu              sync.RWMutex
    currentProcessor processor.Logs  // The actual transform processor instance
    
    cancelWatch  func()
    shutdownChan chan struct{}
    logger       *zap.Logger
}

func newDynamicTransformProcessor(
    ctx context.Context,
    set processor.CreateSettings,
    config *Config,
    nextConsumer consumer.Logs,
) (*dynamicTransformProcessor, error) {
    p := &dynamicTransformProcessor{
        config:       config,
        settings:     set,
        nextConsumer: nextConsumer,
        shutdownChan: make(chan struct{}),
        logger:       set.TelemetrySettings.Logger,
    }
    
    return p, nil
}

func (p *dynamicTransformProcessor) Start(ctx context.Context, host component.Host) error {
    // Find the extension
    var provider ProcessorConfigProvider
    for _, ext := range host.GetExtensions() {
        if prov, ok := ext.(ProcessorConfigProvider); ok {
            // Check if this is the extension we're looking for
            // This is simplified - in reality we'd check the component ID
            provider = prov
            break
        }
    }
    
    if provider == nil {
        return fmt.Errorf("extension %s not found or does not implement ProcessorConfigProvider", 
            p.config.ExtensionID)
    }
    
    p.provider = provider
    
    // Load initial configuration
    if err := p.reloadConfig(ctx, host); err != nil {
        p.logger.Warn("Failed to load initial config, using fallback mode",
            zap.Error(err),
            zap.String("fallback_mode", p.config.FallbackMode))
    }
    
    // Start watching for config changes
    p.cancelWatch = p.provider.WatchProcessorConfig(p.config.ProcessorKey, func(config map[string]interface{}) {
        if err := p.reloadConfig(ctx, host); err != nil {
            p.logger.Error("Failed to reload config on update", zap.Error(err))
        } else {
            p.logger.Info("Successfully reloaded processor configuration")
        }
    })
    
    // Start periodic reload as backup
    go p.periodicReload(ctx, host)
    
    return nil
}

func (p *dynamicTransformProcessor) Shutdown(ctx context.Context) error {
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

func (p *dynamicTransformProcessor) Capabilities() consumer.Capabilities {
    p.mu.RLock()
    defer p.mu.RUnlock()
    
    if p.currentProcessor != nil {
        return p.currentProcessor.Capabilities()
    }
    
    return consumer.Capabilities{MutatesData: false}
}

func (p *dynamicTransformProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
    p.mu.RLock()
    current := p.currentProcessor
    p.mu.RUnlock()
    
    if current != nil {
        return current.ConsumeLogs(ctx, ld)
    }
    
    // No processor loaded, apply fallback behavior
    switch p.config.FallbackMode {
    case "passthrough":
        return p.nextConsumer.ConsumeLogs(ctx, ld)
    case "drop":
        p.logger.Debug("Dropping logs - no processor config loaded")
        return nil
    case "error":
        return fmt.Errorf("no processor configuration loaded for key: %s", p.config.ProcessorKey)
    default:
        return p.nextConsumer.ConsumeLogs(ctx, ld)
    }
}

func (p *dynamicTransformProcessor) reloadConfig(ctx context.Context, host component.Host) error {
    // Fetch config from extension
    config, err := p.provider.GetProcessorConfig(ctx, p.config.ProcessorKey)
    if err != nil {
        return fmt.Errorf("failed to get processor config: %w", err)
    }
    
    // Create transform processor factory
    transformFactory := transformprocessor.NewFactory()
    
    // Create default transform config
    transformConfig := transformFactory.CreateDefaultConfig()
    
    // Merge the config from Elasticsearch
    // This requires converting map[string]interface{} to the transform processor's config struct
    // We'll need a helper function for this
    if err := mergeConfig(transformConfig, config); err != nil {
        return fmt.Errorf("failed to merge config: %w", err)
    }
    
    // Create new transform processor instance
    newProcessor, err := transformFactory.CreateLogsProcessor(
        ctx,
        p.settings,
        transformConfig,
        p.nextConsumer,
    )
    if err != nil {
        return fmt.Errorf("failed to create transform processor: %w", err)
    }
    
    // Start the new processor
    if err := newProcessor.Start(ctx, host); err != nil {
        return fmt.Errorf("failed to start transform processor: %w", err)
    }
    
    // Swap processors atomically
    p.mu.Lock()
    oldProcessor := p.currentProcessor
    p.currentProcessor = newProcessor
    p.mu.Unlock()
    
    // Shutdown old processor gracefully
    if oldProcessor != nil {
        shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        
        if err := oldProcessor.Shutdown(shutdownCtx); err != nil {
            p.logger.Warn("Failed to shutdown old processor", zap.Error(err))
        }
    }
    
    p.logger.Info("Processor configuration reloaded successfully",
        zap.String("processor_key", p.config.ProcessorKey))
    
    return nil
}

func (p *dynamicTransformProcessor) periodicReload(ctx context.Context, host component.Host) {
    ticker := time.NewTicker(p.config.ReloadInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            if err := p.reloadConfig(ctx, host); err != nil {
                p.logger.Debug("Periodic reload failed", zap.Error(err))
            }
        case <-p.shutdownChan:
            return
        case <-ctx.Done():
            return
        }
    }
}

func mergeConfig(target component.Config, source map[string]interface{}) error {
    // This is a placeholder - actual implementation would use mapstructure
    // or similar to properly merge the configuration
    // For now, we can use reflection or a config merging library
    return nil // TODO: Implement proper config merging
}
```

### 2. Extension Interface Updates

Location: `extension/elasticpipelineextension/`

#### 2.1 Add Interface Definition (`config_provider.go`)

```go
package elasticpipelineextension

import "context"

// ProcessorConfigProvider is the interface that this extension implements
// to provide processor configurations to dynamic processors
type ProcessorConfigProvider interface {
    // GetProcessorConfig returns the current configuration for a processor by key
    GetProcessorConfig(ctx context.Context, key string) (map[string]interface{}, error)
    
    // WatchProcessorConfig registers a callback for configuration updates
    // Returns a cancel function to stop watching
    WatchProcessorConfig(key string, callback func(config map[string]interface{})) (cancel func())
}
```

#### 2.2 Implement Interface in Extension (`extension.go`)

```go
// Add to existing extension struct
type elasticPipelineExtension struct {
    // ... existing fields ...
    
    // New fields for processor config provider
    processorConfigs map[string]map[string]interface{}
    configWatchers   map[string][]func(config map[string]interface{})
    configMutex      sync.RWMutex
}

// Implement ProcessorConfigProvider interface
func (e *elasticPipelineExtension) GetProcessorConfig(ctx context.Context, key string) (map[string]interface{}, error) {
    e.configMutex.RLock()
    defer e.configMutex.RUnlock()
    
    config, exists := e.processorConfigs[key]
    if !exists {
        return nil, fmt.Errorf("processor config not found for key: %s", key)
    }
    
    return config, nil
}

func (e *elasticPipelineExtension) WatchProcessorConfig(key string, callback func(config map[string]interface{})) (cancel func()) {
    e.configMutex.Lock()
    defer e.configMutex.Unlock()
    
    if e.configWatchers == nil {
        e.configWatchers = make(map[string][]func(config map[string]interface{}))
    }
    
    e.configWatchers[key] = append(e.configWatchers[key], callback)
    
    // Return cancel function
    return func() {
        e.configMutex.Lock()
        defer e.configMutex.Unlock()
        
        watchers := e.configWatchers[key]
        for i, w := range watchers {
            // Compare function pointers (this is tricky in Go)
            // In practice, we might use a different approach like returning an ID
            _ = w
            _ = i
        }
    }
}

// Add method to update processor configs when fetched from Elasticsearch
func (e *elasticPipelineExtension) updateProcessorConfigs(configs map[string]map[string]interface{}) {
    e.configMutex.Lock()
    
    // Update configs
    e.processorConfigs = configs
    
    // Notify watchers
    for key, watchers := range e.configWatchers {
        if config, exists := configs[key]; exists {
            for _, callback := range watchers {
                // Call in goroutine to avoid blocking
                go callback(config)
            }
        }
    }
    
    e.configMutex.Unlock()
}
```

#### 2.3 Update Config Fetching Logic

Modify the extension's Elasticsearch fetching logic to extract and store processor configs:

```go
func (e *elasticPipelineExtension) onConfigUpdate(docs []elasticsearch.PipelineDocument) {
    // ... existing logic ...
    
    // Extract processor configs for dynamic processors
    processorConfigs := make(map[string]map[string]interface{})
    
    for _, doc := range docs {
        for key, config := range doc.Config.Processors {
            if configMap, ok := config.(map[string]interface{}); ok {
                processorConfigs[key] = configMap
            }
        }
    }
    
    // Update and notify watchers
    e.updateProcessorConfigs(processorConfigs)
    
    // ... rest of existing logic ...
}
```

### 3. Static Configuration Updates

Update `config.sampling.yaml`:

```yaml
extensions:
  elasticpipeline:
    source:
      elasticsearch:
        endpoint: http://localhost:9200
        username: elastic
        password: password
        index: .otel-pipeline-config
        cache_duration: 5m
    watcher:
      poll_interval: 30s
      cache_duration: 5m

processors:
  # Dynamic transform processor that loads config from elasticpipeline extension
  elasticdynamictransform/stream_processing:
    extension: elasticpipeline
    processor_key: "transform/stream_processing"
    reload_interval: 30s
    fallback_mode: passthrough
    initial_wait_timeout: 5s

connectors:
  routing/stream_ingress:
    default_pipelines: [logs/stream_processing]
    table:
      - statement: route()
        pipelines: [logs/stream_processing]
  
  routing/stream_egress:
    default_pipelines: [logs/sampling_decision]
    table:
      - statement: route()
        pipelines: [logs/sampling_decision]

service:
  extensions:
    - elasticpipeline
  
  pipelines:
    logs/intake:
      receivers: [otlp]
      processors: [rawcapture, resource, attributes]
      exporters: [routing/stream_ingress]
    
    # Pipeline with dynamic transform processor
    logs/stream_processing:
      receivers: [routing/stream_ingress]
      processors: [elasticdynamictransform/stream_processing]
      exporters: [routing/stream_egress]
    
    logs/sampling_decision:
      receivers: [routing/stream_egress]
      processors: [transform, batch, samplingdecide]
      exporters: [routing/output_split]
```

### 4. Elasticsearch Document Format

Remains the same - processor-only configs:

```json
{
  "pipeline_id": "elastic-streams-pipeline",
  "agent": {
    "environment": "production",
    "cluster": "default"
  },
  "config": {
    "processors": {
      "transform/stream_processing": {
        "error_mode": "propagate",
        "log_statements": [
          {
            "context": "log",
            "statements": [
              "set(attributes[\"stream.name\"], \"logs\")"
            ]
          },
          {
            "context": "log",
            "conditions": [
              "attributes[\"stream.name\"] == \"logs\""
            ],
            "statements": [
              "set(attributes[\"target_stream\"], \"logs\")",
              "set(attributes[\"otel_processed\"], true)"
            ]
          }
        ]
      }
    }
  },
  "metadata": {
    "created_at": "2025-11-10T18:57:43.288Z",
    "updated_at": "2025-11-10T18:57:43.288Z",
    "created_by": "system",
    "version": 1,
    "enabled": true,
    "priority": 100
  }
}
```

## Implementation Phases

### Phase 1: Basic Processor (Week 1)
- [ ] Create `processor/elasticdynamictransform` directory structure
- [ ] Implement basic config and factory
- [ ] Implement processor with passthrough mode only
- [ ] Add unit tests
- [ ] Add to collector manifest

### Phase 2: Extension Interface (Week 1)
- [ ] Add `ProcessorConfigProvider` interface to extension
- [ ] Implement `GetProcessorConfig` method
- [ ] Update extension to extract and store processor configs
- [ ] Add tests for config retrieval

### Phase 3: Dynamic Loading (Week 2)
- [ ] Implement transform processor instantiation in dynamic processor
- [ ] Add config merging logic (map[string]interface{} → transform config)
- [ ] Test with static configs first
- [ ] Test with extension integration

### Phase 4: Hot Reloading (Week 2)
- [ ] Implement `WatchProcessorConfig` in extension
- [ ] Add watcher registration in processor
- [ ] Implement processor hot-swap logic
- [ ] Add thread safety tests
- [ ] Test graceful shutdown of old processors

### Phase 5: Production Hardening (Week 3)
- [ ] Add comprehensive error handling
- [ ] Implement all fallback modes
- [ ] Add metrics and logging
- [ ] Performance testing
- [ ] Memory leak testing
- [ ] Documentation

## Testing Strategy

### Unit Tests
- Config validation
- Fallback mode behaviors
- Config merging logic
- Thread safety of processor swapping

### Integration Tests
- Extension→Processor communication
- Hot reload scenarios
- Multiple processors watching same config
- Config update propagation

### E2E Tests
- Full collector with Elasticsearch
- Config updates via Elasticsearch
- Verify transform is applied correctly
- Measure reload latency

## Benefits

✅ **True dynamic configuration** - No collector restarts needed
✅ **Processor-only configs** - Simple Elasticsearch documents
✅ **Hot reloading** - Updates apply within reload_interval
✅ **Graceful fallback** - Continues working if Elasticsearch unavailable
✅ **Type safe** - Uses transform processor's actual config structure
✅ **Observable** - Metrics and logs for reload events
✅ **Reusable** - Pattern can be applied to other processor types

## Limitations & Considerations

- **Reload latency**: Changes take up to `reload_interval` to apply (default 30s)
- **Memory overhead**: Multiple processor instances briefly during reload
- **Transform processor only**: Initially supports only transform processor type
- **No validation before apply**: Invalid configs detected at reload time
- **Extension dependency**: Requires elasticpipeline extension running

## Future Enhancements

1. **Multi-processor support**: Support other processor types besides transform
2. **Config validation API**: Pre-validate configs before committing to Elasticsearch
3. **Metrics**: Expose reload success/failure metrics
4. **Health checks**: Report processor config health status
5. **A/B testing**: Gradually roll out new configs to percentage of data
