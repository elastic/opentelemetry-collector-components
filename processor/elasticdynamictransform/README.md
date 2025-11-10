# Dynamic Transform Processor

The `elasticdynamictransform` processor dynamically loads transform processor configurations from the `elasticpipeline` extension at runtime, enabling configuration updates without restarting the OpenTelemetry Collector.

## How It Works

1. **Static Configuration**: Define the processor in your collector config with a reference to the extension and processor key
2. **Dynamic Loading**: At startup, the processor connects to the extension and loads the transform configuration
3. **Hot Reloading**: The processor watches for configuration updates and automatically reloads when changes are detected
4. **Fallback Modes**: If configuration is unavailable, the processor can passthrough data, drop it, or return an error

## Configuration

```yaml
processors:
  elasticdynamictransform/stream_processing:
    # Extension to load configuration from
    extension: elasticpipeline
    
    # Key to look up in the extension's processor configs
    # Must match a processor key in your Elasticsearch document
    processor_key: "transform/stream_processing"
    
    # How often to check for configuration updates (default: 30s)
    reload_interval: 30s
    
    # Behavior when config is unavailable
    # Options: passthrough, error, drop (default: passthrough)
    fallback_mode: passthrough
    
    # Timeout for initial config load (default: 5s)
    initial_wait_timeout: 5s
```

## Elasticsearch Document Format

Store your processor configurations in Elasticsearch:

```json
{
  "pipeline_id": "my-pipeline",
  "config": {
    "processors": {
      "transform/stream_processing": {
        "error_mode": "propagate",
        "log_statements": [
          {
            "context": "log",
            "statements": [
              "set(attributes[\"processed\"], true)"
            ]
          }
        ]
      }
    }
  }
}
```

## Usage Example

### Static Configuration (config.yaml)

```yaml
extensions:
  elasticpipeline:
    source:
      elasticsearch:
        endpoint: http://localhost:9200
        username: elastic
        password: password
        index: .otel-pipeline-config
    watcher:
      poll_interval: 30s

processors:
  elasticdynamictransform/stream_processing:
    extension: elasticpipeline
    processor_key: "transform/stream_processing"
    reload_interval: 30s
    fallback_mode: passthrough

service:
  extensions: [elasticpipeline]
  
  pipelines:
    logs/processing:
      receivers: [otlp]
      processors: [elasticdynamictransform/stream_processing]
      exporters: [debug]
```

## Fallback Modes

### Passthrough (Default)
```yaml
fallback_mode: passthrough
```
Forwards logs unchanged when configuration is unavailable. Safe default for production.

### Error
```yaml
fallback_mode: error
```
Returns an error when configuration is unavailable. Use when you want to ensure transforms are always applied.

### Drop
```yaml
fallback_mode: drop
```
Silently drops logs when configuration is unavailable. Use with caution.

## Monitoring

The processor logs important events:

- Configuration loads (info level)
- Reload successes/failures (info/error level)
- Fallback mode usage (debug level)
- Extension connection issues (warn level)

## Current Implementation Status

**Phase 1 (Complete)**: Basic processor structure
- ✅ Configuration parsing
- ✅ Factory implementation
- ✅ Processor lifecycle (Start/Shutdown)
- ✅ Fallback mode implementation
- ✅ Extension interface definition
- ✅ Periodic reload mechanism
- ✅ Watch-based reload mechanism

**Phase 2 (Complete)**: Transform processor integration
- ✅ Create transform processor from config
- ✅ Config merging (map[string]interface{} → transform config)
- ✅ Hot-swap processor instances with cleanup
- ✅ Extension ProcessorConfigProvider implementation
- ✅ Dynamic config extraction from Elasticsearch
- ✅ Watch notification system

**Phase 3 (Planned)**: Production hardening
- ⏳ Comprehensive error handling
- ⏳ Metrics and observability
- ⏳ Performance optimization
- ⏳ Memory leak prevention
- ⏳ Integration testing

## Limitations

- Currently supports logs signal type only
- Config updates take up to `reload_interval` to apply (default: 30s)
- Brief memory overhead during processor swap
- Watch-based updates preferred over periodic polling

## Next Steps

To complete the implementation:

1. **Add Extension Interface**: Update `elasticpipelineextension` to implement `ProcessorConfigProvider`
2. **Transform Processor Creation**: Implement actual transform processor instantiation
3. **Hot Swapping**: Implement thread-safe processor instance swapping
4. **Testing**: Add comprehensive unit and integration tests
