# Raw Sample Exporter Implementation Plan

## Overview
Create a new exporter `rawsampleexporter` to send raw sampled logs to Elasticsearch's sample API endpoint.

## API Requirements
- **Endpoint**: `POST http://localhost:9200/{index}/_sample/docs`
- **Content-Type**: `application/json`
- **Authentication**: Basic Auth
- **Payload Format**: JSON array of documents (not Elasticsearch bulk format)

```json
[
    { "message": "log1", "field1": "value1" },
    { "message": "log2", "field2": "value2" }
]
```

## Research Findings

### Elasticsearch Exporter Serialization
From examining `open-telemetry/opentelemetry-collector-contrib`:

1. **Main serialization package**: `exporter/elasticsearchexporter/internal/objmodel/`
   - `objmodel.Document` - flattens, dedups, and serializes log records to JSON
   - Handles attribute mapping, deduplication, dedotting

2. **Multiple encoding modes**:
   - `MappingNone` (legacy) - uses `objmodel.Document`
   - `MappingECS` - ECS field mapping mode, uses `objmodel.Document`
   - `MappingOTel` - OTEL mode, uses `otelserializer.Serializer`
   - `MappingBodyMap` - Direct body mapping

3. **Key serialization path for ECS mode** (most relevant):
   ```go
   // exporter/elasticsearchexporter/model.go:197-238
   func (ecsModeEncoder) encodeLog(ec encodingContext, record plog.LogRecord, ...) error {
       var document objmodel.Document
       // Map resource/scope/record attributes to ECS fields
       encodeAttributesECSMode(&document, ...)
       // Add timestamps, trace IDs, etc.
       document.AddTimestamp("@timestamp", ...)
       document.AddTraceID("trace.id", ...)
       // Serialize to JSON
       return document.Serialize(buf, true) // true = dedot
   }
   ```

4. **objmodel.Document.Serialize()**:
   - Uses `github.com/elastic/go-structform/json` for JSON generation
   - Deduplicates fields
   - Optionally dedots fields (converts "a.b.c" to nested objects)

## Implementation Plan

### 1. Create Exporter Structure

**Location**: `exporter/rawsampleexporter/`

**Files to create**:
- `factory.go` - Factory implementation
- `config.go` - Configuration structure
- `exporter.go` - Main exporter logic
- `metadata.yaml` - Component metadata
- `go.mod` - Module dependencies
- `README.md` - Documentation

### 2. Configuration Structure

```go
// config.go
type Config struct {
    // Elasticsearch connection
    Endpoint   string `mapstructure:"endpoint"`
    Username   string `mapstructure:"username"`
    Password   string `mapstructure:"password"`
    
    // Index name for sample API
    Index string `mapstructure:"index"` // e.g., "logs-raw-samples"
    
    // Batching (array size)
    MaxBatchSize int `mapstructure:"max_batch_size"` // default: 100
    
    // HTTP settings
    Timeout       time.Duration `mapstructure:"timeout"`
    RetrySettings configretry.BackOffConfig `mapstructure:"retry_on_failure"`
}
```

### 3. Reuse Elasticsearch Exporter Serialization (OTEL Mode)

**Strategy**: Use the OTEL serializer from `elasticsearchexporter` which uses `objmodel.Document` internally.

**Dependencies**:
```go
import (
    "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/serializer/otelserializer"
)
```

**Approach**: Since we need `objmodel.Document` and always use OTEL mode:
1. Use the `otelserializer.Serializer` directly (same as elasticsearchexporter's OTEL mode)
2. This handles all the objmodel.Document logic internally
3. Serialize each log record to JSON using the serializer
4. Collect serialized logs into an array
5. Send as JSON array to sample API

**Note**: The OTEL serializer uses `objmodel.Document` under the hood and provides proper encoding with:
- Timestamp handling
- Trace/Span ID encoding
- Resource and scope attributes
- Proper JSON structure

### 4. Core Exporter Logic

```go
// exporter.go
type rawSampleExporter struct {
    config     *Config
    httpClient *http.Client
    serializer *otelserializer.Serializer
    logger     *zap.Logger
}

func (e *rawSampleExporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
    // 1. Collect all serialized log documents
    var jsonDocs []json.RawMessage
    
    // 2. Iterate through all logs and serialize using OTEL mode
    for i := 0; i < logs.ResourceLogs().Len(); i++ {
        rl := logs.ResourceLogs().At(i)
        resource := rl.Resource()
        resourceSchemaURL := rl.SchemaUrl()
        
        for j := 0; j < rl.ScopeLogs().Len(); j++ {
            sl := rl.ScopeLogs().At(j)
            scope := sl.Scope()
            scopeSchemaURL := sl.SchemaUrl()
            
            for k := 0; k < sl.LogRecords().Len(); k++ {
                record := sl.LogRecords().At(k)
                
                // Serialize using OTEL serializer (uses objmodel.Document internally)
                var buf bytes.Buffer
                err := e.serializer.SerializeLog(
                    resource, resourceSchemaURL,
                    scope, scopeSchemaURL,
                    record,
                    elasticsearch.Index{}, // Empty index, not used for sample API
                    &buf,
                )
                if err != nil {
                    e.logger.Warn("Failed to serialize log", zap.Error(err))
                    continue
                }
                
                // Collect as RawMessage (already valid JSON)
                jsonDocs = append(jsonDocs, json.RawMessage(buf.Bytes()))
            }
        }
    }
    
    if len(jsonDocs) == 0 {
        return nil
    }
    
    // 3. Send in batches
    return e.sendBatches(ctx, jsonDocs)
}

func (e *rawSampleExporter) sendBatches(ctx context.Context, docs []json.RawMessage) error {
    batchSize := e.config.MaxBatchSize
    if batchSize <= 0 {
        batchSize = 100 // default
    }
    
    for i := 0; i < len(docs); i += batchSize {
        end := i + batchSize
        if end > len(docs) {
            end = len(docs)
        }
        
        batch := docs[i:end]
        if err := e.sendBatch(ctx, batch); err != nil {
            return err
        }
    }
    
    return nil
}

func (e *rawSampleExporter) sendBatch(ctx context.Context, docs []json.RawMessage) error {
    // Marshal JSON array
    payload, err := json.Marshal(docs)
    if err != nil {
        return fmt.Errorf("failed to marshal document array: %w", err)
    }
    
    // Build URL
    url := fmt.Sprintf("%s/%s/_sample/docs", e.config.Endpoint, e.config.Index)
    
    // Create request
    req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
    if err != nil {
        return err
    }
    
    req.Header.Set("Content-Type", "application/json")
    req.SetBasicAuth(e.config.Username, e.config.Password)
    
    // Send request
    resp, err := e.httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    
    if resp.StatusCode >= 300 {
        body, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("sample API error: %d - %s", resp.StatusCode, string(body))
    }
    
    e.logger.Debug("Sent batch to sample API",
        zap.Int("count", len(docs)),
        zap.String("index", e.config.Index),
    )
    
    return nil
}
```

### 5. Factory Implementation

```go
// factory.go
func NewFactory() exporter.Factory {
    return exporter.NewFactory(
        metadata.Type,
        createDefaultConfig,
        exporter.WithLogs(createLogsExporter, metadata.LogsStability),
    )
}

func createDefaultConfig() component.Config {
    return &Config{
        Endpoint:     "http://localhost:9200",
        Index:        "logs-raw-samples",
        MaxBatchSize: 100,
        Timeout:      30 * time.Second,
        RetrySettings: configretry.NewDefaultBackOffConfig(),
    }
}

func createLogsExporter(
    ctx context.Context,
    set exporter.Settings,
    cfg component.Config,
) (exporter.Logs, error) {
    config := cfg.(*Config)
    
    // Create OTEL serializer (uses objmodel.Document internally)
    serializer, err := otelserializer.New()
    if err != nil {
        return nil, fmt.Errorf("failed to create serializer: %w", err)
    }
    
    exporter := &rawSampleExporter{
        config:     config,
        httpClient: &http.Client{Timeout: config.Timeout},
        serializer: serializer,
        logger:     set.Logger,
    }
    
    return exporterhelper.NewLogs(
        ctx,
        set,
        cfg,
        exporter.ConsumeLogs,
        exporterhelper.WithStart(exporter.Start),
        exporterhelper.WithShutdown(exporter.Shutdown),
        exporterhelper.WithRetry(config.RetrySettings),
    )
}
```

## File Structure

```
exporter/rawsampleexporter/
├── config.go          # Configuration struct
├── config_test.go     # Config tests
├── exporter.go        # Main exporter implementation
├── exporter_test.go   # Exporter tests
├── factory.go         # Factory implementation
├── factory_test.go    # Factory tests
├── metadata.yaml      # Component metadata
├── go.mod            # Module file
├── README.md         # Documentation
└── internal/
    └── encoder/
        └── encoder.go # Optional: ECS encoding logic if needed
```

## Dependencies

```go
// go.mod
require (
    go.opentelemetry.io/collector/pdata v1.44.0
    go.opentelemetry.io/collector/component v0.138.0
    go.opentelemetry.io/collector/exporter v0.138.0
    go.opentelemetry.io/collector/exporter/exporterhelper v0.138.0
    go.opentelemetry.io/collector/config/configretry v1.44.0
    go.uber.org/zap v1.27.0
    github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter v0.138.0
)
```

## Testing Strategy

1. **Unit tests**: Mock HTTP server, verify payload format
2. **Integration test**: Optional - test against real Elasticsearch
3. **Config test**: Validate configuration parsing

## Configuration Example

```yaml
exporters:
  rawsample:
    endpoint: "http://localhost:9200"
    username: "elastic"
    password: "changeme"
    index: "logs-raw-samples"
    max_batch_size: 100
    timeout: 30s
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 300s

service:
  pipelines:
    logs/sampling:
      receivers: [routing]
      processors: [samplingdecide, rawretriever]
      exporters: [rawsample, debug]
```

## Implementation Steps

1. ✅ Research elasticsearch exporter serialization (DONE)
2. 📝 Create exporter skeleton (factory, config, metadata)
3. 📝 Integrate otelserializer.Serializer (OTEL mode with objmodel.Document)
4. 📝 Implement batch collection and HTTP sending
5. 📝 Add error handling and logging
6. 📝 Write unit tests
7. 📝 Update manifest.yaml
8. 📝 Update config.sampling.yaml
9. 📝 Test end-to-end

## Key Design Points

1. **Serialization**: Uses `otelserializer.Serializer` which internally uses `objmodel.Document`
   - This ensures proper OTEL-compliant JSON serialization
   - Handles all field encoding (timestamps, trace IDs, attributes, etc.)
   - Same serialization as elasticsearch exporter's OTEL mode

2. **Index**: Simple static index configuration
   - No dataset extraction needed
   - URL: `{endpoint}/{index}/_sample/docs`
   
3. **Batching**: All logs batched together (no grouping by dataset)
   - Simple batching by max_batch_size
   - All go to the same index endpoint
   
4. **Error handling**: Log serialization errors as warnings, continue with remaining logs

## Next Steps

Ready to implement! Start with:
1. Create directory structure
2. Implement config.go and factory.go
3. Implement simple exporter.go with JSON marshaling
4. Test with sample data
