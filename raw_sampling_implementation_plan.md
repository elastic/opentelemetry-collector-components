# Raw Sampling Implementation Plan

## Overview
This plan outlines the implementation of a tail-based sampling system for OpenTelemetry logs using custom extensions and processors. The architecture will store raw logs in a buffer, process them through standard pipelines, and conditionally export raw logs to a dedicated datastore based on tail-based sampling decisions.

## Architecture Summary

### Data Flow
1. **logs/intake**: Receives raw logs → captures raw data with UUID → processes logs → routes to both production and sampling pipelines
2. **logs/prod**: Receives processed logs → removes sampling ID → exports to production Elasticsearch
3. **logs/sampling**: Receives processed logs → evaluates sampling condition → retrieves raw log from buffer → exports to raw datastore

### Components to Implement

#### 1. Extension: `rawsamplingbuffer`
- **Purpose**: Shared in-memory circular buffer storing raw log data
- **Location**: `extension/rawsamplingbuffer/`

#### 2. Processor: `rawcapture`
- **Purpose**: Captures raw logs and stores them in the buffer with a UUID
- **Location**: `processor/rawcapture/`

#### 3. Processor: `samplingdecide`
- **Purpose**: Evaluates tail-based sampling conditions and drops logs that don't match
- **Location**: `processor/samplingdecide/`

#### 4. Processor: `rawretriever`
- **Purpose**: Retrieves raw logs from the buffer using the UUID
- **Location**: `processor/rawretriever/`

---

## Detailed Implementation Plan

### Phase 1: Extension - `rawsamplingbuffer`

**Directory Structure:**
```
extension/rawsamplingbuffer/
├── doc.go
├── factory.go
├── factory_test.go
├── config.go
├── config_test.go
├── extension.go
├── extension_test.go
├── metadata.yaml
├── Makefile
├── README.md
├── go.mod
├── go.sum
├── generated_component_test.go
├── generated_package_test.go
├── internal/
│   └── buffer/
│       ├── circular_buffer.go
│       └── circular_buffer_test.go
└── testdata/
    └── config.yaml
```

**Implementation Steps:**

1. **Create metadata.yaml**
   - Type: `rawsamplingbuffer`
   - Status: `development` stability for extension

2. **Create config.go**
   - Define `Config` struct with:
     - `BufferSize int` (default: 1000) - maximum number of entries
     - `MaxEntrySize int` (default: 1MB) - max size per log entry
     - `TTL time.Duration` (default: 5m) - how long entries live in buffer
     - `EvictionPolicy string` (default: "lru") - eviction strategy

3. **Create factory.go**
   - Implement `NewFactory()` returning `extension.Factory`
   - Implement `createDefaultConfig()` setting defaults
   - Implement `createExtension()` creating the extension instance

4. **Create internal/buffer/circular_buffer.go**
   - Implement thread-safe circular buffer:
     - `Store(id string, data []byte) error` - store raw log with UUID
     - `Retrieve(id string) ([]byte, error)` - get raw log by UUID
     - `Delete(id string) error` - remove entry
     - `Size() int` - current buffer size
     - Handle TTL-based expiration
     - Handle size-based eviction (LRU)
     - Thread-safe operations using sync.RWMutex

5. **Create extension.go**
   - Implement `rawSamplingBufferExtension` struct
   - Embed the circular buffer from internal/buffer
   - Implement `extension.Extension` interface:
     - `Start(context.Context, component.Host) error`
     - `Shutdown(context.Context) error`
   - Expose methods for processors to use:
     - `GetBuffer() *buffer.CircularBuffer`

6. **Create tests**
   - Unit tests for circular buffer (thread safety, eviction, TTL)
   - Config validation tests
   - Integration tests

7. **Create documentation**
   - README.md explaining usage and configuration
   - Example configurations in testdata/

---

### Phase 2: Processor - `rawcapture`

**Directory Structure:**
```
processor/rawcapture/
├── doc.go
├── factory.go
├── factory_test.go
├── config.go
├── config_test.go
├── processor.go
├── processor_test.go
├── metadata.yaml
├── Makefile
├── README.md
├── go.mod
├── go.sum
├── generated_component_test.go
├── generated_package_test.go
└── testdata/
    └── config.yaml
```

**Implementation Steps:**

1. **Create metadata.yaml**
   - Type: `rawcapture`
   - Status: `development` stability for logs

2. **Create config.go**
   - Define `Config` struct with:
     - `AttributeKey string` (default: "raw.id") - where to store UUID
     - `ExtensionName string` (required) - reference to rawsamplingbuffer extension
     - `SkipOnError bool` (default: false) - whether to continue if buffer store fails

3. **Create factory.go**
   - Implement `NewFactory()` returning `processor.Factory`
   - Register for logs pipeline: `processor.WithLogs(createLogsProcessor, metadata.LogsStability)`
   - Implement `createDefaultConfig()`
   - Implement `createLogsProcessor()`

4. **Create processor.go**
   - Implement `rawCaptureProcessor` struct:
     - Reference to buffer extension
     - Configuration
     - Logger
   - Implement `processor.Logs` interface:
     - `ConsumeLogs(context.Context, pdata.Logs) error`
   - In `ConsumeLogs()`:
     - For each log record:
       1. Generate UUID (using google/uuid or similar)
       2. Marshal the raw log record to bytes (using pdata encoding)
       3. Store `(UUID, raw_bytes)` in buffer via extension
       4. Add UUID as attribute to log record: `attributes[config.AttributeKey] = UUID`
     - Handle errors according to `SkipOnError` config
   - Implement `Start()`:
     - Look up buffer extension from component.Host
     - Verify extension is available

5. **Create tests**
   - Unit tests for UUID generation and attribute addition
   - Mock buffer for testing
   - Error handling tests
   - Integration tests with real buffer

---

### Phase 3: Processor - `samplingdecide`

**Directory Structure:**
```
processor/samplingdecide/
├── doc.go
├── factory.go
├── factory_test.go
├── config.go
├── config_test.go
├── processor.go
├── processor_test.go
├── metadata.yaml
├── Makefile
├── README.md
├── go.mod
├── go.sum
├── generated_component_test.go
├── generated_package_test.go
└── testdata/
    └── config.yaml
```

**Implementation Steps:**

1. **Create metadata.yaml**
   - Type: `samplingdecide`
   - Status: `development` stability for logs

2. **Create config.go**
   - Define `Config` struct with:
     - `Condition string` (required) - OTTL condition for sampling (e.g., `attributes["network.name"] == "Guest"`)
     - `SampleRate float64` (default: 1.0) - probability of sampling when condition matches (0.0-1.0)
     - `InvertMatch bool` (default: false) - invert the condition logic

3. **Create factory.go**
   - Similar to other processors
   - Use OTTL for condition parsing (see lsmintervalprocessor example)

4. **Create processor.go**
   - Implement `samplingDecideProcessor` struct:
     - OTTL statement parser for condition
     - Random number generator for sample rate
     - Configuration
     - Logger
   - Implement `processor.Logs` interface:
     - `ConsumeLogs(context.Context, pdata.Logs) error`
   - In `ConsumeLogs()`:
     - For each log record:
       1. Evaluate OTTL condition against log record
       2. If condition matches:
          - Check sample rate (random < config.SampleRate)
          - If sampled, keep the log
          - If not sampled, drop the log
       3. If condition doesn't match (and not inverted), drop the log
     - Return only logs that pass the sampling decision
   - Use OTTL contexts (ottllog) for condition evaluation

5. **Create tests**
   - Condition evaluation tests
   - Sample rate tests (probabilistic - may need multiple runs)
   - Invert logic tests
   - Integration tests

---

### Phase 4: Processor - `rawretriever`

**Directory Structure:**
```
processor/rawretriever/
├── doc.go
├── factory.go
├── factory_test.go
├── config.go
├── config_test.go
├── processor.go
├── processor_test.go
├── metadata.yaml
├── Makefile
├── README.md
├── go.mod
├── go.sum
├── generated_component_test.go
├── generated_package_test.go
└── testdata/
    └── config.yaml
```

**Implementation Steps:**

1. **Create metadata.yaml**
   - Type: `rawretriever`
   - Status: `development` stability for logs

2. **Create config.go**
   - Define `Config` struct with:
     - `AttributeKey string` (default: "raw.id") - where to read UUID from
     - `ExtensionName string` (required) - reference to rawsamplingbuffer extension
     - `RemoveAttribute bool` (default: true) - whether to remove UUID attribute after retrieval
     - `OnRetrievalError string` (default: "drop") - "drop", "keep_processed", "error"

3. **Create factory.go**
   - Similar pattern to rawcapture

4. **Create processor.go**
   - Implement `rawRetrieverProcessor` struct
   - Implement `processor.Logs` interface:
     - `ConsumeLogs(context.Context, pdata.Logs) error`
   - In `ConsumeLogs()`:
     - For each log record:
       1. Read UUID from `attributes[config.AttributeKey]`
       2. Retrieve raw log bytes from buffer using UUID
       3. Unmarshal raw bytes back to log record
       4. Replace the current (processed) log record with the raw log record
       5. Optionally remove the UUID attribute
     - Handle errors according to `OnRetrievalError` config:
       - "drop": remove the log record
       - "keep_processed": keep the processed log (don't replace)
       - "error": return error and fail the pipeline
   - Implement `Start()` to get buffer extension reference

5. **Create tests**
   - Retrieval and replacement tests
   - Error handling tests
   - Missing ID tests
   - Integration tests with buffer

---

### Phase 5: Integration and Testing

**Steps:**

1. **Update manifest.yaml**
   - Add all new components to `distributions/elastic-components/manifest.yaml`:
     ```yaml
     extensions:
       # ... existing extensions ...
       - gomod: github.com/elastic/opentelemetry-collector-components/extension/rawsamplingbuffer v0.0.1
         path: ../../extension/rawsamplingbuffer

     processors:
       # ... existing processors ...
       - gomod: github.com/elastic/opentelemetry-collector-components/processor/rawcapture v0.0.1
         path: ../../processor/rawcapture
       - gomod: github.com/elastic/opentelemetry-collector-components/processor/samplingdecide v0.0.1
         path: ../../processor/samplingdecide
       - gomod: github.com/elastic/opentelemetry-collector-components/processor/rawretriever v0.0.1
         path: ../../processor/rawretriever

     replaces:
       # ... existing replaces ...
       - github.com/elastic/opentelemetry-collector-components/extension/rawsamplingbuffer => ../extension/rawsamplingbuffer
       - github.com/elastic/opentelemetry-collector-components/processor/rawcapture => ../processor/rawcapture
       - github.com/elastic/opentelemetry-collector-components/processor/samplingdecide => ../processor/samplingdecide
       - github.com/elastic/opentelemetry-collector-components/processor/rawretriever => ../processor/rawretriever
     ```

2. **Add routing processor from contrib**
   - Add to manifest.yaml:
     ```yaml
     processors:
       # ... existing processors ...
       - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/processor/routingprocessor v0.138.0
     ```

3. **Create config.sampling.yaml**
   - Location: `distributions/elastic-components/config.sampling.yaml`
   - Based on `config.yaml` but with sampling pipeline
   - See detailed config below

4. **Build and test**
   ```bash
   # Build the collector
   TARGET_GOOS=linux CGO_ENABLED=0 make genelasticcol
   
   # Validate with sampling config
   ./_build/elastic-collector-components validate --config ./distributions/elastic-components/config.sampling.yaml
   
   # Build docker image
   make builddocker TAG=sampling-v0.1.0
   ```

5. **Integration testing**
   - Create test scripts to send logs
   - Verify raw logs are captured
   - Verify sampling decisions work correctly
   - Verify raw logs are retrieved and exported
   - Check buffer eviction and TTL behavior

---

### Phase 6: Configuration File - config.sampling.yaml

**File Location:** `distributions/elastic-components/config.sampling.yaml`

**Content:**
```yaml
extensions:
  health_check:
    endpoint: 0.0.0.0:13133
  pprof:
    endpoint: 0.0.0.0:1777
  basicauth/es:
    client_auth:
      username: elastic
      password: changeme
  
  # Raw sampling buffer extension
  rawsamplingbuffer:
    buffer_size: 10000
    max_entry_size: 1048576  # 1MB
    ttl: 5m
    eviction_policy: lru

receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  # == Custom Raw Sampling Processors ==
  
  rawcapture:
    attribute_key: raw.id
    extension_name: rawsamplingbuffer
    skip_on_error: false
  
  samplingdecide:
    # Example: Sample all logs from "Guest" network at 5% rate
    condition: 'attributes["network.name"] == "Guest"'
    sample_rate: 0.05
    invert_match: false
  
  rawretriever:
    attribute_key: raw.id
    extension_name: rawsamplingbuffer
    remove_attribute: true
    on_retrieval_error: drop
  
  # == Standard Processors ==
  
  batch:
    timeout: 1s
    send_batch_size: 1024
  
  resource:
    attributes:
      - key: deployment.environment
        value: production
        action: upsert
  
  attributes:
    actions:
      - key: environment
        value: production
        action: insert
  
  # Drop the raw.id attribute from production logs
  attributes/drop_id:
    actions:
      - key: raw.id
        action: delete
  
  transform:
    error_mode: ignore
    log_statements:
      - context: log
        statements:
          - set(attributes["processed"], true)
  
  # Routing processor to fork the pipeline
  routing:
    default_pipelines: []
    table:
      - statement: route()
        pipelines: [logs/prod, logs/sampling]

exporters:
  debug:
    verbosity: detailed
  
  # Production Elasticsearch exporter
  elasticsearch/prod:
    endpoints:
      - http://localhost:9200
    auth:
      authenticator: basicauth/es
    logs_index: logs-production
    timeout: 30s
    mapping:
      mode: ecs
  
  # Raw logs Elasticsearch exporter
  elasticsearch/raw:
    endpoints:
      - http://localhost:9200
    auth:
      authenticator: basicauth/es
    logs_index: logs-raw-samples
    timeout: 30s
    mapping:
      mode: ecs

service:
  extensions:
    - health_check
    - pprof
    - basicauth/es
    - rawsamplingbuffer
  
  pipelines:
    # == 1. Intake and Processing Pipeline ==
    logs/intake:
      receivers:
        - otlp
      processors:
        - rawcapture      # 1. Captures raw log, stores in buffer, adds raw.id
        - resource        # 2. Standard processing
        - attributes
        - transform
        - batch           # 3. Batch before routing
        - routing         # 4. Routes to both prod and sampling pipelines
      exporters: []       # Routing processor handles forwarding
    
    # == 2. Production Pipeline (Cleans and Exports) ==
    logs/prod:
      receivers:
        - routing         # Receives from routing processor
      processors:
        - attributes/drop_id  # Remove raw.id attribute
      exporters:
        - elasticsearch/prod
        - debug
    
    # == 3. Sampling Pipeline (Decides and Exports Raw) ==
    logs/sampling:
      receivers:
        - routing         # Receives from routing processor
      processors:
        - samplingdecide  # Evaluate condition, drop non-matches
        - rawretriever    # Retrieve raw log from buffer
      exporters:
        - elasticsearch/raw
        - debug

  telemetry:
    logs:
      level: info
```

---

## Technical Considerations

### 1. **Routing Processor Alternative**
   - The plan uses the standard `routingprocessor` from OTel contrib
   - An alternative is using connectors to bridge pipelines
   - Routing processor is simpler and more aligned with the original proposal

### 2. **Buffer Implementation**
   - Consider using a battle-tested data structure (e.g., `groupcache/lru` or `hashicorp/golang-lru`)
   - Implement proper memory limits to avoid OOM
   - Consider disk-backed buffer for larger deployments (using something like badger or pebble)

### 3. **UUID Generation**
   - Use `github.com/google/uuid` for UUID generation
   - Consider UUIDs vs ULIDs for better time-ordering
   - Store UUID as string in attributes

### 4. **OTTL Usage**
   - Use `ottllog` context for log condition evaluation
   - Reference `lsmintervalprocessor` for OTTL parser setup
   - Support complex conditions (AND/OR/NOT)

### 5. **Error Handling**
   - All processors should gracefully handle missing extensions
   - Buffer overflow should be handled (eviction policies)
   - Missing raw.id in retriever should be configurable (drop/keep/error)

### 6. **Performance**
   - Buffer operations should be thread-safe but efficient
   - Consider using sync.RWMutex for read-heavy workloads
   - Batch buffer operations where possible
   - Monitor memory usage with telemetry

### 7. **Testing Strategy**
   - Unit tests for each component
   - Integration tests with all components together
   - End-to-end test with full pipeline

---

## Implementation Order

1. **Week 1**: Extension `rawsamplingbuffer`
   - Core circular buffer implementation
   - Extension wrapper
   - Tests and documentation

2. **Week 2**: Processor `rawcapture`
   - Implement processor
   - Integration with buffer extension
   - Tests

3. **Week 3**: Processor `samplingdecide`
   - OTTL condition parsing
   - Sampling logic
   - Tests

4. **Week 4**: Processor `rawretriever`
   - Implement retrieval logic
   - Error handling
   - Tests

5. **Week 5**: Integration
   - Update manifest.yaml
   - Create config.sampling.yaml
   - Build collector
   - End-to-end testing

6. **Week 6**: Documentation and Refinement
   - README files for each component
   - Performance tuning
   - Bug fixes from testing

---

## Code Generation Commands

Each component will need:

```bash
# For extension
cd extension/rawsamplingbuffer
make test
make lint

# For processors
cd processor/rawcapture
make test
make lint

# Similar for other processors
```

---

## Dependencies to Add

### Extension: rawsamplingbuffer
```go
require (
    go.opentelemetry.io/collector/component v0.138.0
    go.opentelemetry.io/collector/extension v0.138.0
    go.uber.org/zap v1.27.0
    github.com/stretchr/testify v1.9.0
    // Potentially: github.com/hashicorp/golang-lru/v2 for buffer
)
```

### Processor: rawcapture
```go
require (
    go.opentelemetry.io/collector/component v0.138.0
    go.opentelemetry.io/collector/consumer v0.138.0
    go.opentelemetry.io/collector/processor v0.138.0
    go.opentelemetry.io/collector/pdata v1.44.0
    github.com/google/uuid v1.6.0
    github.com/elastic/opentelemetry-collector-components/extension/rawsamplingbuffer v0.0.1
)
```

### Processor: samplingdecide
```go
require (
    go.opentelemetry.io/collector/component v0.138.0
    go.opentelemetry.io/collector/consumer v0.138.0
    go.opentelemetry.io/collector/processor v0.138.0
    go.opentelemetry.io/collector/pdata v1.44.0
    github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl v0.138.0
    github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog v0.138.0
)
```

### Processor: rawretriever
```go
require (
    go.opentelemetry.io/collector/component v0.138.0
    go.opentelemetry.io/collector/consumer v0.138.0
    go.opentelemetry.io/collector/processor v0.138.0
    go.opentelemetry.io/collector/pdata v1.44.0
    github.com/elastic/opentelemetry-collector-components/extension/rawsamplingbuffer v0.0.1
)
```

---

## File Templates

### Common File Header
All files should include the Elastic license header (see existing files for template).

### Makefile Template
Each component should have a Makefile similar to existing ones:
```makefile
include ../../Makefile.Common
```

### go.mod Template
Each component needs its own go.mod:
```go
module github.com/elastic/opentelemetry-collector-components/[extension|processor]/[name]

go 1.23.0

// Include necessary dependencies
```

---

## Success Criteria

1. ✅ All components build successfully
2. ✅ All tests pass (unit + integration)
3. ✅ Collector builds with new components
4. ✅ Validation passes with config.sampling.yaml
5. ✅ End-to-end test demonstrates:
   - Raw logs captured and stored
   - Logs processed through standard pipeline
   - Production logs exported without raw.id
   - Sampling decisions work correctly
   - Raw logs retrieved and exported to raw index
6. ✅ Documentation complete

---

## Future Enhancements (Post-MVP)

1. **Persistent Buffer**: Use disk-backed storage for larger buffers
2. **Distributed Buffer**: Support for multi-instance deployments with shared buffer
3. **Advanced Sampling**: Support for rate limiting, adaptive sampling
4. **Buffer Compression**: Compress stored raw logs to save memory
5. **Metrics Dashboard**: Pre-built dashboards for monitoring sampling
6. **Dynamic Configuration**: Support for dynamic sampling rule updates
