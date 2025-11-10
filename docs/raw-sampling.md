# Raw Log Sampling for OpenTelemetry Collector

This document describes the tail-based sampling implementation for logs in the Elastic OpenTelemetry Collector distribution.

## Overview

The raw sampling feature enables tail-based sampling of logs by:
1. Capturing raw logs before any processing
2. Applying standard transformations to logs for production use
3. Evaluating sampling decisions on processed logs
4. Retrieving and exporting the original unprocessed logs when they match sampling criteria

This is useful for scenarios where you want to:
- Send all logs through processing pipeline to production
- Sample only specific types of logs (e.g., errors) for detailed analysis
- Preserve the original raw format of sampled logs without transformations

## Architecture

### Components

#### 1. **Extension: `rawsamplingbuffer`**
An in-memory buffer that stores raw log data temporarily.

**Configuration:**
```yaml
rawsamplingbuffer:
  buffer_size: 10000          # Maximum number of entries
  max_entry_size: 1048576     # 1MB per entry
  ttl: 5m                     # Time-to-live for entries
  eviction_policy: lru        # Least Recently Used eviction
```

**Features:**
- Thread-safe circular buffer with LRU eviction
- Automatic cleanup via background goroutine (runs every TTL/2)
- Protocol Buffer serialization for efficient storage

#### 2. **Processor: `rawcapture`**
Captures the raw log before any processing occurs.

**Configuration:**
```yaml
rawcapture:
  attribute_key: raw.id       # Attribute name for UUID
  extension_name: rawsamplingbuffer
  skip_on_error: false
```

**Operation:**
1. Generates a UUID for the log record
2. Serializes the log to Protocol Buffer format
3. Stores in the buffer with UUID as key
4. Adds `raw.id` attribute to the log for later retrieval

#### 3. **Processor: `samplingdecide`**
Evaluates sampling criteria using OTTL (OpenTelemetry Transformation Language).

**Configuration:**
```yaml
samplingdecide:
  condition: 'severity_text == "ERROR" or attributes["level"] == "error"'
  sample_rate: 0.10           # 10% sampling rate
  invert_match: false         # false = keep matches, true = keep non-matches
```

**Operation:**
1. Evaluates OTTL condition against processed log
2. If condition matches, applies probabilistic sampling rate
3. Drops logs that don't meet criteria or sampling probability

#### 4. **Processor: `rawretriever`**
Retrieves the original raw log from the buffer.

**Configuration:**
```yaml
rawretriever:
  attribute_key: raw.id
  extension_name: rawsamplingbuffer
  remove_attribute: true      # Remove raw.id after retrieval
  on_retrieval_error: drop    # What to do if retrieval fails
```

**Operation:**
1. Reads `raw.id` attribute from the log
2. Retrieves original log from buffer using UUID
3. Replaces the processed log with the raw version
4. Optionally removes the `raw.id` attribute

### Pipeline Flow

```
┌─────────────┐
│ OTLP Logs   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────┐
│ Pipeline: logs/intake                   │
│ ┌─────────────────────────────────────┐ │
│ │ 1. rawcapture                       │ │ ← Stores raw + adds UUID
│ │ 2. resource (add deployment.env)    │ │
│ │ 3. attributes (add environment)     │ │
│ │ 4. transform (parse JSON, extract)  │ │ ← Modifies logs
│ │ 5. batch                            │ │
│ └─────────────────────────────────────┘ │
└──────┬──────────────────────────────────┘
       │
       ▼
┌────────────────┐
│ Routing        │ ← Forks to both pipelines
│ Connector      │
└───┬────────┬───┘
    │        │
    ▼        ▼
┌───────┐  ┌──────────────────────────┐
│ Prod  │  │ Sampling                 │
│       │  │ ┌──────────────────────┐ │
│ Drop  │  │ │ 1. samplingdecide    │ │ ← Filters
│ UUID  │  │ │ 2. rawretriever      │ │ ← Gets original
│       │  │ └──────────────────────┘ │
│  ↓    │  │          ↓               │
│ ES    │  │        Debug             │
└───────┘  └──────────────────────────┘
Processed     Original Raw Logs
```

## Building the Collector

### Prerequisites
- Go 1.24.0 or later
- OpenTelemetry Collector Builder (ocb)

### Build Steps

1. Navigate to the distribution directory:
   ```bash
   cd distributions/elastic-components
   ```

2. Build using the OpenTelemetry Collector Builder:
   ```bash
   ../../.tools/builder --config manifest.yaml
   ```

3. The binary will be created at:
   ```
   ./_build/elastic-collector-with-pipeline-extension
   ```

### Validate Configuration

```bash
./_build/elastic-collector-with-pipeline-extension validate --config config.sampling.yaml
```

### Run the Collector

```bash
./_build/elastic-collector-with-pipeline-extension --config config.sampling.yaml
```

## Testing

### Send a Test Log

Send an error log via OTLP HTTP endpoint:

```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
  "resourceLogs": [{
    "resource": {
      "attributes": [{
        "key": "service.name",
        "value": {"stringValue": "test-service"}
      }]
    },
    "scopeLogs": [{
      "scope": {
        "name": "test-scope"
      },
      "logRecords": [{
        "timeUnixNano": "1699632000000000000",
        "severityText": "ERROR",
        "body": {
          "stringValue": "{\"message\": \"Database connection failed\", \"level\": \"error\", \"user_id\": 12345}"
        },
        "attributes": [{
          "key": "http.method",
          "value": {"stringValue": "POST"}
        }]
      }]
    }]
  }]
}'
```

### Send a Non-Error Log (Won't be Sampled)

```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
  "resourceLogs": [{
    "resource": {
      "attributes": [{
        "key": "service.name",
        "value": {"stringValue": "test-service"}
      }]
    },
    "scopeLogs": [{
      "scope": {
        "name": "test-scope"
      },
      "logRecords": [{
        "timeUnixNano": "1699632000000000000",
        "severityText": "INFO",
        "body": {
          "stringValue": "{\"message\": \"Request completed successfully\", \"level\": \"info\", \"duration_ms\": 45}"
        }
      }]
    }]
  }]
}'
```

### Expected Behavior

#### Production Pipeline (`logs/prod`):
Both logs will be exported to Elasticsearch with:
- `processed=true` attribute
- `processed_at` timestamp
- JSON body parsed into attributes (`message`, `level`, `user_id`, etc.)
- `severity_text` extracted from body
- **No** `raw.id` attribute (removed by `attributes/drop_id`)
- Resource attribute: `deployment.environment=production`

#### Sampling Pipeline (`logs/sampling`):
Only the ERROR log will be exported to debug with:
- **Original raw format** (no transformations)
- **No** parsed attributes from JSON
- **No** `processed`, `processed_at` attributes
- **No** resource attributes added by pipeline
- Original body: `{"message": "Database connection failed", "level": "error", "user_id": 12345}`

The INFO log will be dropped by `samplingdecide` as it doesn't match the condition.

## Configuration Reference

### Complete Example

See `distributions/elastic-components/config.sampling.yaml` for a complete working configuration.

### Key Configuration Points

1. **Buffer Extension Must Be Listed First** in service extensions:
   ```yaml
   service:
     extensions:
       - rawsamplingbuffer  # Must be before processors use it
   ```

2. **Routing Connector** forks logs to multiple pipelines:
   ```yaml
   connectors:
     routing:
       table:
         - statement: route()
           pipelines: [logs/prod, logs/sampling]
   ```

3. **Order Matters** in intake pipeline processors:
   ```yaml
   processors:
     - rawcapture      # MUST be first to capture unmodified log
     - resource        # Transformations happen after capture
     - attributes
     - transform
     - batch
   ```

## Customization

### Change Sampling Condition

Modify the `samplingdecide` condition to match your criteria:

```yaml
# Sample high-severity logs
condition: 'severity_number >= SEVERITY_NUMBER_ERROR'

# Sample specific services
condition: 'resource.attributes["service.name"] == "payment-service"'

# Sample based on attributes
condition: 'attributes["user_tier"] == "premium"'

# Combine conditions
condition: 'severity_text == "ERROR" and attributes["network.name"] == "production"'
```

### Adjust Sampling Rate

Change the `sample_rate` to control what percentage of matching logs are sampled:

```yaml
samplingdecide:
  condition: 'severity_text == "ERROR"'
  sample_rate: 0.01  # Sample 1% of errors
```

### Change Buffer Size

Adjust buffer capacity based on your traffic volume:

```yaml
rawsamplingbuffer:
  buffer_size: 50000        # Increase for high-volume scenarios
  max_entry_size: 5242880   # 5MB for large log entries
  ttl: 10m                  # Keep entries longer
```

## Troubleshooting

### Logs Not Being Sampled

1. Check the condition syntax is valid OTTL
2. Verify the sampling rate (0.0 to 1.0)
3. Check buffer extension is started before processors
4. Review collector logs for errors

### Retrieval Failures

If `rawretriever` can't find logs:
- Buffer may be full (increase `buffer_size`)
- TTL may have expired (increase `ttl`)
- Check `raw.id` attribute exists on logs

### Performance Issues

- Reduce `buffer_size` if memory usage is high
- Decrease `ttl` to expire entries faster
- Adjust `sample_rate` to reduce volume

## Implementation Details

### Code Location

- **Extension**: `extension/rawsamplingbuffer/`
- **Buffer Logic**: `extension/rawsamplingbuffer/buffer/circular_buffer.go`
- **Processors**: 
  - `processor/rawcapture/`
  - `processor/samplingdecide/`
  - `processor/rawretriever/`

### Dependencies

- `github.com/google/uuid` - UUID generation
- `go.opentelemetry.io/collector/pdata/plog` - Log data model
- `github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl` - Condition evaluation

## Limitations

- Buffer is in-memory only (lost on restart)
- No distributed coordination (single collector instance)
- Buffer size must accommodate traffic volume and TTL
- Large log entries consume buffer capacity quickly

## Future Enhancements

Potential improvements:
- Persistent buffer backend (disk, Redis)
- Distributed buffer for multi-collector deployments
- Buffer metrics and monitoring
- Adaptive sampling rates based on traffic
- Support for trace/metric sampling
