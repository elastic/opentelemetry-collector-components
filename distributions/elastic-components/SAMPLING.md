# Raw Sampling Quick Reference

## What It Does

Tail-based sampling for logs that:
1. ✅ Sends ALL logs to production (processed with transformations)
2. ✅ Samples only ERROR logs to a separate output (as raw, unprocessed format)
3. ✅ Preserves original log format for sampled logs
4. ✅ **NEW: Dynamic sampling rules** - Update sampling conditions without restarts!

## Features

### Static Configuration
- Define sampling rules in YAML config
- Requires collector restart to update

### Dynamic Configuration (NEW!)
- Store sampling rules in Elasticsearch
- Update rules in real-time without restarting collector
- Configure different rates per stream/index
- See [DYNAMIC_SAMPLING.md](./DYNAMIC_SAMPLING.md) for full guide

## Quick Start

```bash
# 1. Build
cd distributions/elastic-components
go run go.opentelemetry.io/collector/cmd/builder@v0.138.0 --config manifest.yaml

# 2. Validate config
./_build/elastic-collector-with-pipeline-extension validate --config config.sampling.yaml

# 3. Run
./_build/elastic-collector-with-pipeline-extension --config config.sampling.yaml

# 4. Test (in another terminal)
./test-sampling.sh both
```

## Pipeline Flow

```
OTLP Logs → rawcapture → processing → batch → routing connector
                  ↓                              ↓         ↓
            Stored in buffer              logs/prod   logs/sampling
                                              ↓             ↓
                                    Drop raw.id      samplingdecide
                                              ↓             ↓
                                         Processed    rawretriever
                                          (JSON parsed)    ↓
                                              ↓        Raw original
                                              ↓             ↓
                                        Elasticsearch   Debug
```

## What Gets Modified

**Production Pipeline** (all logs):
- ✅ JSON body parsed into attributes
- ✅ `severity_text` extracted from body
- ✅ `processed=true` attribute added
- ✅ `processed_at` timestamp added
- ✅ `deployment.environment=production` resource attribute
- ❌ `raw.id` removed

**Sampling Pipeline** (ERROR logs only, 10% rate):
- ✅ Original raw log format
- ❌ No JSON parsing
- ❌ No processed attributes
- ❌ No resource attributes

## Test Commands

### Send Error (will be sampled at 10%)
```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
  "resourceLogs": [{
    "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "test"}}]},
    "scopeLogs": [{
      "scope": {"name": "test"},
      "logRecords": [{
        "timeUnixNano": "1699632000000000000",
        "severityText": "ERROR",
        "body": {"stringValue": "{\"message\": \"Error occurred\", \"level\": \"error\"}"}
      }]
    }]
  }]
}'
```

### Send Info (will NOT be sampled)
```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
  "resourceLogs": [{
    "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "test"}}]},
    "scopeLogs": [{
      "scope": {"name": "test"},
      "logRecords": [{
        "timeUnixNano": "1699632000000000000",
        "severityText": "INFO",
        "body": {"stringValue": "{\"message\": \"All good\", \"level\": \"info\"}"}
      }]
    }]
  }]
}'
```

## Customization

### Change Sampling Condition
Edit `config.sampling.yaml`:
```yaml
samplingdecide:
  # Current: Sample ERROR logs
  condition: 'severity_text == "ERROR" or attributes["level"] == "error"'
  
  # Other examples:
  # condition: 'severity_number >= SEVERITY_NUMBER_WARN'
  # condition: 'resource.attributes["service.name"] == "critical-service"'
  # condition: 'attributes["user_tier"] == "premium"'
```

### Change Sampling Rate

**Static (requires restart):**
```yaml
samplingdecide:
  condition: 'severity_text == "ERROR"'
  sample_rate: 0.01  # 1% instead of 10%
```

**Dynamic (no restart):**
```bash
# Update rule in Elasticsearch
curl -X POST "http://localhost:9200/.elastic-sampling-config/_update/my-rule" \
  -H "Content-Type: application/json" \
  -u elastic:password \
  -d '{
    "doc": {
      "sample_rate": 0.01
    }
  }'
# Wait 30 seconds for collector to pick up change
```

See [DYNAMIC_SAMPLING.md](./DYNAMIC_SAMPLING.md) for complete guide.

### Change Buffer Size
```yaml
rawsamplingbuffer:
  buffer_size: 50000  # Default: 10000
  ttl: 10m            # Default: 5m
```

## Components

| Component | Purpose |
|-----------|---------|
| `rawsamplingbuffer` (extension) | In-memory buffer to store raw logs |
| `rawcapture` (processor) | Captures raw log before processing |
| `samplingdecide` (processor) | Evaluates OTTL condition & sampling rate |
| `rawretriever` (processor) | Retrieves original log from buffer |
| `routing` (connector) | Forks logs to prod & sampling pipelines |

## See Also

- Full documentation: [../../docs/raw-sampling.md](../../docs/raw-sampling.md)
- Main README: [README.md](README.md)
