# Dynamic Pipeline Creation from Processor Configs

## Overview

The `elasticpipelineextension` creates **dynamic pipelines** from processor configurations stored in Elasticsearch. You define only the processors you want, and the extension automatically creates a complete pipeline that bridges between routing connectors in the static configuration.

## Architecture

### Static Configuration (config.sampling.yaml)

The static config defines the **routing infrastructure** with connectors:

```yaml
connectors:
  # Entry point: routes incoming logs to dynamic pipelines or bypasses to sampling
  routing/stream_ingress:
    default_pipelines: [logs/sampling_decision]  # Bypass until dynamic pipelines loaded
    table:
      - statement: route()
        pipelines: [logs/sampling_decision]
  
  # Exit point: routes from dynamic pipelines back to sampling decision
  routing/stream_egress:
    default_pipelines: [logs/sampling_decision]
    table:
      - statement: route()
        pipelines: [logs/sampling_decision]

pipelines:
  # Intake pipeline - receives raw logs
  logs/intake:
    receivers: [otlp]
    processors: [rawcapture, resource, attributes]
    exporters: [routing/stream_ingress]  # Routes to dynamic processing
  
  # Sampling decision pipeline - receives from dynamic pipelines or directly from ingress
  logs/sampling_decision:
    receivers:
      - routing/stream_ingress  # Direct bypass when no dynamic pipelines
      - routing/stream_egress   # From dynamic pipelines
    processors: [transform, batch, samplingdecide]
    exporters: [routing/output_split]
```

### Dynamic Configuration (Elasticsearch)

The Elasticsearch document contains **only processor configurations**. The extension creates a complete pipeline from these:

```json
{
  "pipeline_id": "elastic-streams-pipeline",
  "agent": {
    "environment": "production",
    "cluster": "default",
    "labels": {
      "source": "elastic-streams"
    }
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

### What the Extension Creates

From the processor config above, the extension automatically creates:

```yaml
# Dynamic pipeline: logs/elastic-streams-pipeline
pipelines:
  logs/elastic-streams-pipeline:
    receivers: [routing/stream_ingress]
    processors: [transform/stream_processing]  # From Elasticsearch
    exporters: [routing/stream_egress]
```

This pipeline is created at runtime and bridges between the routing connectors.

## Data Flow

```
┌─────────────┐
│  OTLP Input │
└──────┬──────┘
       │
       v
┌──────────────────┐
│  logs/intake     │  Static pipeline
│  - rawcapture    │
│  - resource      │
│  - attributes    │
└──────┬───────────┘
       │
       v
┌────────────────────────┐
│ routing/stream_ingress │  Connector
└──────┬─────────────────┘
       │
       ├─ (default) ──────────────────────────────┐
       │                                           │
       ├─ (when dynamic pipeline loaded) ─────┐   │
       │                                       │   │
       v                                       v   v
┌──────────────────────────────────┐   ┌──────────────────────┐
│ logs/elastic-streams-pipeline    │   │ logs/sampling_decision │
│ (Created by extension)           │   │  (static)             │
│  - transform/stream_processing   │   └──────────────────────┘
└──────┬───────────────────────────┘
       │
       v
┌────────────────────────┐
│ routing/stream_egress  │  Connector
└──────┬─────────────────┘
       │
       v
┌──────────────────────┐
│ logs/sampling_decision │  Static pipeline
│  - transform         │
│  - batch             │
│  - samplingdecide    │
└──────┬───────────────┘
       │
       v
┌──────────────────┐
│  Output routing  │
└──────────────────┘
```

**Flow Explanation:**
1. Logs arrive at `logs/intake` → processed → sent to `routing/stream_ingress`
2. **Without dynamic pipelines:** Connector routes directly to `logs/sampling_decision` (bypass)
3. **With dynamic pipelines:** Connector routes to `logs/elastic-streams-pipeline` → dynamic processors apply transforms → outputs to `routing/stream_egress` → back to `logs/sampling_decision`

## How It Works

### 1. Extension Initialization

When the collector starts:
1. Static configuration is loaded and validated
2. The `elasticpipelineextension` starts (extensions start before pipelines)
3. Extension connects to Elasticsearch and fetches processor configurations
4. For each document, extension creates a dynamic pipeline

### 2. Dynamic Pipeline Creation

For a processor config like:
```json
{
  "pipeline_id": "my-pipeline",
  "config": {
    "processors": {
      "transform/custom": { ... },
      "batch/custom": { ... }
    }
  }
}
```

The extension creates:
1. A pipeline named `logs/my-pipeline`
2. With receivers: `["routing/stream_ingress"]`
3. With processors: `["transform/custom", "batch/custom"]` (created from the configs)
4. With exporters: `["routing/stream_egress"]`

### 3. Integration with Static Config

- The dynamic pipeline receives data from the `routing/stream_ingress` connector
- Applies the transform processors
- Sends results to the `routing/stream_egress` connector
- Which routes back to the static `logs/sampling_decision` pipeline

### 4. Routing Updates

The extension can update the routing connector's configuration to direct traffic to the new dynamic pipeline, or you can configure routing rules based on attributes/conditions.

## Elasticsearch Document Structure

### Document Format

```json
{
  "pipeline_id": "unique-pipeline-id",
  "agent": {
    "environment": "production",      // Optional: target environment
    "cluster": "default",              // Optional: target cluster
    "labels": {                        // Optional: custom labels for targeting
      "source": "elastic-streams"
    }
  },
  "config": {
    "processors": {
      "processor-name": {
        // Processor-specific configuration
        // Format depends on processor type (transform, batch, etc.)
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

**Note:** Only processor configurations are supported. Full pipeline definitions (with receivers, exporters, and pipeline sections) are not supported.

## Benefits

### 1. **Simplified Configuration**
- No need to define full pipelines in Elasticsearch
- Only specify the processor configs you want to customize
- Static config handles the pipeline structure

### 2. **Stable Infrastructure**
- Connectors defined once in static config
- Pipeline structure is predictable and version-controlled
- Less chance of misconfigurations

### 3. **Dynamic Business Logic**
- Processor configs can be updated in Elasticsearch
- Changes picked up by the extension's polling interval
- No collector restart required

### 4. **Clear Separation of Concerns**
- **Static**: Infrastructure (connectors, pipelines, routing)
- **Dynamic**: Business logic (transforms, enrichment, parsing)

## Example: Stream Processing

### Use Case
You want to apply different transforms to logs based on their stream name, and you want to update these transforms without restarting the collector.

### Static Config
```yaml
pipelines:
  logs/stream_processing:
    receivers: [routing/stream_ingress]
    processors: [transform/stream_processing]  # Dynamic
    exporters: [routing/stream_egress]
```

### Elasticsearch Document
```json
{
  "pipeline_id": "streams-v1",
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
            "conditions": ["attributes[\"stream.name\"] == \"logs\""],
            "statements": [
              "set(attributes[\"enriched\"], true)",
              "set(attributes[\"version\"], \"v1\")"
            ]
          }
        ]
      }
    }
  }
}
```

### Updating Logic

To update the transform logic:
1. Update the Elasticsearch document with new log_statements
2. Increment the `metadata.version`
3. Extension polls Elasticsearch (every `poll_interval`)
4. Extension detects new version and recreates the processor
5. New logs use the updated transform immediately

## Configuration Reference

### Extension Config
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
      poll_interval: 30s      # How often to check for updates
      cache_duration: 5m
      filters: []
    pipeline_management:
      namespace: elastic
      enable_health_reporting: true
      health_report_interval: 60s
      startup_timeout: 6s
      shutdown_timeout: 30s
      max_pipelines: 50
      max_components_per_pipeline: 20
      validate_configs: true
      dry_run_mode: false
```

### Key Settings

- `poll_interval`: How frequently to check Elasticsearch for updates (default: 30s)
- `cache_duration`: How long to cache fetched configs (default: 5m)
- `max_pipelines`: Maximum number of pipeline configs to load (default: 50)
- `validate_configs`: Whether to validate configs before applying (default: true)
- `dry_run_mode`: If true, logs what would be done without making changes (default: false)

## Migration Guide

### From Full Pipeline Definitions

**Before (Complex):**
```json
{
  "config": {
    "receivers": {
      "routing/stream_ingress": {}  // Error: Can't override connectors
    },
    "processors": {
      "transform/custom": { ... }
    },
    "exporters": {
      "routing/stream_egress": {}  // Error: Can't override connectors
    },
    "pipelines": {
      "logs/custom": {
        "receivers": ["routing/stream_ingress"],
        "processors": ["transform/custom"],
        "exporters": ["routing/stream_egress"]
      }
    }
  }
}
```

**After (Simple):**
```json
{
  "config": {
    "processors": {
      "transform/stream_processing": { ... }  // Just the processor config
    }
  }
}
```

**Static Config:**
```yaml
pipelines:
  logs/stream_processing:  # Define once in static config
    receivers: [routing/stream_ingress]
    processors: [transform/stream_processing]  # References dynamic processor
    exporters: [routing/stream_egress]
```

## Troubleshooting

### Processor Not Found

**Symptom:** Collector fails to start with "processor transform/stream_processing not found"

**Cause:** Extension hasn't loaded the processor before pipelines are built

**Solution:** 
- Check extension is in service.extensions list
- Verify Elasticsearch connection
- Check document exists in index
- Look for extension errors in logs

### Processor Not Updating

**Symptom:** Updated Elasticsearch config not reflected in collector behavior

**Causes:**
1. Cache duration not expired
2. Poll interval not reached
3. Version not incremented
4. Extension errors (check logs)

**Solution:**
- Increment `metadata.version` in Elasticsearch document
- Wait for `poll_interval` (default 30s)
- Check extension logs for errors
- Verify extension is running (health check)

### Connector Errors

**Symptom:** "connector routing/stream_egress not found"

**Cause:** Connector not defined in static config

**Solution:**
- Add connector definition to static config
- Connectors must be in static config, not Elasticsearch
- Don't try to create connectors dynamically

### Unsupported Configuration

**Symptom:** "no processors defined in configuration" or similar error

**Cause:** Trying to use full pipeline definitions instead of processor-only format

**Solution:**
- Use only the `processors` section in your Elasticsearch document
- Remove `receivers`, `exporters`, `connectors`, and `pipelines` sections
- Define pipeline structure in static config instead

## Best Practices

1. **Version Control Static Config**: Keep static config in git
2. **Version Elasticsearch Configs**: Use `metadata.version` for tracking
3. **Test Before Deploying**: Use `dry_run_mode: true` to validate
4. **Monitor Extension Health**: Enable health reporting and check endpoints
5. **Use Descriptive IDs**: Make `pipeline_id` meaningful
6. **Keep Processors Simple**: Each processor should do one thing well
7. **Document Transforms**: Add comments in log_statements for clarity

## Future Enhancements

Potential improvements to consider:

1. **Multiple Signal Types**: Support for metrics and traces processors
2. **Conditional Loading**: Load processors based on agent labels/environment
3. **Validation API**: Pre-validate configs before committing to Elasticsearch
4. **Rollback Support**: Automatic rollback if processor fails
5. **A/B Testing**: Load different processor versions for different percentages
