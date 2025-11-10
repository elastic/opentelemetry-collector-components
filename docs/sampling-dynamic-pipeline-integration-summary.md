# Sampling + Dynamic Pipeline Integration - Summary

## Overview

This integration combines three powerful capabilities:
1. **Raw Sampling** - Capture original data before processing for sampling decisions
2. **Dynamic Pipelines** - Load stream processing logic from Elasticsearch at runtime
3. **Dynamic Sampling Rules** - Fetch sampling configuration from Elasticsearch

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         TELEMETRY FLOW                                │
└──────────────────────────────────────────────────────────────────────┘

OTLP Input
    │
    ▼
┌────────────────────────────────────────────────────────────────────┐
│ STAGE 1: INTAKE (Static)                                           │
│ - rawcapture: Store original data in buffer                        │
│ - resource/attributes: Basic enrichment                            │
│ - routing/stream_ingress: Route to stream processing              │
└────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌────────────────────────────────────────────────────────────────────┐
│ STAGE 2: STREAM PROCESSING (Dynamic from .otel-pipeline-config)   │
│ - Custom transforms per stream                                     │
│ - Parent/child stream routing                                      │
│ - Stream-specific enrichment                                       │
│ - routing/stream_egress: Exit back to static flow                 │
└────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌────────────────────────────────────────────────────────────────────┐
│ STAGE 3: SAMPLING DECISION (Static)                               │
│ - transform: Post-processing                                       │
│ - batch: Batching                                                  │
│ - samplingdecide: Query rules from .elastic-sampling-config       │
│ - routing/output_split: Split sampled vs all logs                 │
└────────────────────────────────────────────────────────────────────┘
    │
    ├─────────────────────┬──────────────────────┐
    ▼                     ▼                      ▼
┌─────────────┐  ┌──────────────┐     ┌─────────────────┐
│ SAMPLED     │  │ PRODUCTION   │     │ NOT SAMPLED     │
│ rawretriever│  │ attributes/  │     │ (dropped by     │
│ rawsample   │  │ elasticsearch│     │ samplingdecide) │
└─────────────┘  └──────────────┘     └─────────────────┘
```

## Key Features

### 1. Raw Capture Before Processing
- Original logs stored in buffer before any transforms
- Stream processing can modify logs freely
- Sampling exports original unmodified data
- Production gets fully processed logs

### 2. Dynamic Stream Processing
- Pipeline definitions stored in `.otel-pipeline-config` Elasticsearch index
- Loaded at runtime by `elasticpipelineextension`
- Automatic transformation to integrate with static flow
- Support for complex routing and parent/child streams

### 3. Dynamic Sampling Rules
- Sampling rules stored in `.elastic-sampling-config` Elasticsearch index
- Polled by `samplingconfigextension`
- Rules can match on stream name and resource attributes
- Priority-based rule evaluation

## Configuration Files

### 1. Static Config (`config.sampling.yaml`)

**Extensions**:
- `rawsamplingbuffer` - Stores raw data
- `samplingconfigextension` - Fetches sampling rules
- `elasticpipelineextension` - Loads dynamic pipelines

**Connectors**:
- `routing/stream_ingress` - Entry to dynamic pipelines
- `routing/stream_egress` - Exit from dynamic pipelines
- `routing/output_split` - Split to sampling/production paths

**Pipelines**:
- `logs/intake` - Initial capture and enrichment
- `logs/sampling_decision` - Sampling logic application
- `logs/sampling` - Raw sample export
- `logs/prod` - Production export

### 2. Dynamic Pipeline Config (`.otel-pipeline-config` index)

Generated in **streaming mode** with:
- Entry pipelines receive from `routing/stream_ingress`
- Exit pipelines export to `routing/stream_egress`
- All internal connectors prefixed with `stream_`
- No standalone receivers/exporters

Example structure:
```json
{
  "pipeline_id": "elastic-streams-pipeline",
  "config": {
    "processors": { /* stream-specific transforms */ },
    "connectors": { /* stream_routing/* connectors */ },
    "service": {
      "pipelines": {
        "logs/stream_logs": {
          "receivers": ["routing/stream_ingress"],
          "processors": ["transform/logs_metadata"],
          "exporters": ["stream_routing/logs"]
        },
        "logs/stream_output": {
          "receivers": ["stream_routing/logs"],
          "exporters": ["routing/stream_egress"]
        }
      }
    }
  }
}
```

### 3. Sampling Rules (`.elastic-sampling-config` index)

Example:
```json
{
  "id": "sample-errors-high",
  "enabled": true,
  "priority": 100,
  "match": {
    "stream": "logs.application.*",
    "resource_attrs": {
      "deployment.environment": "production"
    }
  },
  "sample_rate": 1.0
}
```

## Data Flow Example

### Scenario: Application log with error

1. **OTLP receives log**: `{"body": "Error connecting to database", "level": "error"}`

2. **logs/intake**:
   - `rawcapture`: Stores in buffer → `{raw.id: "abc123", body: "Error...", level: "error"}`
   - `resource`: Adds → `{deployment.environment: "production"}`
   - Routes to `routing/stream_ingress`

3. **Dynamic Pipeline** (logs/stream_logs):
   - `transform/logs_metadata`: Sets → `{stream.name: "logs.application"}`
   - Internal routing processes child streams
   - Routes to `routing/stream_egress`

4. **logs/sampling_decision**:
   - `transform`: Parses, enriches → `{severity_text: "ERROR", ...}`
   - `samplingdecide`: 
     - Queries `samplingconfigextension`
     - Finds rule: "logs.application.*" in production → sample_rate: 1.0
     - Keeps log, adds → `{sampled: true}`
   - Routes to `routing/output_split`

5. **Split Output**:
   - **logs/sampling** (because sampled=true):
     - `rawretriever`: Gets original from buffer → `{"body": "Error...", "level": "error"}`
     - `rawsample`: Exports to sample API
   
   - **logs/prod** (always):
     - `attributes/drop_id`: Removes raw.id
     - `elasticsearch/prod`: Indexes → `{stream.name: "logs.application", severity_text: "ERROR", ...}`

## Implementation Steps

### Phase 1: Extension Updates
1. Add integration mode to `elasticpipelineextension` config
2. Implement pipeline transformation logic
3. Add connector validation
4. Update manager to use streaming mode
5. Add tests for transformation logic

### Phase 2: Pipeline Generator Updates
1. Add streaming mode to generator
2. Implement component prefixing
3. Add entry/exit pipeline detection
4. Implement receiver/exporter filtering
5. Add validation for streaming configs
6. Update tests for both modes

### Phase 3: Configuration Deployment
1. Deploy updated static config (`config.sampling.yaml`)
2. Update collector with new extensions
3. Generate and index dynamic pipelines in streaming mode
4. Configure sampling rules
5. Monitor and validate flow

## Monitoring & Validation

### Metrics to Watch
- `rawsamplingbuffer_entries` - Buffer usage
- `samplingdecide_evaluated` - Sampling decisions
- `rawretriever_retrieved` - Raw log retrievals
- Pipeline-specific metrics for each stage

### Validation Steps
1. Send test log via OTLP
2. Verify raw capture in buffer
3. Verify stream processing applied
4. Verify sampling decision made
5. Verify raw sample exported (if sampled)
6. Verify processed log in Elasticsearch

## Rollback Plan

If issues occur:
1. Set `elasticpipeline.integration.mode = "standalone"` (disables stream processing)
2. Remove dynamic pipeline documents from `.otel-pipeline-config`
3. Logs will flow directly: intake → sampling_decision → sampling/prod
4. Stream processing bypassed, but sampling still works

## Benefits

✅ **Flexibility**: Change stream processing without collector restarts  
✅ **Accuracy**: Sampling uses original data, production uses processed data  
✅ **Scalability**: Dynamic rules + pipelines scale independently  
✅ **Observability**: Clear stages with independent metrics  
✅ **Safety**: Validation before applying, dry-run mode available  
✅ **Backward Compatible**: Can disable dynamic pipelines if needed  

## Related Documentation

- [Elastic Pipeline Extension - Sampling Integration](./elasticpipelineextension-sampling-integration.md)
- [Pipeline Generation - Sampling Integration](./pipeline-generation-sampling-integration.md)
- [Raw Sampling Implementation Plan](../raw_sampling_implementation_plan.md)
