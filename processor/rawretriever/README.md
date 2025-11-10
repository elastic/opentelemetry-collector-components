# Raw Retriever Processor

This processor retrieves raw log records from a buffer and replaces processed logs with their original raw versions.

## Configuration

```yaml
processors:
  rawretriever:
    attribute_key: raw.id          # Attribute containing UUID (default: raw.id)
    extension_name: rawsamplingbuffer  # Name of buffer extension (required)
    remove_attribute: true         # Remove UUID after retrieval (default: true)
    on_retrieval_error: drop       # Error handling: drop|keep_processed|error (default: drop)
```

## How It Works

1. For each log record:
   - Reads the UUID from the configured attribute
   - Retrieves the raw log data from the buffer using the UUID
   - Unmarshals the raw log
   - Replaces the processed log with the raw log
   - Optionally removes the UUID attribute

2. Error handling options:
   - `drop`: Drop logs that can't be retrieved
   - `keep_processed`: Keep the processed version if raw can't be retrieved
   - `error`: Fail the pipeline if retrieval fails

## Usage

Used in the sampling pipeline after filtering to retrieve raw versions:

```yaml
extensions:
  rawsamplingbuffer:
    buffer_size: 10000

processors:
  rawretriever:
    extension_name: rawsamplingbuffer
    remove_attribute: true
    on_retrieval_error: drop

service:
  extensions:
    - rawsamplingbuffer
  pipelines:
    logs/sampling:
      processors:
        - samplingdecide
        - rawretriever
```
