# Raw Capture Processor

This processor captures raw log records and stores them in a buffer for later retrieval in tail-based sampling scenarios.

## Configuration

```yaml
processors:
  rawcapture:
    attribute_key: raw.id          # Attribute to store UUID (default: raw.id)
    extension_name: rawsamplingbuffer  # Name of buffer extension (required)
    skip_on_error: false           # Continue on errors (default: false)
```

## How It Works

1. For each log record:
   - Generates a unique UUID
   - Clones the raw log record
   - Marshals it to bytes
   - Stores it in the buffer with the UUID
   - Adds the UUID as an attribute to the log record

2. The log record continues through the pipeline with the UUID attribute

## Usage

Must be used with the `rawsamplingbuffer` extension:

```yaml
extensions:
  rawsamplingbuffer:
    buffer_size: 10000

processors:
  rawcapture:
    extension_name: rawsamplingbuffer

service:
  extensions:
    - rawsamplingbuffer
  pipelines:
    logs:
      processors:
        - rawcapture
```
