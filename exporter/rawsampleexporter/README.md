# Raw Sample Exporter

This exporter sends OpenTelemetry logs to an Elasticsearch sample API endpoint as JSON arrays.

## Configuration

```yaml
exporters:
  rawsample:
    # Elasticsearch base URL
    endpoint: "http://localhost:9200"
    
    # Basic authentication credentials
    username: "elastic"
    password: "changeme"
    
    # Target index for the sample API
    index: "logs-raw-samples"
    
    # Maximum number of documents per batch (default: 100)
    max_batch_size: 100
    
    # HTTP request timeout (default: 30s)
    timeout: 30s
    
    # Retry configuration
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 300s
```

## How It Works

1. Receives OpenTelemetry logs from the pipeline
2. Serializes each log record using OTEL mode (objmodel.Document)
3. Batches serialized JSON documents into arrays
4. Sends batches to `{endpoint}/{index}/_sample/docs`
5. Uses Basic Auth for authentication

## API Format

The exporter sends logs as JSON arrays to the Elasticsearch sample API:

```
POST http://localhost:9200/logs-raw-samples/_sample/docs
Content-Type: application/json

[
  {"@timestamp": "2024-01-01T00:00:00Z", "message": "log1", ...},
  {"@timestamp": "2024-01-01T00:00:01Z", "message": "log2", ...}
]
```

## Use Case

This exporter is designed for tail-based log sampling scenarios where sampled raw logs need to be stored separately from processed logs. It integrates with the `rawsamplingbuffer` extension and related processors to capture and export raw log samples.
