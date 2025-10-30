# Elastic Pipeline Extension Distribution

This directory contains configuration files and distribution manifests for the OpenTelemetry Collector with the Elastic Pipeline Extension.

## Files

### Configuration Files

1. **`sample-config.yaml`** - Complete production-ready configuration with:
   - Elastic Pipeline Extension configured for localhost:9200 with elastic:changeme credentials
   - Multiple receivers (OTLP, Prometheus, Host Metrics)
   - Various processors for data transformation
   - Elasticsearch exporters for traces, metrics, and logs
   - Health check and authentication extensions

2. **`minimal-config.yaml`** - Minimal testing configuration with:
   - Basic OTLP receiver
   - Elastic Pipeline Extension with localhost:9200 connection
   - Logging exporter for debugging

### Distribution Manifests

1. **`manifest.yaml`** - Full Elastic distribution with all components
2. **`simple-manifest.yaml`** - Simplified distribution with core components and the pipeline extension

## Using the Elastic Pipeline Extension

The `elasticpipelineextension` provides dynamic pipeline management capabilities:

### Key Features

1. **Dynamic Configuration**: Modify OpenTelemetry collector pipelines at runtime without restarts
2. **Elasticsearch Integration**: Store and retrieve pipeline configurations from Elasticsearch
3. **Signal-Type Separation**: Manages traces, metrics, and logs pipelines independently
4. **HTTP API**: RESTful API for pipeline management operations
5. **Authentication**: Built-in authentication for secure access

### Configuration Options

```yaml
extensions:
  elasticpipeline:
    # HTTP server configuration
    endpoint: "localhost:8080"
    
    # Authentication (optional)
    auth:
      type: "basic"
      username: "admin"
      password: "secure123"
    
    # Elasticsearch connection
    elasticsearch:
      endpoints: ["http://localhost:9200"]
      username: "elastic"
      password: "changeme"
      timeout: 30s
      retry_on_failure:
        enabled: true
        max_retries: 3
        initial_interval: 1s
        max_interval: 5s
    
    # Configuration polling
    poll_interval: 30s
    
    # Default pipeline configurations
    default_pipelines:
      traces:
        receivers: [otlp]
        processors: [batch]
        exporters: [elasticsearch/traces]
```

### API Endpoints

When the extension is running, it exposes these HTTP endpoints:

- `GET /health` - Health check
- `GET /pipelines` - Get all pipeline configurations
- `GET /pipelines/{signal}` - Get configuration for specific signal type
- `POST /pipelines/{signal}` - Update configuration for specific signal type
- `DELETE /pipelines/{signal}` - Remove configuration for specific signal type

### Running with Docker

```bash
# Start Elasticsearch
docker run -d --name elasticsearch \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "ELASTIC_PASSWORD=changeme" \
  -e "xpack.security.enabled=true" \
  docker.elastic.co/elasticsearch/elasticsearch:8.15.0

# Run OpenTelemetry Collector with the configuration
./otelcol --config=minimal-config.yaml
```

### Example API Usage

```bash
# Check extension health
curl http://localhost:8080/health

# Get current pipeline configurations
curl http://localhost:8080/pipelines

# Update metrics pipeline
curl -X POST http://localhost:8080/pipelines/metrics \
  -H "Content-Type: application/json" \
  -d '{
    "receivers": ["otlp", "prometheus"],
    "processors": ["batch", "resourcedetection"],
    "exporters": ["elasticsearch/metrics"]
  }'
```

## Prerequisites

1. **Elasticsearch**: Running instance at localhost:9200 with authentication
2. **OpenTelemetry Collector**: Custom build including the elasticpipelineextension
3. **Go 1.21+**: For building custom distributions

## Building Custom Distribution

To build a custom collector with the pipeline extension:

```bash
# Install OpenTelemetry Collector Builder
go install go.opentelemetry.io/collector/cmd/builder@latest

# Build the distribution
builder --config simple-manifest.yaml
```

## Troubleshooting

### Common Issues

1. **Connection to Elasticsearch fails**
   - Verify Elasticsearch is running on localhost:9200
   - Check credentials (elastic:changeme)
   - Ensure network connectivity

2. **Extension fails to start**
   - Check port 8080 is available
   - Verify configuration syntax
   - Review collector logs

3. **Pipeline updates not applying**
   - Check API endpoints are accessible
   - Verify authentication if enabled
   - Monitor extension logs for errors

### Debug Mode

Enable debug logging in the collector configuration:

```yaml
service:
  telemetry:
    logs:
      level: "debug"
```

### Logs Location

The extension logs are integrated with the main collector logs. Look for entries prefixed with `elasticpipelineextension`.

## Security Considerations

1. **Authentication**: Always enable authentication in production
2. **Network Security**: Restrict access to extension ports
3. **Elasticsearch Security**: Use proper Elasticsearch authentication
4. **TLS**: Enable TLS for production deployments

## Support

For issues related to the Elastic Pipeline Extension, please check:

1. Extension logs for error messages
2. Elasticsearch connectivity
3. Configuration file syntax
4. API endpoint accessibility

The list of components included in the public Elastic OpenTelemetry collector can be found [here](https://github.com/elastic/elastic-agent/blob/main/internal/pkg/otel/README.md).

## Contributing

All components within this repository should be added in this distribution's manifest in order to ensure its integration with any OpenTelemetry Collector binary. For example, to add a receiver component named `customreceiver` and located in the root repository directory `./receiver/customreceiver`, append the following entries into the [manifest.yaml](./manifest.yaml) file:

```
receivers:
  - gomod: github.com/elastic/opentelemetry-collector-components/receiver/customreceiver v0.0.1

...
replaces:
  - github.com/open-telemetry/opentelemetry-collector-contrib/receiver/customreceiver => ../receiver/customreceiver
```

Finally, add the component into the example [distribution's configuration file](./config.yaml).
