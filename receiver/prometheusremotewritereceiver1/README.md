# Prometheus Remote Write v1 Receiver

This receiver implements the [Prometheus Remote Write 1.0 specification](https://prometheus.io/docs/specs/prw/remote_write_spec/) and exposes an HTTP endpoint that accepts Prometheus remote write data.

| Status      |                                                     |
|-------------|-----------------------------------------------------|
| Stability   | alpha: metrics                                      |
| Pipelines   | metrics                                             |

## Overview

The Prometheus Remote Write protocol (v1) allows Prometheus and compatible agents to send metric data over HTTP in a compact binary format (Protocol Buffers + Snappy compression). This receiver accepts those writes and translates them into OpenTelemetry metrics (OTLP).

### What is supported

- All metric time series from Prometheus remote write senders
- `target_info` series are converted to resource attributes (`service.name`, `service.namespace`, `service.instance.id`)
- Metrics with a `_total` suffix are translated as monotonic cumulative **Sum** (counter) metrics
- All other metrics are translated as **Gauge** metrics
- Stale markers (`NaN 0x7ff0000000000002`) are translated to data points with `NoRecordedValue` set

### What is not supported

Because the v1 protocol does **not** carry metric metadata (type, unit, help text), the receiver cannot reconstruct:

- Prometheus Classic Histograms (bucket series arrive in separate write requests; atomicity cannot be guaranteed)
- Summaries (same atomicity issue as histograms)
- Metric units (no unit information in v1)
- Metric descriptions / help text

For full metadata support including native histograms, consider using a sender/receiver pair that implements [Prometheus Remote Write 2.0](https://prometheus.io/docs/specs/remote_write_spec_2_0/).

## Configuration

The receiver embeds [`confighttp.ServerConfig`](https://pkg.go.dev/go.opentelemetry.io/collector/config/confighttp#ServerConfig) for all HTTP server options (TLS, authentication, etc.).

```yaml
receivers:
  prometheusremotewrite:
    endpoint: 0.0.0.0:9090
```

### Full example with TLS

```yaml
receivers:
  prometheusremotewrite:
    endpoint: 0.0.0.0:9090
    tls:
      cert_file: /path/to/cert.pem
      key_file: /path/to/key.pem
```

## Configuring Prometheus to send to this receiver

Add a `remote_write` block to your `prometheus.yml`:

```yaml
remote_write:
  - url: "http://<collector-host>:9090/api/v1/write"
```

## Wire format

Per the [v1 specification](https://prometheus.io/docs/specs/prw/remote_write_spec/):

- **Path**: `/api/v1/write`
- **Method**: `POST`
- **Content-Type**: `application/x-protobuf`
- **Content-Encoding**: `snappy` (block format, not framed)
- **X-Prometheus-Remote-Write-Version**: `0.1.0`

The receiver validates the `Content-Type` header and decompresses the snappy-encoded protobuf body before processing.
