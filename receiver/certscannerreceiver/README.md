# Certificate Scanner Receiver

| Status        |                   |
|---------------|-------------------|
| Stability     | [development]: logs |
| Distributions | [] |

[development]: https://github.com/open-telemetry/opentelemetry-collector#development

A receiver that discovers TLS-enabled services listening on the local host, performs handshakes to extract certificate metadata, and emits the data as logs (with optional metrics).

## Features

- **Automatic Port Discovery** - Detects listening TCP ports via `/proc/net/tcp` (Linux) or `netstat` (Windows/macOS)
- **TLS Certificate Extraction** - Performs TLS handshakes and extracts full certificate chain metadata
- **ECS-Aligned Output** - Log records follow Elastic Common Schema for seamless Elasticsearch integration
- **Configurable Filtering** - Include/exclude port lists, scan all mode, configurable intervals

## Configuration

```yaml
receivers:
  certscanner:
    # How often to scan (default: 1h, minimum: 1m)
    interval: 1h

    # Connection timeout per port (default: 3s)
    timeout: 3s

    # Port filtering
    ports:
      # Explicit list to scan (required when scan_all is false)
      include: [443, 8443, 636, 9200]
      # Ports to skip
      exclude: [22]
      # If true, scan all listening ports not in exclude list
      scan_all: false

```

## Output

### Log Record (ECS-aligned)

```json
{
  "Timestamp": "2024-01-15T10:30:00Z",
  "SeverityText": "INFO",
  "Body": "TLS certificate discovered",
  "Attributes": {
    "observer.type": "certscanner",
    "observer.hostname": "webserver-01",
    "network.transport": "tcp",
    "destination.port": 443,
    "destination.address": "127.0.0.1",
    "tls.version": "TLS 1.3",
    "tls.cipher": "TLS_AES_256_GCM_SHA384",
    "tls.server.x509.subject.common_name": "*.example.com",
    "tls.server.x509.subject.distinguished_name": "CN=*.example.com,O=Example Inc,C=US",
    "tls.server.x509.issuer.common_name": "DigiCert TLS RSA SHA256 2020 CA1",
    "tls.server.x509.serial_number": "0A:1B:2C:3D:4E:5F",
    "tls.server.x509.not_before": "2024-01-01T00:00:00Z",
    "tls.server.x509.not_after": "2025-01-01T00:00:00Z",
    "tls.server.hash.sha256": "2F:4E:7A:...",
    "tls.server.x509.alternative_names": ["*.example.com", "example.com"],
    "tls.server.x509.alternative_ip": ["1.2.3.4"],
    "tls.server.x509.alternative_email": ["admin@example.com"],
    "tls.server.certificate.chain_depth": 3
  }
}
```

## Platform Support

| Platform | Port Discovery Method |
|----------|----------------------|
| Linux    | `/proc/net/tcp` and `/proc/net/tcp6` |
| Windows  | `netstat -ano -p TCP` |
| macOS/BSD | `netstat -an -p tcp` |
