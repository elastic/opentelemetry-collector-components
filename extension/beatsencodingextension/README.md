# Beats Encoding Extension

The Beats encoding extension converts raw log bytes into OpenTelemetry log records formatted for Elastic Beats/Agent integration compatibility. It implements the `encoding.LogsUnmarshalerExtension` interface and is intended for use with receivers that accept raw payloads (e.g., `httpreceiver`).

Each extracted record is stored as a raw string under a configurable body map key (default: `message`). Data stream routing attributes (`data_stream.type`, `data_stream.dataset`, `data_stream.namespace`) are set on each log record so that mOTLP routes the document to the correct integration data stream.

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `format` | string | `json` | Input format: `json`, `ndjson`, or `text`. |
| `unwrap` | string | _(empty)_ | JSONPath expression to extract records from a wrapper structure. Only used with `json` format. |
| `target_field` | string | `message` | Body map key where each record's raw content is stored. |
| `routing.dataset` | string | _(required)_ | Data stream dataset (e.g., `azure.activitylogs`). |
| `routing.namespace` | string | `default` | Data stream namespace. |

### Formats

- **`json`** — The entire input is a JSON document. When `unwrap` is set, the JSONPath expression extracts individual records from a wrapper structure (e.g., `$.records[*]` for Azure Diagnostic Settings, `$.Records[*]` for AWS CloudTrail). When `unwrap` is empty, the entire input is treated as a single record.
- **`ndjson`** — Newline-delimited JSON. Each non-empty line becomes a separate log record.
- **`text`** — Newline-delimited text. Each non-empty line becomes a separate log record.

### Examples

#### Azure Diagnostic Settings (JSON with unwrap)

```yaml
extensions:
  beats_encoding/azure:
    format: json
    unwrap: "$.records[*]"
    routing:
      dataset: azure.activitylogs

receivers:
  httpreceiver/azure:
    encoding: beats_encoding/azure

service:
  extensions: [beats_encoding/azure]
  pipelines:
    logs:
      receivers: [httpreceiver/azure]
```

#### AWS CloudTrail (JSON with unwrap)

```yaml
extensions:
  beats_encoding/cloudtrail:
    format: json
    unwrap: "$.Records[*]"
    routing:
      dataset: aws.cloudtrail

service:
  extensions: [beats_encoding/cloudtrail]
```

#### Newline-delimited JSON

```yaml
extensions:
  beats_encoding/ndjson:
    format: ndjson
    routing:
      dataset: custom.app

service:
  extensions: [beats_encoding/ndjson]
```

#### Plain text logs

```yaml
extensions:
  beats_encoding/text:
    format: text
    routing:
      dataset: generic

service:
  extensions: [beats_encoding/text]
```
