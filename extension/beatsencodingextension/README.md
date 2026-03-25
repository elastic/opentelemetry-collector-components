# Beats Encoding Extension

The Beats encoding extension converts raw log bytes into OpenTelemetry log records formatted for Elastic Beats/Agent integration compatibility. It implements the `encoding.LogsUnmarshalerExtension` and `encoding.LogsDecoderExtension` (streaming) interfaces and is intended for use with receivers that accept raw payloads (e.g., `httpreceiver`).

Each extracted record is stored as a raw string under the `message` body map key. Data stream routing attributes (`data_stream.type`, `data_stream.dataset`, `data_stream.namespace`) are set on each log record so that mOTLP routes the document to the correct integration data stream.

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `format` | string | `json` | Input format: `json` or `text`. |
| `unwrap` | string | _(empty)_ | Restricted path expression to extract records from a wrapper structure (e.g., `$.records[*]`). Only `$.key1.key2...keyN[*]` is supported. Only used with `json` format. |
| `data_stream.dataset` | string | _(required)_ | Data stream dataset (e.g., `azure.activitylogs`). |
| `data_stream.namespace` | string | `default` | Data stream namespace. |
| `input_type` | string | _(empty)_ | Sets the `input.type` field in the log record body (e.g., `aws-s3`, `azure-eventhub`). |
| `tags` | []string | _(empty)_ | List of strings appended to the `tags` field in the log record body (e.g., `["forwarded", "aws-cloudtrail"]`). |
| `fields` | map[string]any | _(empty)_ | Key-value pairs added to every log record body (e.g., `{environment: production}`). |

### Formats

- **`json`** — The entire input is a JSON document. When `unwrap` is set, the JSONPath expression extracts individual records from a wrapper structure (e.g., `$.records[*]` for Azure Diagnostic Settings, `$.Records[*]` for AWS CloudTrail). When `unwrap` is empty, the entire input is treated as a single record.
- **`text`** — Newline-delimited text. Each non-empty line becomes a separate log record.

### Examples

#### Azure Diagnostic Settings (JSON with unwrap)

```yaml
extensions:
  beats_encoding/azure:
    format: json
    unwrap: "$.records[*]"
    data_stream:
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
    data_stream:
      dataset: aws.cloudtrail

service:
  extensions: [beats_encoding/cloudtrail]
```

#### AWS VPC Flow Logs with custom fields

```yaml
extensions:
  beats_encoding/vpcflow:
    format: text
    data_stream:
      dataset: aws.vpcflow
    input_type: aws-s3
    tags: ["forwarded", "aws-vpcflow"]
    fields:
      environment: production
      team: security

service:
  extensions: [beats_encoding/vpcflow]
```

#### Plain text logs

```yaml
extensions:
  beats_encoding/text:
    format: text
    data_stream:
      dataset: generic

service:
  extensions: [beats_encoding/text]
```
