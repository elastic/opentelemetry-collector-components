# Beats Encoding Extension

The Beats encoding extension converts raw log bytes into OpenTelemetry log records formatted for Elastic Beats/Agent integration compatibility. It implements the `encoding.LogsUnmarshalerExtension` and `encoding.LogsDecoderExtension` (streaming) interfaces and is intended for use with receivers that accept raw payloads (e.g., `httpreceiver`).

Each extracted record is stored as a raw string under the `message` body map key. Data stream routing attributes (`data_stream.type`, `data_stream.dataset`, `data_stream.namespace`) are set on each log record so that mOTLP routes the document to the correct integration data stream. Every record also gets a baseline `@timestamp` (the decode time, like a Beats document); integration ingest pipelines that derive `@timestamp` from the event override it.

## Configuration

| Field                   | Type           | Default      | Description                                                                                                                                    |
|-------------------------|----------------|--------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `format`                | string         | `json`       | Input format: `json`, `text`, or `csv`.                                                                                                        |
| `unwrap`                | []string       | _(empty)_    | Sequence of JSON object keys to traverse to reach the target array (e.g., `["records"]` or `["data", "items"]`). Only used with `json` format. |
| `mappings`              | []object       | _(empty)_    | Field extraction rules for JSON array elements (see below). Only used with `json` format and `unwrap`.                                         |
| `mappings.source`       | string         | _(required)_ | JSON key to read from the decoded object.                                                                                                      |
| `mappings.destination`  | string         | _(required)_ | Key to write in the log body map.                                                                                                              |
| `mappings.type`         | string         | _(required)_ | OTel value type: `String` or `Integer`.                                                                                                        |
| `mappings.multiplier`   | int            | `0`          | Scales numeric values before storing (Integer only). `0` means no scaling.                                                                     |
| `data_stream.dataset`   | string         | _(required)_ | Data stream dataset (e.g., `azure.activitylogs`).                                                                                              |
| `data_stream.namespace` | string         | `default`    | Data stream namespace.                                                                                                                         |
| `input_type`            | string         | _(empty)_    | Sets the `input.type` field in the log record body (e.g., `aws-s3`, `azure-eventhub`).                                                         |
| `tags`                  | []string       | _(empty)_    | List of strings appended to the `tags` field in the log record body (e.g., `["forwarded", "aws-cloudtrail"]`).                                 |
| `fields`                | map[string]any | _(empty)_    | Key-value pairs added to every log record body (e.g., `{environment: production}`).                                                            |
| `csv.comma`             | string         | `,`          | CSV field separator, a single character. Only used with `csv` format (e.g., a single space for Netskope).                                      |
| `csv.comment`           | string         | _(empty)_    | CSV comment character, a single character. Lines starting with it (before the first record) are skipped. Only used with `csv` format.          |
| `csv.fields_names`      | []string       | _(empty)_    | Overrides the header. When empty, the first non-comment record is the header. Only used with `csv` format.                                     |
| `csv.lazy_quotes`       | bool           | `false`      | Allows a quote in an unquoted field and a non-doubled quote in a quoted field. Only used with `csv` format.                                     |
| `csv.trim_leading_space`| bool           | `false`      | Trims leading white space in a field. Only used with `csv` format.                                                                             |

### Formats

- **`json`** — The entire input is a JSON document. When `unwrap` is set, the extension traverses the listed keys to reach a target array and extracts each element as a separate log record (e.g., `["records"]` for Azure Diagnostic Settings, `["Records"]` for AWS CloudTrail). When `unwrap` is empty, the entire input is treated as a single record.
- **`text`** — Newline-delimited text. Each non-empty line becomes a separate log record.
- **`csv`** — CSV input. The first record (or `csv.fields_names`, if set) is the header; each subsequent record becomes a log record whose `message` is a JSON object keyed by the header. The `csv.*` options mirror the Beats `aws-s3` input's `decoding.codec.csv` settings. A single malformed record (wrong field count, or a stray quote without `csv.lazy_quotes`) fails the whole input, matching the Beats codec's strict behaviour.

### Examples

#### Azure Diagnostic Settings (JSON with unwrap)

```yaml
extensions:
  beats_encoding/azure:
    format: json
    unwrap:
      - records
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
    unwrap:
      - Records
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

#### Netskope CSV (space-separated)

```yaml
extensions:
  beats_encoding/netskope:
    format: csv
    csv:
      comma: " "
    data_stream:
      dataset: netskope.alerts_events_v2
    input_type: aws-s3
    tags: ["forwarded", "netskope-alerts_and_events"]

service:
  extensions: [beats_encoding/netskope]
```

Given a CSV input:
```
_id action alert
abc allow no
```

The resulting log body `message` will be:
```json
{"_id":"abc","action":"allow","alert":"no"}
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

#### JSON with field mappings

Extract specific fields from each array element instead of storing the entire JSON as `message`:

```yaml
extensions:
  beats_encoding/mapped:
    format: json
    unwrap:
      - data
      - items
    data_stream:
      dataset: custom
    input_type: aws-s3
    mappings:
      - source: log
        destination: message
        type: String
      - source: timestamp
        destination: "@timestamp"
        type: Integer
        multiplier: 1000  # seconds → millis

service:
  extensions: [beats_encoding/mapped]
```

Given input:
```json
{"data": {"items": [{"id": 1, "log": "hello", "timestamp": 1779463864}]}}
```

The resulting log body will contain:
- `message`: `"hello"`
- `@timestamp`: `1779463864000`

