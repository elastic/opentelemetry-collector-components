## Streamlang processor

The streamlang processor executes [Kibana's streamlang DSL](https://www.elastic.co/guide/en/elasticsearch/reference/current/processors.html) directly on OpenTelemetry log records. It provides a familiar Elasticsearch ingest-pipeline style processing model inside the collector, with support for 20+ built-in operations including grok parsing, field manipulation, type conversion, and conditional logic.

### How it works

1. A `pipeline` of steps is defined in YAML using the streamlang DSL.
2. At startup the pipeline is compiled into an execution plan by the selected backend (`closure` or `vm`).
3. For each incoming log record the processor creates a `Document` view over its attributes (or body in transport mode) and executes the compiled pipeline.
4. Records marked by `drop_document` are removed from the batch before it is passed downstream.

### Configuration

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `backend` | string | `"closure"` | Execution backend. `"closure"` compiles steps into a chain of Go closures. `"vm"` compiles to a bytecode program executed in a tight loop. |
| `transport_mode` | bool | `false` | When `true`, operates on the log record **body** (which must be a map) instead of **attributes**. Useful for receivers that place the raw document in the body. |
| `pipeline` | object | required | The streamlang pipeline definition. Must contain at least one step. |

#### Regular mode (attributes)

In the default mode the processor reads and writes log record **attributes**:

```yaml
processors:
  streamlang:
    backend: closure
    pipeline:
      steps:
        - processor:
            action: rename
            from: host.name
            to: hostname
        - processor:
            action: set
            field: env
            value: production
```

#### Transport mode (body)

When `transport_mode: true` the processor treats the log record body map as the document. This is useful when a receiver places the entire raw event in the body:

```yaml
processors:
  streamlang:
    transport_mode: true
    pipeline:
      steps:
        - processor:
            action: grok
            field: message
            patterns:
              - "%{COMMONAPACHELOG}"
        - processor:
            action: remove
            field: message
```

### Backend selection

| Backend | How it works | Best for |
| --- | --- | --- |
| `closure` (default) | Compiles each step into a Go closure; steps execute sequentially as a function chain. | Simple pipelines, easier debugging. |
| `vm` | Compiles the pipeline to bytecode opcodes (`OpSet`, `OpRename`, …) executed in a tight `switch` loop with jump instructions for conditions. | Large or branching pipelines where the reduced per-step overhead matters. |

Both backends produce identical results for the same input. The choice is purely a performance trade-off.

```yaml
processors:
  streamlang:
    backend: vm   # or "closure"
    pipeline:
      steps:
        - processor:
            action: set
            field: processed_by
            value: vm-backend
```

### Pipeline DSL

A pipeline contains a list of **steps**. Each step is either a **processor** (an operation on the document) or a **condition block** (branching logic).

#### Processor step

```yaml
steps:
  - processor:
      action: <action_name>
      # action-specific fields ...
      where:            # optional guard condition
        field: status
        gte: 400
      ignore_failure: false   # continue on error (default false)
      ignore_missing: false   # skip if source field absent (default false)
```

#### Condition block

```yaml
steps:
  - condition:
      condition:
        field: log.level
        eq: "ERROR"
      steps:
        - processor:
            action: set
            field: alert
            value: true
      else:
        - processor:
            action: set
            field: alert
            value: false
```

### Available operations

| Action | Description | Key fields |
| --- | --- | --- |
| `set` | Set a field to a literal value or copy from another field. | `field`, `value` or `copy_from`; `override` |
| `rename` | Move a field from one path to another. | `from`, `to`; `override`, `ignore_missing` |
| `remove` | Delete a field. | `field`; `ignore_missing` |
| `remove_by_prefix` | Delete all fields matching a prefix. | `field` (the prefix) |
| `convert` | Convert a field's type. | `field`, `type` (`integer`, `long`, `double`, `boolean`, `string`) |
| `uppercase` | Convert a string field to uppercase. | `field` |
| `lowercase` | Convert a string field to lowercase. | `field` |
| `trim` | Strip leading/trailing whitespace. | `field` |
| `grok` | Parse a field using grok patterns. | `field`, `patterns`; `pattern_definitions` |
| `dissect` | Parse a field using a dissect pattern. | `field`, `pattern`; `append_separator` |
| `replace` | Replace the first regex match in a field. | `field`, `pattern`, `replacement` |
| `redact` | Redact field content. | `field`; `prefix`, `suffix` |
| `split` | Split a string into an array. | `field`, `separator`; `preserve_trailing` |
| `join` | Join multiple fields into one string. | `field`, `from_fields`, `delimiter` |
| `sort` | Sort an array field. | `field`; `order` (`asc` or `desc`) |
| `append` | Append values to an array field. | `field`, `values`; `allow_duplicates` |
| `concat` | Concatenate field values and literals. | `field`, `concat_from` (list of `{field}` or `{value}`) |
| `date` | Parse and reformat a date field. | `field`, `formats`; `output_format`, `timezone`, `locale` |
| `math` | Evaluate a math expression. | `field`, `expression` |
| `json_extract` | Extract fields from a JSON string. | `field`, `extractions` (list of `{path, target, type}`) |
| `drop_document` | Remove the entire log record from the batch. | — |
| `network_direction` | Determine traffic direction (inbound/outbound/internal/external). | `source_ip`, `destination_ip`, `target_field`; `internal_networks` |

### Conditions

Conditions can appear as a `where` guard on any processor or as the top-level predicate of a condition block.

| Operator | Description |
| --- | --- |
| `eq`, `neq` | Equality / inequality. |
| `lt`, `lte`, `gt`, `gte` | Numeric or lexicographic comparison. |
| `contains` | String contains. |
| `starts_with`, `ends_with` | String prefix / suffix. |
| `exists` | Field existence (`true` / `false`). |
| `includes` | Array includes value. |
| `range` | Combined range check with `gt`, `gte`, `lt`, `lte` sub-fields. |
| `and`, `or`, `not` | Logical combinators (recursive). |
| `always`, `never` | Constant true / false. |

### Example collector configurations

#### Apache log parsing

Parse Apache access logs using grok, extract the timestamp, and drop health-check requests:

```yaml
receivers:
  filelog:
    include:
      - /var/log/apache2/access.log

processors:
  streamlang:
    pipeline:
      steps:
        - processor:
            action: grok
            field: body
            patterns:
              - "%{COMMONAPACHELOG}"
        - processor:
            action: date
            field: timestamp
            formats:
              - "dd/MMM/yyyy:HH:mm:ss Z"
        - processor:
            action: remove
            field: body
        - condition:
            condition:
              field: request
              contains: "/health"
            steps:
              - processor:
                  action: drop_document

exporters:
  elasticsearch:
    endpoints:
      - https://localhost:9200

service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [streamlang]
      exporters: [elasticsearch]
```

#### Field normalization

Rename and normalize fields from heterogeneous log sources into a common schema:

```yaml
processors:
  streamlang:
    pipeline:
      steps:
        - processor:
            action: rename
            from: src_ip
            to: source.ip
            ignore_missing: true
        - processor:
            action: rename
            from: dst_ip
            to: destination.ip
            ignore_missing: true
        - processor:
            action: lowercase
            field: log.level
        - processor:
            action: convert
            field: response_code
            type: integer
        - processor:
            action: set
            field: event.kind
            value: event
```

#### Conditional routing and enrichment

Use conditions to apply different processing based on field values:

```yaml
processors:
  streamlang:
    backend: vm
    pipeline:
      steps:
        - condition:
            condition:
              field: log.level
              eq: "ERROR"
            steps:
              - processor:
                  action: set
                  field: event.severity
                  value: 3
              - processor:
                  action: set
                  field: alert.enabled
                  value: true
            else:
              - processor:
                  action: set
                  field: event.severity
                  value: 1
        - processor:
            action: network_direction
            source_ip: source.ip
            destination_ip: destination.ip
            target_field: network.direction
            internal_networks:
              - "10.0.0.0/8"
              - "172.16.0.0/12"
              - "192.168.0.0/16"
        - processor:
            action: append
            field: tags
            values:
              - processed
            allow_duplicates: false
```
