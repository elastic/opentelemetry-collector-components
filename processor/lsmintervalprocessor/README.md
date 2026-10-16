## LSM interval processor

The LSM interval processor is a stateful metrics processor that aggregates metric streams over
configured time windows using a log-structured merge-tree (LSM) database (Pebble). It is a rewrite
of the upstream [interval processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/intervalprocessor)
with db-backed persistence so aggregation state can survive restarts and large batches.

### How it works (high level)

1. Incoming metrics are merged into a Pebble database using a custom merger that aggregates by
   resource, scope, metric, and datapoint identity.
2. Each aggregation key includes the interval duration, the current processing window, and optional
   `metadata_keys` from the client context so different tenants can be isolated.
3. A timer runs on the smallest configured interval. When it fires, the processor commits pending
   batches and exports aggregated metrics for every interval that has reached its boundary.
4. Optional OTTL statements on each interval run after a metric has matured for that interval.
5. Gauge metrics are passed through unchanged; summary metrics are aggregated unless
   `pass_through.summary` is enabled.

### Configuration

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `directory` | string | `""` | Pebble data directory. Empty means in-memory storage with no persistence. |
| `pass_through.summary` | bool | `false` | Pass summary metrics through without aggregation. |
| `intervals` | list | `[60s]` | Interval configurations. Durations must be increasing and a factor of the smallest interval. |
| `intervals[].duration` | duration | required | Aggregation window for the interval. |
| `intervals[].statements` | list | `[]` | OTTL datapoint statements applied after aggregation for this interval. |
| `metadata_keys` | list | `[]` | Client metadata keys to partition aggregation. Keys are case-insensitive and must be unique. |
| `resource_limit` | object | `{}` | Resource cardinality limit (`max_cardinality`) and overflow attributes. |
| `scope_limit` | object | `{}` | Scope cardinality limit (`max_cardinality`) and overflow attributes. |
| `metric_limit` | object | `{}` | Metric cardinality limit (`max_cardinality`) and overflow attributes. |
| `datapoint_limit` | object | `{}` | Datapoint cardinality limit (`max_cardinality`) and overflow attributes. |
| `exponential_histogram_max_buckets` | int | `160` | Maximum buckets for merged exponential histograms. |
| `pre_aggregation.enabled` | bool | `false` | Merge incoming metrics in memory before writing them to Pebble. See [Pre-aggregation](#pre-aggregation). |
| `pre_aggregation.max_size` | int | `33554432` (32MiB) | Size, in bytes of incoming protobuf, after which the pre-aggregation buffer is flushed to Pebble. |
| `pre_aggregation.log.enabled` | bool | `false` | Write every request to an append-only log before merging it into the pre-aggregation buffer, so the buffer survives process restarts. Requires `pre_aggregation.enabled` and `directory`. See [Pre-aggregation log](#pre-aggregation-log). |

`max_cardinality` values of `0` disable overflow tracking for that level.

#### Example configuration

```yaml
processors:
  lsminterval:
    directory: /var/lib/otel/lsminterval
    metadata_keys: ["tenant_id"]
    pass_through:
      summary: true
    intervals:
      - duration: 1m
      - duration: 5m
        statements:
          - set(attributes["interval"], "5m")
    resource_limit:
      max_cardinality: 1000
      overflow:
        attributes:
          - key: resource_overflow
            value: true
    scope_limit:
      max_cardinality: 2000
      overflow:
        attributes:
          - key: scope_overflow
            value: true
    metric_limit:
      max_cardinality: 5000
      overflow:
        attributes:
          - key: metric_overflow
            value: true
    datapoint_limit:
      max_cardinality: 100000
      overflow:
        attributes:
          - key: datapoint_overflow
            value: true
```

### Pre-aggregation

By default, every `ConsumeMetrics` call writes one merge operand per interval to the Pebble key for
its interval, processing window and metadata. Pebble then merges those operands again on every
memtable flush and compaction. For a busy key, that means the accumulated value is decoded and
re-merged many times, and flushes can fall behind ingestion. When they do, writes stop and
callers block.

With `pre_aggregation.enabled: true`, incoming metrics are merged in memory into one value per
key. That value is written to Pebble as a single merge operand per interval when one of these
happens:

- the buffer reaches `pre_aggregation.max_size`;
- the processing window advances;
- the processor shuts down.

Aggregated results are identical in both modes, including overflow handling.

The buffer holds at most `pre_aggregation.max_size` bytes of incoming metrics since its last flush,
plus the memory of the merged values themselves, which is usually much smaller. Unless the
[pre-aggregation log](#pre-aggregation-log) is enabled, unflushed buffered data is lost on a crash,
in the same way as the processor's unflushed in-memory batch.

```yaml
processors:
  lsminterval:
    directory: /var/lib/otel/lsminterval
    pre_aggregation:
      enabled: true
      max_size: 33554432 # 32MiB
```

#### Pre-aggregation log

With `pre_aggregation.log.enabled: true`, every request is appended to a log segment in
`<directory>/preagg-log/` before it is merged into the buffer, and `ConsumeMetrics` returns only
after the record is written to the operating system. A process that stops without a clean
shutdown, for example after a container restart, loses nothing that was written: on start, the
processor replays the segments that are not yet reflected in Pebble.

- **Flush:** a flush starts a new segment and writes the buffer to Pebble together with the number
  of the last segment it covers, in the same synced batch. The covered segments are then deleted.
  A crash between the commit and the deletion is safe, because replay skips covered segments.
- **Failed flush:** if the commit fails, the segments are kept, and the next flush reads them back.
- **Torn writes:** each record has a length and a CRC-32C checksum. Replay stops at the first
  incomplete record, which can only be the last one written before the process stopped.
- **Host failures:** records reach the page cache on every write and are fsynced every 8MiB, which
  matches the batch commit threshold without the log.
- **Write errors:** a failed write is sticky, and the processor rejects further data until it
  restarts. Requests merged into memory but not written are reported as failed, so they may be
  counted twice if they are retried.

Records are written in groups: concurrent requests share a single write, and records for the same
aggregation key are written in the order they are merged, which keeps overflow handling identical
on replay. Each record holds the S2-compressed request and its client metadata, so the log grows
by the compressed size of the incoming metrics until the next flush. `pre_aggregation.max_size`
therefore bounds the log size as well as the buffer.

Because the log makes the buffer durable, `max_size` no longer limits how much data a crash can
lose, only memory and log size. A larger value means fewer flushes and fewer merge operands per key
for Pebble to merge.

```yaml
processors:
  lsminterval:
    directory: /var/lib/otel/lsminterval
    pre_aggregation:
      enabled: true
      max_size: 268435456 # 256MiB
      log:
        enabled: true
```

### Overflow handling

Overflow caps cardinality at the resource, scope, metric, and datapoint levels to protect the
collector from unbounded aggregation growth. When a `max_cardinality` limit is reached:

- **Resource/scope overflow**: new resources or scopes are merged into a single overflow bucket
  decorated with the configured `overflow.attributes`. Overflow buckets are not counted against the
  normal cardinality limit and can overflow independently.
- **Metric overflow**: new metric identities are discarded, and a delta sum metric named
  `_overflow_metric` records the estimated count of unique metrics dropped. Overflow attributes from
  `metric_limit.overflow.attributes` are added to this metric.
- **Datapoint overflow**: new datapoint identities are discarded, and a delta sum metric named
  `_overflow_datapoints` records the estimated count of unique datapoints dropped. Overflow
  attributes from `datapoint_limit.overflow.attributes` are added to this metric.

Overflow counts are approximate. The processor uses a HyperLogLog estimator to track the unique
identities that exceeded each limit.
