## LSM interval processor

The LSM interval processor is a stateful metrics processor that aggregates metric streams over
configured time windows using a log-structured merge-tree (LSM) database (Pebble). It is a rewrite
of the upstream [interval processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/intervalprocessor)
with db-backed persistence so aggregation state can survive restarts and large batches.

### How it works (high level)

1. Incoming metrics are merged into a Pebble database using a custom merger that aggregates by
   resource, scope, metric, and datapoint identity.
2. Each aggregation key includes the interval duration, the current processing window (the
   processing time truncated to the interval), and optional `metadata_keys` from the client context
   so different tenants can be isolated.
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
| `budget_metadata_keys` | list | `[]` | Client metadata keys whose values select the cardinality budget. Each limit applies per distinct combination of values. Keys are case-insensitive and must be unique. |
| `resource_limit` | object | `{}` | Resource cardinality limit (`max_cardinality`) and overflow attributes. |
| `scope_limit` | object | `{}` | Scope cardinality limit (`max_cardinality`) and overflow attributes. |
| `metric_limit` | object | `{}` | Metric cardinality limit (`max_cardinality`) and overflow attributes. |
| `datapoint_limit` | object | `{}` | Datapoint cardinality limit (`max_cardinality`) and overflow attributes. |
| `exponential_histogram_max_buckets` | int | `160` | Maximum buckets for merged exponential histograms. |
| `storage.memtable_size` | int | `33554432` (32MiB) | Size in bytes of each Pebble memtable. Must be between 16MiB and 4GiB. |
| `storage.memtable_stop_writes_threshold` | int | `2` | Number of memtables (active plus queued for flush) at which writes stop until a flush completes. Must be at least 2. |
| `storage.max_concurrent_compactions` | int | `1` | Maximum number of Pebble compactions run concurrently. Must be at least 1. |

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

### Storage tuning

Every `ConsumeMetrics` call appends a merge operand per interval to the Pebble key for its
interval, processing window and metadata. When a memtable flushes, Pebble merges the operands
that share a key. If flushes can't keep up with ingestion, writes stop once
`storage.memtable_stop_writes_threshold` memtables are full. While writes are stopped, the
processor holds its mutex, so every `ConsumeMetrics` caller waits.

Raising `storage.memtable_size` and `storage.memtable_stop_writes_threshold` reduces these stalls in
two ways. Each flush merges more operands per key, and more flush latency can be absorbed before
writes stop. The cost is memory:

- The memtables themselves need about `memtable_size * memtable_stop_writes_threshold` bytes.
- In practice, resident memory grows by several times that amount, because flushing larger
  memtables allocates more while merging.

Size the memory limit with headroom and measure before rolling out. The defaults are unchanged.

Compactions merge the operands of a key across on-disk files after they have been flushed.
`storage.max_concurrent_compactions` lets Pebble run more of them in parallel, which costs CPU and
only helps when compaction, not flushing, falls behind (Pebble reports those stalls with a
different reason than memtable stalls).

```yaml
processors:
  lsminterval:
    directory: /var/lib/otel/lsminterval
    storage:
      memtable_size: 67108864 # 64MiB
      memtable_stop_writes_threshold: 4
      max_concurrent_compactions: 2
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

#### Cardinality budgets

By default every limit applies once per aggregated value, so a tenant whose data reaches the
processor through several sources (for example several Kafka partitions) shares one budget when
the sources land on the same instance and gets one budget per instance otherwise.
`budget_metadata_keys` makes the limits apply per distinct combination of the named client
metadata values instead, independent of how the sources are placed:

- A new resource, scope, metric or datapoint takes one slot in the budget that delivered it.
  An entity delivered through several budgets takes a single slot, in the budget that delivered it
  first. Once a budget is full its new entities overflow as described above.
- Budget keys do not split the aggregation and are not propagated: data from every budget merges
  into the same aggregate and the exported metrics are unchanged. Requests without any of the keys
  share one default budget.
- The stored format only changes for values that hold a budget other than the default, so leaving
  the option unset, or setting it without any `max_cardinality`, keeps the on-disk format identical.
- Budgets are meant for a bounded number of pools per aggregated value, such as the partitions a
  tenant's topics have on one instance. Every tracker keeps one count per budget, 8 bytes in memory
  and about 1 byte on disk, so hundreds of budgets are fine while a key with thousands of distinct
  values (a tenant or user id, for example) multiplies the size of every value accordingly.

```yaml
processors:
  lsminterval:
    budget_metadata_keys: [kafka.partition]
    resource_limit:
      max_cardinality: 8000 # per partition
```
