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

Overflow is best effort and not deterministic. The limits bound how many entities a stored value
holds; they do not define which entities those are, and the same input can produce a different
overflow set on another run:

- Admission favours older data: inside one merge the operands are folded oldest first, so what
  older data admitted keeps its slots and newer entities overflow. Within one merge this is first
  come, first served by write order.
- Not everything is one merge. A request is capped on its own in `ConsumeMetrics`, before anything
  is known about the stored value, and so is every compaction that combines only part of a key's
  records: a memtable flush, an L0 compaction, a compaction between two lower levels that has not
  reached the oldest record. Each of them writes a capped intermediate that later merges see as a
  single operand, and an overflow decided in one of them is final. Across merges the rule is
  therefore "oldest visible operand wins", not write order.
- The visible effect is an entity split between its own series and the overflow bucket. Take a
  resource `Z` admitted by an early request and a flush that combines only later requests: if that
  flush hits the limit, `Z`'s later data is folded into `_other` with its identity gone, and the
  next merge cannot move it back even though `Z` is admitted there. With budgets the same split can
  leave a newer entity in overflow although its budget turns out to have room, because the
  intermediate charged a shared entity to a different budget than older data does.
- Which intermediates exist depends on write volume and on Pebble's compaction choices, and which
  entities overflow near a limit depends on how data is batched into requests. The bound itself
  always holds, per budget. Keep limits well above the size of one request and of what one key
  accumulates between flushes, so a single record or flush never triggers overflow on its own.

#### Merge order

What Pebble does, verified against v1.1.5:

- A merge runs through one `pebble.ValueMerger` per key and read. Pebble creates it with one
  operand and submits the rest with either `MergeNewer` or `MergeOlder`, never both in the same
  merge, and the merger may buffer the operands until `Finish`
  ([`internal/base/merger.go`](https://github.com/cockroachdb/pebble/blob/v1.1.5/internal/base/merger.go#L13-L60)).
- Compaction visits a key newest first: it creates the merger with the newest MERGE record and
  calls `MergeOlder` for every older MERGE and for the SET the chain ends on
  ([`compaction_iter.go`](https://github.com/cockroachdb/pebble/blob/v1.1.5/compaction_iter.go#L843-L910)).
  A memtable flush is a compaction and takes the same path
  ([`compaction.go`](https://github.com/cockroachdb/pebble/blob/v1.1.5/compaction.go#L2875-L3019)).
  `Finish` gets `includesBase=true` only when the chain ended on a SET; otherwise the result is
  stored as a MERGE record that later merges see as one operand
  ([`compaction_iter.go`](https://github.com/cockroachdb/pebble/blob/v1.1.5/compaction_iter.go#L525-L548)).
- Forward iteration, which is what the export path uses, does the same on read: newest first,
  `MergeOlder` down to the SET, `Finish(true)`
  ([`iterator.go`](https://github.com/cockroachdb/pebble/blob/v1.1.5/iterator.go#L704-L743),
  [`iterator.go`](https://github.com/cockroachdb/pebble/blob/v1.1.5/iterator.go#L1144-L1195)).
- Only reverse iteration submits operands oldest first, with `MergeNewer`
  ([`iterator.go`](https://github.com/cockroachdb/pebble/blob/v1.1.5/iterator.go#L1060-L1090)).
  The processor never iterates backwards.

So on every path this processor uses, the stored value is the oldest operand and arrives last. The
merger buffers the operands as they arrive and folds them at `Finish`, oldest first, into the
oldest value. Every operand is decoded and replayed once, and the large stored value is merged
into rather than copied, so a long run of small requests costs their size, not the size of the
growing aggregate. Folding on arrival instead would either copy the growing aggregate into every
older operand, quadratic in the number of requests, or merge into the newest value, which rewrites
the admitted set of a full value on every new request and pushes series present since the start
of the window into the overflow bucket. `MergeNewer` is handled with the same fold so a reverse
scan would see the same policy.

#### Cardinality budgets

By default every limit applies once per aggregated value, so a tenant whose data reaches the
processor through several sources (for example several Kafka partitions) shares one budget when
the sources land on the same instance and gets one budget per instance otherwise.
`budget_metadata_keys` makes the limits apply per distinct combination of the named client
metadata values instead, so a source keeps its own budget whether or not it shares an instance
with others. Placement still shows near a limit: an entity that several sources on one instance
deliver takes one slot in one budget, where sources on separate instances each pay for it, and a
source that moves to another instance mid-window starts a fresh budget there while its old counts
stay in the value it left. Budgets reduce the dependence on placement; they do not remove it:

- A new resource, scope, metric or datapoint takes one slot in the budget that delivered it.
  An entity delivered through several budgets takes a single slot, in the budget of the oldest
  operand that carried it; the other budgets get it for free. Once a budget is full its new entities
  overflow as described above, with the same best-effort caveats: the bound per budget always holds,
  membership near a limit depends on how requests and compaction intermediates were capped.
- Budget keys do not split the aggregation and are not propagated: data from every budget merges
  into the same aggregate and the exported metrics are unchanged. Requests without any of the keys
  share one default budget.
- The stored format version moves from 1 to 2 to carry the budgets. Values written by earlier
  versions are still read, with everything on the default budget. Version 2 is written even when
  `budget_metadata_keys` is unset. Rolling back to a version that predates budgets can permanently
  discard in-flight window data: the older exporter rejects version 2 entries and deletes their
  window range rather than retaining them for a later upgrade.
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
