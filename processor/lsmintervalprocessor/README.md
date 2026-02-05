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
