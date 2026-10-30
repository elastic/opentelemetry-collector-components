# Decision: recovery ordering and reader-first format rollout

Status: proposed with the pre-aggregation stack (#1483, #1507, #1521).

## Context

Buffering groups metrics into different operands than direct Pebble writes.
An arrival-order tie rule therefore selects different cumulative samples at
identical timestamps. Replay can also change operand order or strand old
windows if export only considers the current processing time. Persisting a new
format immediately would prevent rollback to the previous release.

## Decisions

- Cumulative timestamp ties use the lexicographically greater canonical OTLP
  datapoint encoding, excluding stream attributes and sorting exemplar maps
  recursively. This is an arbitrary but deterministic choice for conflicting
  samples, not a claim that one measurement is more correct. Timestamp order
  continues to take precedence. Equal-timestamp ties can cost more CPU because
  they require canonicalization. The rule is shared by both value formats.
- Replay retains segment boundaries and emits processing windows in ascending
  order, with metadata as a deterministic secondary sort key. Failed-segment
  operands precede current-segment operands. Cardinality admission remains
  order-sensitive; buffering is not guaranteed to retain the same identities
  as unbuffered processing under overflowing limits.
- Log-enabled export discovers overdue persisted windows, and a failed flush
  holds the export boundary until the flush can be recovered. Failed downstream
  export retains the window. Delivery is at least once, not exactly once:
  successful tenants can be redelivered when another tenant fails or the process
  crashes before deletion. Passthrough metrics are outside this recovery model.
- The first format release reads v2 and writes v1, without a configuration
  bypass. Enabling v2 writes is a separate release decision after the supported
  rollback release can read v2. Version 2 pins the current identity hash contract;
  changing it requires a version bump and reader migration, guarded by fixtures.

## Validation and limits

Regression tests cover timestamp ties, overdue recovery, replay ordering and
segment boundaries, and v2-to-v1 output read by the previous implementation with
its overflow estimators intact. Existing golden and concurrency tests remain.
The benchmark uses 1s/2s/4s windows and reports intake and drain-inclusive work
separately, verifies exported service-summary totals, and settles Pebble writes
before disk accounting. It measures the processor, not the trace connector or
production one-hour cardinality. Shortened windows are intentionally not a
uniform rescaling of 1m/10m/60m; their results must not be extrapolated as production
speedups. Filesystem metadata and device-level write amplification are excluded.
