# Pre-aggregation: completed-work benchmark, 2026-10-29

This supersedes the short intake-only figures in issue #1522. The tested code
is the v1-format log branch at `3f45ddb`; the buffer and log are opt-in.

| Configuration | Completed datapoints/s, median (range) | Completed datapoints/CPU-second, median | Reported write amplification, median |
| --- | ---: | ---: | ---: |
| Buffer off | 159,095 (158,618–159,733) | 62,402 | 3.601 |
| Buffer 32 MiB, log off | 845,446 (833,014–850,291) | 373,122 | 1.033 |
| Buffer 256 MiB, log on | 2,107,035 (2,095,970–2,193,103) | 678,075 | 0.3077 |

Completed-throughput ratios are **5.31×** and **13.24×** relative to buffer off
for this specific workload. All nine measured runs verified the exact exported
`service_summary` total across all three intervals. Intake lasted 6.16–7.17
longest-window durations per run. Draining added 0.67–1.39 seconds.

## Method

- Apple M4 Pro, Go 1.25.5, darwin/arm64, `GOMAXPROCS=4`.
- High-cardinality APM-shaped metrics, 16 metadata tenants, eight callers, on-disk
  Pebble with explicit default storage settings, 512 deterministic input batches.
- Windows **1s/2s/4s**, not 1m/10m/60m. This exercises repeated expiry of every
  interval within each measured run. It is not a uniform time rescaling.
- Three repetitions per configuration, `-benchtime=24s`; Go calibrates request
  counts, so actual elapsed times vary. Configurations ran in the listed order.
  There is no separate steady-state warm-up; cold-window effects are included.
- Stop callers, stop the export goroutine, commit remaining buffered and batched
  data, export every partial interval, flush Pebble, and synchronously compact.
  Only then record completed wall time, CPU time, and bytes written. Intake and
  drain-inclusive rates are both retained in the raw output.
- Count the exact input request sequence, including its partial final cycle.
  Assert the exported service-summary sum equals input sum times three.
- Reported writes are Pebble WAL + flush + compaction bytes + pre-aggregation log
  bytes, including final draining. Filesystem metadata and device amplification
  are not included. Forced final compaction is a comparison boundary, not a
  claim that production compacts on every shutdown.
- Other agent builds/tests were paused during measurement.

Reproduce from `processor/lsmintervalprocessor`:

```sh
GOMAXPROCS=4 go test -run '^$' \
  -bench 'BenchmarkPreAggregation/cardinality=high/tenants=16/callers=8/pre_aggregation=(off|on|on,log,max_size=256MiB)$' \
  -benchtime=24s -count=3 -timeout=15m .
```

The recorded run used a separately compiled test binary with the equivalent
`-test.*` flags, so subsequent source edits could not affect the measurements.
[Raw output](preaggregation-2026-10-29.txt) includes every repetition.

## Interpretation limits

These are processor-level results, excluding trace decoding, span-to-metric
conversion, and real downstream network/indexing work. Short windows reduce
retained cardinality and compaction history relative to production one-hour
windows. The fixed run order can introduce thermal/order effects. Three runs
show repeatability here, not a production confidence interval. Do not reuse the
13.24× ratio as a production speedup or generalize it to one-tenant workloads.
The optional reader-first v2 PR was not benchmarked by this run, and its old
v2-write numbers do not describe its new default behavior.
