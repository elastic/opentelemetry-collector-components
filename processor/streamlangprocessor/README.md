# streamlangprocessor

| Status        |                            |
| ------------- |----------------------------|
| Stability     | development: traces, logs, metrics |
| Distributions | none yet                   |

Executes a Streamlang DSL pipeline against OpenTelemetry log records, span
records and metric data points. Mirrors the semantics of the other Streamlang
execution targets (Elasticsearch ingest pipelines, ES|QL) so a pipeline
written once behaves consistently across them.

## Status

| Signal  | Support                                                    |
|---------|------------------------------------------------------------|
| logs    | full pipeline execution                                    |
| metrics | full pipeline execution per data point (all 5 types)       |
| traces  | factory wired, lifecycle works, **passes records through unchanged** |

The processor accepts a configuration on all three signals today; logs and
metrics actually run the pipeline. Trace support is pending.

## Configuration

```yaml
processors:
  streamlang:
    failure_mode: drop          # "drop" (default) | "propagate"
    steps:
      - action: set
        to: attributes.processed_by
        value: streamlangprocessor

      - action: rename
        from: attributes.original_message
        to: body
        ignore_missing: true

      - action: grok
        from: body
        patterns:
          - "%{IP:client_ip} %{WORD:method} %{URIPATHPARAM:path}"

      # Condition blocks. The nested `steps` go *inside* the `condition`
      # object (not as a sibling key).
      - condition:
          field: attributes.severity
          eq: ERROR
          steps:
            - action: set
              to: attributes.alert
              value: true
```

Or load from disk:

```yaml
processors:
  streamlang:
    path: /etc/otelcol/streamlang/pipeline.yaml
```

`steps` and `path` are mutually exclusive.

### Top-level options

| Key             | Type                  | Default | Description                                               |
|-----------------|-----------------------|---------|-----------------------------------------------------------|
| `steps`         | array of step objects | —       | Inline DSL pipeline. Mutually exclusive with `path`.      |
| `path`          | string                | —       | Filesystem path to a YAML/JSON DSL document. Loaded once at startup. |
| `failure_mode`  | `drop` \| `propagate` | `drop`  | Per-document failure behavior. `propagate` is for testing only. |
| `logs.enabled`  | bool                  | `true`  | Toggle the logs path.                                     |
| `metrics.enabled` | bool                | `true`  | Toggle the metrics path (currently a no-op).              |
| `traces.enabled`  | bool                | `true`  | Toggle the traces path (currently a no-op).               |

## Supported processors

22 of the 24 Streamlang actions are implemented. Two are intentionally
unsupported in this collector context.

### Implemented

| Action              | Notes                                                  |
|---------------------|--------------------------------------------------------|
| `set`               | `value` or `copy_from` (mutually exclusive). `override` defaults to **true** (matches ES `set`). |
| `append`            | `allow_duplicates` defaults to false (dedupe).         |
| `remove`            | Honors `ignore_missing`.                               |
| `remove_by_prefix`  | Strips both flat (`foo.bar`) and nested-map matches.   |
| `rename`            | `override` defaults to **false** (matches ES `rename`).|
| `drop_document`     | `where` is required. Marks the record dropped — pruned from the batch before forwarding. |
| `uppercase` / `lowercase` / `trim` | Standard string ops.                  |
| `replace`           | Regex pattern, compiled once at config time.           |
| `convert`           | Targets `integer`, `long`, `double`, `boolean`, `string`. |
| `grok`              | Backed by `github.com/elastic/go-grok`. Tries `patterns[]` in order and stops at first match. Typed captures (`%{NUMBER:port:int}`) coerced to int/float/bool. |
| `dissect`           | Hand-rolled parser. Supports `%{key}`, `%{?skip}`, `%{+name}` (append), `%{name->}` (right-pad). |
| `date`              | Built-ins: `ISO8601`, `epoch_millis`, `epoch_second`. Joda tokens `yyyy/yy/MM/M/MMM/MMMM/dd/d/HH/H/hh/h/mm/m/ss/s/SSS/SS/S/a/Z/ZZ/ZZZ/X/XX/XXX` and quoted literals (`'T'`, `'Z'`). Default target: `@timestamp`. |
| `redact`            | Reuses the grok library; iteratively finds and replaces named captures with `<NAME>` (configurable `prefix`/`suffix`). |
| `math`              | TinyMath subset: `+ - * / ()` + unary minus + dotted field references. **No functions in v1** (no `pow`/`sqrt`/`abs`/...). |
| `json_extract`      | Selector dialect: dotted path + `[N]` array index. **No `*`, `..`, or filters in v1.** Per-extraction `type` coercion (`keyword`/`integer`/`long`/`double`/`boolean`). Missing JSON paths skip silently. |
| `network_direction` | Backed by stdlib `net/netip`. Accepts CIDR entries and named groups: `private`, `public`, `loopback`, `link_local`, `link_local_multicast`, `interface_local_multicast`, `multicast`, `broadcast`, `global_unicast`, `unspecified`. Output values: `internal`, `outbound`, `inbound`, `external`. |
| `split`             | Regex separator, compiled once. `preserve_trailing` controls trailing-empty handling. |
| `sort`              | Stable sort. Heuristic: all-numeric → numeric compare; all-bool → false<true; otherwise lexicographic. |
| `join`              | Recursively joins slice values with the same delimiter. Map values are an error. |
| `concat`            | Mix of `{type: literal, value}` and `{type: field, value}` parts; no delimiter. |

### Not supported

| Action                    | Why                                                  |
|---------------------------|------------------------------------------------------|
| `enrich`                  | Requires Elasticsearch enrich-policy lookup. An `EnrichProvider` extension point is planned for a future iteration. |
| `manual_ingest_pipeline`  | Wraps raw Elasticsearch ingest processors — not applicable inside a collector. Use `elasticsearchexporter`'s `pipeline` option for that flow. |

The DSL parser rejects these two with a clear error referencing this section.

### Common processor options

Every processor accepts:

| Key                | Type            | Description                                                      |
|--------------------|-----------------|------------------------------------------------------------------|
| `customIdentifier` | string          | Free-form tag for correlating telemetry/debugging.               |
| `description`      | string          | Human-readable note.                                             |
| `ignore_failure`   | bool            | Swallow execution errors. Document keeps flowing on failure.     |
| `where`            | condition       | Conditional guard (see below). Not available on `remove_by_prefix`. |
| `ignore_missing`   | bool            | Where supported, controls behavior when the source field is missing. |

## Conditions

Supported operators inside a `where` clause or a top-level `condition` block:

- Binary: `eq`, `neq`, `lt`, `lte`, `gt`, `gte`, `contains`, `startsWith`,
  `endsWith`, `range` (with `gt`/`gte`/`lt`/`lte`), `includes`.
- Unary: `exists`.
- Combinators: `and`, `or`, `not`, `always: {}`, `never: {}`.

Numeric coercion follows ES ingest semantics: `"200" eq 200` is **true**.
String operators (`contains`/`startsWith`/`endsWith`) are case-insensitive.

### Nested condition blocks

A condition block carries its own `steps`. The block's condition is
AND-combined with each contained processor's existing `where` at compile
time:

```yaml
- condition:
    field: attributes.severity
    eq: ERROR
    steps:
      - action: set
        to: attributes.alert
        value: true
      - condition:
          field: attributes.kind
          eq: auth
          steps:
            - action: set
              to: attributes.escalate
              value: true
```

## Path conventions

The Streamlang DSL refers to fields by dotted path. The processor maps these
onto pdata.

For attribute-map paths, the dotted *rest* of the path expands into nested
`pcommon.Map` values on **Set**. **Get** also falls back to a flat-dotted-key
lookup at the top level so wire-serialised attributes (`http.method`) resolve
naturally.

### Logs

| Path                          | Target                                          |
|-------------------------------|-------------------------------------------------|
| `resource.attributes.<k>`     | `ResourceLogs.Resource().Attributes()`          |
| `scope.name` / `scope.version` / `scope.attributes.<k>` | `InstrumentationScope`|
| `attributes.<k>`              | `LogRecord.Attributes()`                        |
| `body`                        | `LogRecord.Body()` (string body)                |
| `body.<k>`                    | `LogRecord.Body()` map keys (auto-promoted)     |
| `severity_text`, `severity_number`, `time_unix_nano`, `observed_time_unix_nano`, `trace_id`, `span_id`, `flags`, `event_name` | direct fields on `LogRecord` |
| _bare key without prefix_     | `LogRecord.Attributes()` (ES ingest compat)     |

`trace_id` and `span_id` are read-only on Set: assignment returns
`ErrUnsupportedTarget`. With `ignore_failure: true` the document keeps
flowing.

### Metrics

One Document is built per data point. All five OTLP metric types are
supported: Gauge, Sum, Histogram, ExponentialHistogram, Summary.

| Path                                         | Target                                                |
|----------------------------------------------|-------------------------------------------------------|
| `resource.attributes.<k>`                    | `ResourceMetrics.Resource().Attributes()`             |
| `scope.name` / `scope.version` / `scope.attributes.<k>` | `InstrumentationScope`                     |
| `name`, `description`, `unit`                | parent `Metric` fields                                |
| `metadata.<k>`                               | `Metric.Metadata()`                                   |
| `attributes.<k>` / `data_point.attributes.<k>` | the data point's `Attributes()` (alias)             |
| `data_point.value`                           | `NumberDataPoint` Int/Double value (Gauge/Sum only)   |
| `data_point.count`                           | `Count()` on Histogram / ExponentialHistogram / Summary |
| `data_point.sum`                             | `Sum()` (where present)                               |
| `data_point.min`, `data_point.max`           | Histogram / ExponentialHistogram (where present)      |
| `data_point.time_unix_nano`, `data_point.start_time_unix_nano` | the data point's timestamps        |
| `data_point.flags`                           | the data point's `Flags()`                            |
| _bare key without prefix_                    | data point's `Attributes()`                           |

**`name`/`description`/`unit` are read-only when the parent metric has more
than one data point** — the processor returns `ErrUnsupportedTarget`. The
parent metric is shared with siblings so a write would silently corrupt them.
Use `ignore_failure: true` if you want the document to keep flowing on this
case. (When a metric has exactly one data point, writes succeed normally.)

`drop_document` removes the data point. If a metric loses all its data
points it is pruned; same for `ScopeMetrics` and `ResourceMetrics`
containers.

## Error semantics

Per-document errors never abort the batch. Behavior matches Elasticsearch
ingest pipelines and ES|QL:

- A processor failure on a document terminates _that_ document (it is dropped
  from the output by default).
- `ignore_failure: true` on a processor swallows the error and the document
  keeps flowing through subsequent processors.
- `ignore_missing: true` on a missing source field becomes a *skip* (not a
  failure): the offending step is bypassed and the next step runs.
- `failure_mode: propagate` (testing only) surfaces the first per-document
  error to the consumer.

## Telemetry

Instruments are registered under the scope
`github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor`.

| Metric                              | Type                | Attributes                            |
|-------------------------------------|---------------------|---------------------------------------|
| `streamlang.documents.processed`    | counter (i64)       | `tenant.id`                           |
| `streamlang.documents.dropped`      | counter (i64)       | `tenant.id`                           |
| `streamlang.documents.routed`       | counter (i64)       | `tenant.id`, `to_stream`              |
| `streamlang.processor.executions`   | counter (i64)       | `tenant.id`, `action`, `outcome` ∈ {`applied`, `skipped`, `error`, `ignored`} |
| `streamlang.pipeline.duration_ms`   | histogram (f64, ms) | `tenant.id`                           |

`tenant.id` is sourced from the `x-elastic-project-id` metadata header on the
incoming `client.Info`. Empty when absent.

A `streamlang.pipeline.execute_batch` span is emitted per `Consume*` call.
Per-applied-processor sub-spans are not yet attached: the executor already
aggregates per-action `applied|skipped|error|ignored` counts (visible via
`streamlang.processor.executions`) and will surface them as span attributes
in a follow-up.

## Routing

`set { to: stream.name, value: "<dest>" }` is just an attribute write — the
processor does **not** re-route by itself. Pair with a downstream
`routingconnector` / `dynamicroutingconnector` that reads
`attributes.stream.name` to actually move records between pipelines.

## Divergences from other Streamlang targets

The cross-target consistency harness will record these as-shipped:

- **Missing-field `neq` returns true** (matches ES bulk semantics).
  Deliberate alignment with ES bulk; flagged for the cross-target consistency
  harness to keep the rule consistent across targets.
- **`convert` on already-typed pdata values** stringify-then-reparses to
  align with ES; pdata Int → string → parse round-trip preserves precision
  but adds an allocation.
- **`math` v1 has no functions.** TinyMath in ES supports `pow`, `sqrt`,
  `abs`, `floor`, `ceil`, `round`, `log`, etc. Tracked as a follow-up.
- **`json_extract` v1 selector** is dotted + `[N]`. Globs (`*`),
  recursive-descent (`..`), and filter expressions are rejected at compile
  time.
- **`enrich` and `manual_ingest_pipeline`** are unsupported (see above).

## Native execution vs. transpilation

Both shapes are valid. The Streamlang ecosystem has two ways to run the same
DSL on an OTel collector pipeline:

1. **Transpile DSL → OTTL** and run inside the upstream `transformprocessor`.
2. **Execute the DSL natively** via this processor.

For production OTel pipelines the trade-offs lean strongly toward native
execution.

### Coverage

| Action group | OTTL transpiler | `streamlangprocessor` (native) |
|---|---|---|
| `set`, `rename`, `remove`, `grok`, `uppercase`, `lowercase`, `trim`, `replace`, `split`, `convert`, `redact`, `concat`, `join`, `drop_document`, `append`, `date`, `json_extract` | ✅ | ✅ |
| `math`, `sort`, `remove_by_prefix` | Tier 2 (planned, OTTL paths confirmed) | ✅ |
| `dissect` | ❌ no OTTL equivalent (`ExtractDissectPatterns` undefined) | ✅ |
| `network_direction` | ❌ no OTTL equivalent (`NetworkDirection()` undefined) | ✅ |
| `enrich` | ❌ ES-native concept, no OTel mapping | ❌ same reason |
| `manual_ingest_pipeline` | ❌ ingest-only by design | ❌ same reason |

Native: **22/24** today. OTTL transpiler: **17/23** today (20/23 with Tier 2
implemented), three hard-blocked. `dissect` and `network_direction` matter
for log shaping and security analytics — either they are there or they
aren't.

### Semantic fidelity

The OTTL transpiler ships with documented semantic divergences from ES
ingest because OTTL's runtime doesn't expose the primitives ES does. None of
these apply to the native target — it evaluates the DSL directly.

| Behaviour | OTTL transpiler | `streamlangprocessor` (native) |
|---|---|---|
| `ignore_missing: false` raises on missing source | Silent skip (no "error on nil" in OTTL) | Real error → doc dropped per ES rules |
| `rename` atomicity | `set + delete_key` — non-atomic under `error_mode: propagate` | Atomic |
| `grok` first-match-wins | All patterns evaluated; later matches overwrite earlier | First-match-wins, mirrors ES |
| `grok` `%{X:@timestamp}` capture | Silently not populated (go-grok won't sanitize `@`) | Works |
| `includes` / `append allow_duplicates: false` | Regex approximation; metachars produce false positives | Native list operations |
| `date.output_format` | Not implementable; warning emitted, value stored as nanos | Joda→Go layout, native output |
| `date` multi-format | Requires `error_mode: ignore` to fall through | Natural per-format fallthrough |
| `json_extract` numeric type | All JSON numbers stored as `doubleValue` | Honors `type` per extraction |
| `startsWith` / `endsWith` on non-string | Coerces to string (`true` matches `"t"`) | Strict |
| `math` integer division | Truncates unless transpiler wraps each operand with `Double()` | Native float math |
| `math` divide-by-zero, `log(≤0)` | Silent skip under `error_mode: ignore`; ES Painless propagates `Inf`/`NaN` | Real error (or `ignore_failure: true`) |
| `math` comparison expressions | Need two `set()` statements due to OTTL grammar | Single expression |
| `sort` non-array input | Silent no-op | Real error |
| `remove_by_prefix` across `kvlistValue` | Doesn't recurse into nested Maps; ES Painless does | Recurses (mirrors ES) |

### Operational

- **One artifact, one debugger.** What you author is what runs; no
  intermediate OTTL to reason about when something misbehaves.
- **No grammar lossiness.** OTTL restrictions (no comparisons in `set()`
  value position, no atomic rename, no first-match grok) leak into generated
  config as multi-statement workarounds.
- **Coverage growth without upstream OTTL changes.** Adding `dissect` /
  `network_direction` to OTTL would require contributions to the contrib
  repo; here they're already in tree.
- **Performance.** Pipeline compilation happens once at `Start`; the hot
  path is closures over precompiled regex / grok / Joda layouts.

### When transpilation is still the right tool

- Distributions that standardize on `transformprocessor` and don't want a
  new component.
- Authoring tools that target OTTL natively (e.g. an existing
  `transformprocessor` deployment owned by another team).
- Targeting non-Go collector runtimes.

The two approaches are complementary; this processor doesn't replace the
transpilation target.

## Build vs. reuse

Every capability is delivered as a deliberate mix of reused libraries and
purpose-built code. The rule of thumb: build only the parts that don't exist
elsewhere, or whose existing implementations don't match Streamlang
semantics. This keeps the surface area small and the maintenance burden
honest.

### Reused

| Capability | Library | Why reuse |
|---|---|---|
| Grok pattern matching (`grok`, `redact`) | `github.com/elastic/go-grok` | Elastic's official Go port; catalog identical to ES; typed captures (`%{NUMBER:port:int}`) work. Verified via Phase-2 spike against the canonical IP+method+URI patterns. |
| Regex (`split`, `replace`) | stdlib `regexp` | RE2; pre-compiled in constructors so the hot path is alloc-light. |
| IP / CIDR (`network_direction`) | stdlib `net/netip` | Allocation-free CIDR membership; named-group helpers (`IsPrivate` / `IsLoopback` / etc.). |
| JSON parse (`json_extract`) | stdlib `encoding/json` | Standard, fast, no extra deps. |
| Time parse + format (`date`) | stdlib `time` | Parses RFC3339 / Unix; we own only the Joda → Go layout translation on top. |
| Stable sort (`sort`) | stdlib `slices.SortStableFunc` | Right primitive for the stable-sort behavior we need. |
| YAML config loading | `gopkg.in/yaml.v3` | De-facto Go YAML library; round-trips inline DSL when we re-marshal mapstructure output. |
| OTLP data types (logs/metrics/traces) | `go.opentelemetry.io/collector/pdata` | Required to plug into the collector pipeline at all. |
| Collector framework (factory, consumer, processor settings) | `go.opentelemetry.io/collector/...` | Standard processor scaffolding; we conform to it. |
| OTel API (metrics + tracing) | `go.opentelemetry.io/otel` | Collector telemetry plumbing for our own instruments. |
| Test assertions | `github.com/stretchr/testify` | Already present across the components repo. |

### Built

| Component | Reason for building |
|---|---|
| **DSL parser** (`dsl/`) | Streamlang is an Elastic spec with no existing Go implementation. The TypeScript schema in Kibana (`x-pack/platform/packages/shared/kbn-streamlang`) is the source of truth; we mirror it 1:1 with strict unknown-key rejection so user errors surface at config load. |
| **Document** (`document/`) | The dotted-path mutable view over pdata is unique. ES ingest semantics — flat `http.method` *and* nested `attributes.http.method` both resolving, bare keys defaulting to record attributes, dotted Set expanding into nested Maps — cannot be expressed using existing pdata helpers. |
| **Condition compiler + evaluator** (`condition/`) | The condition AST is Streamlang-specific (ES bulk filter shorthand: `eq` / `neq` / `range` / `includes` / `exists` / `and` / `or` / `not` / `always` / `never`). No Go library carries the right semantics — including the deliberate "missing-field `neq` → true" rule. |
| **Coercion helpers** (`internal/coerce/`) | ES bulk type coercion (`"200" eq 200` → true; integer-valued floats stringify without the trailing `.0`) doesn't match Go's reflect rules or any third-party "loose equality" library. |
| **Pipeline executor** (`internal/pipeline/`) | Per-document error isolation matches ES ingest *exactly* (drop offending doc, batch continues; `ignore_failure: true` swallows; condition guard skips don't count as failures). The semantics, telemetry shape, and skip-vs-error distinction are all Streamlang-specific. |
| **Per-processor implementations** (`internal/processors/*`) | Each processor's behavior — defaults, `ignore_missing` rule, `override` semantics — is part of the Streamlang spec. Even when wrapping a library (grok, regex, JSON), the surrounding logic is custom. |
| **Joda → Go layout translator** (inside `date.go`) | Go's `time` reference layout (`2006-01-02 15:04:05`) is not Joda. Existing Go libraries (`vjeantet/jodaTime` etc.) accept Joda but with subtly different token coverage from ES. A small in-tree translator pinned to the ES token set keeps all Streamlang targets accepting the same patterns. |
| **TinyMath subset parser** (inside `math.go`) | ES TinyMath has a tight, defined operator + function set. General-purpose expression evaluators (`expr-lang/expr` etc.) are *too permissive* — accepting more than ES does at the Go target would silently break cross-target portability. A 200-line recursive-descent parser is safer and matches ES exactly within its current scope. |
| **Dissect parser** (`dissect.go`) | No maintained Go dissect library matches ES's modifier set (`%{?skip}`, `%{+name}`, `%{name->}`). A ~150-line hand-rolled parser was cheaper than auditing a third-party choice for spec parity. |
| **JSONPath subset selector** (inside `json_extract.go`) | Existing JSONPath libs (`PaesslerAG/jsonpath`, `ohler55/ojg`) accept significantly more than ES `json_extract` does. Allowing those extras at the Go target but rejecting them in ES would silently break cross-target portability — we accept only the dotted + `[N]` subset and reject the rest at compile time. |
| **Telemetry instruments** (`internal/telemetry/`) | Metric names, span shape, and attribute keys (`tenant.id`, `outcome`, `streamlang.batch.applied_count`) are part of the public dashboard contract; they need to be stable across Streamlang execution targets. |
| **CLI binary** (`cmd/streamlangcli/`) | The cross-target consistency harness needs a stdin/stdout adapter that reuses the very same internal packages as the processor — anything else risks divergence between "what we test" and "what we ship". |
| **Deterministic redact capture pick** (`pickRedactCapture`) | Go's randomized map iteration made redact's choice between `<IPV4>` and `<client>` non-deterministic (intermittent test failures, inconsistent redaction labels in production). User-named captures should always win — the heuristic codifies that. |

### What we declined to build

- **A new grok engine.** Speed-critical and the existing Elastic library matches the catalog. If allocation pressure shows up under real load, the path forward is an upstream PR — not a fork.
- **A full JSONPath / JsonPathLite implementation.** Adding `*`, `..`, and filter expressions is gated on a real customer ask. The dotted + `[N]` subset covers the vast majority of `json_extract` usage we've seen.
- **TinyMath functions.** Implementation exists; the question is whether to expand the parser or drop in a vetted library subset. Tracked as a follow-up.
- **Span / metric pdata wrappers beyond what's already in `metric_document.go`.** Traces are pending.

## Cross-target verification

A separate cross-target consistency suite (out of tree) verifies behavioural
parity with the other Streamlang execution targets — Elasticsearch ingest
pipelines, ES|QL, and the OTTL `transformprocessor`. The
`cmd/streamlangcli` binary in this module is the stdin/stdout adapter the
suite drives.

## Benchmarks

`go test -bench=. -run=^$ .` runs:

- `BenchmarkPipeline` over 5 representative pipelines × {1, 100, 1000}
  records: `simple_set_remove`, `grok_then_set`, `condition_guard_skip_all`,
  `routing_set`, `kitchen_sink`.
- `BenchmarkProcessor` per-action microbench at batch size 1000 covering
  `set`, `remove`, `rename`, `uppercase`, `replace`, `convert`, `grok`,
  `dissect`, `split`, `json_extract`.

Indicative per-record costs on Apple M4 Pro (1000-record batches):

| Pipeline                   | ns/record | allocs/record |
|----------------------------|-----------|---------------|
| `simple_set_remove`        | ~300      | ~12           |
| `condition_guard_skip_all` | ~210      | ~10           |
| `routing_set`              | ~330      | ~15           |
| `grok_then_set`            | ~10 800   | ~21           |
| `kitchen_sink`             | ~11 000   | ~32           |

Non-grok pipelines push roughly 3.3 M records/sec on a single core;
grok-heavy pipelines run at ~90 k records/sec, bottlenecked on go-grok
regex execution.
