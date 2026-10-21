# Prometheus Scrape Receiver

| Status        |           |
| ------------- |-----------|
| Stability     | [development]: metrics |

[development]: https://github.com/open-telemetry/opentelemetry-collector/blob/main/docs/component-stability.md#development

The `prometheusscrape` receiver scrapes Prometheus endpoints and keeps the
data in the **native Prometheus schema**. It does not translate samples into
OpenTelemetry semantics. It is meant to round-trip losslessly through the
`prometheusremotewriteexporter` into Elasticsearch's native
`/_prometheus/.../api/v1/write` endpoint, so that Grafana dashboards and PromQL
keep working unmodified.

It uses the Prometheus scrape manager, so relabeling, `honor_labels`, service
discovery, staleness handling and the `up`/`scrape_*` report series behave as in
Prometheus.

## Configuration

```yaml
receivers:
  prometheusscrape:
    config:
      global:
        external_labels:
          cluster: my-cluster
      scrape_configs:
        - job_name: node
          scrape_interval: 15s
          static_configs:
            - targets: ["localhost:9100"]
```

`config` takes the same format as the Prometheus `scrape_configs` section.
`scrape_configs` or `scrape_config_files` is required. Not supported:
`remote_write`, `remote_read`, `rule_files`, `alerting`,
`convert_classic_histograms_to_nhcb` (see [Histograms](#histograms)), the target
allocator and the API server.

## Data model

Every Prometheus series becomes its own OTel metric, named exactly as scraped
(`__name__` is not modified). The resource and the instrumentation scope are
empty. All labels, including `job` and `instance`, are datapoint attributes.

| Prometheus series | OTel metric |
|---|---|
| `counter`, and `_bucket`/`_sum`/`_count` of `histogram` and `summary` | cumulative monotonic Sum |
| `gauge`, `unknown`, `info`, `stateset`, `gaugehistogram`, summary quantile series, report series | Gauge |

Every metric carries the original family type in the metric metadata
(`prometheus.type`), the raw `UNIT` in `prometheus.unit` (when set), and the
`HELP` text as description. `metric.Unit` is left empty. Samples have no start
timestamp.

Note: a histogram `_sum` can decrease with negative observations. It is still
marked monotonic, matching the Prometheus convention and Elasticsearch's counter
mapping.

### Histograms

- **Classic histograms and summaries** are passed through as flat series: `_bucket`,
  `_sum` and `_count` (and summary `quantile` series) keep their names, with
  `le`/`quantile` kept as labels, as described above.
- **Native histograms** are converted to an OTel `ExponentialHistogram` (cumulative),
  following the
  [OTel Prometheus compatibility spec](https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/#exponential-histograms).
  The name and labels are kept as scraped, and the `prometheusremotewriteexporter`
  writes them back as native histograms (remote write 1.0). To scrape them, enable
  them on the scrape job; native histograms are only exposed in the protobuf format:

  ```yaml
  scrape_configs:
    - job_name: my-app
      scrape_native_histograms: true
      scrape_protocols: [PrometheusProto]
  ```

  Limitations:
  - Float histograms (rare in scrapes) are converted to the integer counts of OTel
    and are rounded down.
  - Custom bucket histograms (NHCB, schema -53) are not exponential and are dropped.
    `convert_classic_histograms_to_nhcb` is rejected in the configuration.
  - A native histogram has no min/max, and the exporter sends counter reset hints
    as "unknown".
  - The exporter fills gaps between bucket spans with explicit zero buckets. This is
    the same histogram, in a different encoding.
  - Native histograms are validated before conversion, as the Prometheus TSDB would. Malformed ones (spans that don't match
    the buckets, negative counts, or an unreasonably large bucket layout) are dropped, and a single warning per scrape reports
    how many were dropped. The same applies to custom bucket histograms.
  - Staleness of a native histogram is kept as a `StaleNaN` sum, with no flag.
  - Whether Elasticsearch stores native histogram samples written to its
    `/_prometheus` endpoint has not been verified yet.

### Differences from the upstream `prometheusreceiver`

The upstream receiver converts into the Collector's OTel metric data model
(resource attributes, typed histograms, flags). Fidelity then depends on the
exporter undoing each step. This receiver avoids those translations:

| Upstream | This receiver |
|---|---|
| `job`/`instance` become resource attributes `service.name`/`service.instance.id`, plus `server.*`, `url.scheme` | Stay datapoint labels, empty resource |
| `target_info` consumed into resource attributes | Ordinary series |
| `otel_scope_info`/`otel_scope_*` become the scope | Ordinary labels and series |
| Classic histograms/summaries grouped, de-cumulated, `+Inf` dropped | Flat series, `le`/`quantile` as parsed by Prometheus |
| `_created` consumed as start time | Ordinary series |
| StaleNaN turned into `NoRecordedValue` flag | Raw StaleNaN value, no flag |
| Unit converted, optional suffix trimming | Not offered |

Prometheus 3 itself normalizes `le`/`quantile` values (for example `0.00001`
becomes `1e-05`); that is kept, as it is what Prometheus stores.

## Required exporter configuration

Use the `prometheusremotewriteexporter` with remote write 1.0 and:

```yaml
exporters:
  prometheusremotewrite:
    endpoint: https://<es>/_prometheus/metrics/<dataset>/<namespace>/api/v1/write
    translation_strategy: UnderscoreEscapingWithoutSuffixes
    disable_scope_info: true
```

- `translation_strategy: UnderscoreEscapingWithoutSuffixes` is **mandatory**. The
  default (`UnderscoreEscapingWithSuffixes`) renames monotonic Sums, for example
  `x_bucket` becomes `x_bucket_total`. `NoTranslation` is rejected by the
  exporter with remote write 1.0.
- Do not set `namespace` or `resource_to_telemetry_conversion`.

### Known limitations

Exporter side (remote write 1.0):

- With the configuration above, labels such as `a__b` and `_foo` are written
  unchanged (covered by a correctness test, which will flag exporter changes).
  Empty label values are dropped by the exporter.

Elasticsearch side:

- NaN and StaleNaN samples are dropped, so staleness is not applied;
- on duplicate label set and timestamp the first write wins;
- help/unit are not stored in the metadata API.

## Routing

Use relabeling to route series to a data stream:

```yaml
relabel_configs:
  - target_label: data_stream_dataset
    replacement: myapp
  - target_label: data_stream_namespace
    replacement: production
```
