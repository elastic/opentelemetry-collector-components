# otelsoak

otelsoak is just an OTel collector.

To generate load to an OTLP target, run Elastic collector components distro with specific pipelines to replay canned data at a configurable rate.

See an example configuration at [`config.example.yaml`](./config.example.yaml). There are rate limiting and trace ID rewriting by default.

## Usage

1. Build the Elastic collector components distro by running `make genelasticcol` at the root of this repository.
2. Run `otelsoak`, which is a symlink to the collector binary.

## Example usage

To override any config, use `--config` or `--set`. See [official OTel configuration documentation](https://opentelemetry.io/docs/collector/configuration/).
```
./otelsoak --config config.example.yaml --set "exporters.otlp.endpoint=http://localhost:8200" --set "exporters.otlp.headers.Authorization=ApiKey xxx" --set "exporters.otlp.headers.X-FOO-HEADER=bar"
```

Alternatively, there's `ELASTIC_APM_SERVER_URL` and `ELASTIC_APM_API_KEY` env var handling out of the box in the example config yaml. Note that `ELASTIC_APM_SECRET_TOKEN` is NOT supported without changing `config.example.yaml`.

```
ELASTIC_APM_SERVER_URL=http://localhost:8200 ELASTIC_APM_API_KEY=some_api_key ./otelsoak --config config.example.yaml
```

It is recommended to create your own `config.yaml` from `config.example.yaml` to fit your needs.

## Vercel drain (raw HTTP NDJSON)

OTLP exporters used in the OTLP otelsoak scenarios cannot target Managed Input
drains. The Vercel drain scenario sends NDJSON over HTTP instead.

Use the `http` exporter
([internal/exporter/httpexporter](../../../internal/exporter/httpexporter)) with
loadgen log bodies that contain drain NDJSON lines. See
[`config.vercel.example.yaml`](./config.vercel.example.yaml).

`VERCEL_SIGNAL` selects an embedded loadgenreceiver logs preset (default `logs`):

| Value | loadgen `logs.preset` | Embedded fixture |
| --- | --- | --- |
| `logs` | `vercel_logs` | [`receiver/loadgenreceiver/testdata/vercel/logs.jsonl`](../../../receiver/loadgenreceiver/testdata/vercel/logs.jsonl) |
| `speed_insights` | `vercel_speed_insights` | [`receiver/loadgenreceiver/testdata/vercel/speed_insights.jsonl`](../../../receiver/loadgenreceiver/testdata/vercel/speed_insights.jsonl) |
| `both` | `vercel_both` | concat of the two embeds |

Logs and speed-insights use the same Managed Input URL; only the NDJSON body shape changes.
Leave `VERCEL_SIGNAL` unset for the default; an empty value (`VERCEL_SIGNAL=`) is not a default and will fail.

`config.vercel.example.yaml` runs `transform/vercel_refresh` before the `http` exporter
so drain JSON fields keep advancing (loadgen only refreshes the OTLP log timestamp):

- Speed Insights: body `timestamp` (ISO) and `deviceId`
- Vercel logs: body unix-millis `timestamp` (including nested `proxy.timestamp`) and `id`

How to run:

```bash
make genelasticcol
VERCEL_SIGNAL=speed_insights \
ELASTIC_SERVER_URL=https://example.ingest.us-central1.gcp.qa.elastic.cloud \
ELASTIC_APM_API_KEY=some_api_key \
make otelsoak-run-vercel
```

For both signals:

```bash
VERCEL_SIGNAL=both \
ELASTIC_SERVER_URL=https://example.ingest.us-central1.gcp.qa.elastic.cloud \
ELASTIC_APM_API_KEY=some_api_key \
make otelsoak-run-vercel
```

Validate the config without sending traffic:

```bash
VERCEL_SIGNAL=both ELASTIC_SERVER_URL=http://localhost:8200 ELASTIC_APM_API_KEY=foobar \
  ./loadgen/cmd/otelsoak/otelsoak validate --config ./loadgen/cmd/otelsoak/config.vercel.example.yaml
```
