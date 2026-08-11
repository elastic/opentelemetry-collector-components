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
[`config.vercel.example.yaml`](./config.vercel.example.yaml) and
[`testdata/vercel/`](./testdata/vercel/).

`VERCEL_SIGNAL` selects which fixture to replay (default `logs`):

| Value | Fixture |
| --- | --- |
| `logs` | [`testdata/vercel/logs.jsonl`](./testdata/vercel/logs.jsonl) |
| `speed_insights` | [`testdata/vercel/speed_insights.jsonl`](./testdata/vercel/speed_insights.jsonl) |
| `both` | Concat of the two files at run time (`make otelsoak-run-vercel` only) |

Logs and speed-insights use the same Managed Input URL; only the NDJSON body shape changes.
Leave `VERCEL_SIGNAL` unset for the default; an empty value (`VERCEL_SIGNAL=`) is not a default and will fail.

How to run:

```bash
make genelasticcol
VERCEL_SIGNAL=speed_insights \
ELASTIC_SERVER_URL=https://example.ingest.us-central1.gcp.qa.elastic.cloud \
ELASTIC_APM_API_KEY=some_api_key \
make otelsoak-run-vercel
```

For both signals (temp file = `logs.jsonl` + `speed_insights.jsonl`):

```bash
VERCEL_SIGNAL=both \
ELASTIC_SERVER_URL=https://example.ingest.us-central1.gcp.qa.elastic.cloud \
ELASTIC_APM_API_KEY=some_api_key \
make otelsoak-run-vercel
```

Validate the config without sending traffic:

```bash
VERCEL_SIGNAL=speed_insights ELASTIC_SERVER_URL=http://localhost:8200 ELASTIC_APM_API_KEY=foobar \
  ./loadgen/cmd/otelsoak/otelsoak validate --config ./loadgen/cmd/otelsoak/config.vercel.example.yaml
```
