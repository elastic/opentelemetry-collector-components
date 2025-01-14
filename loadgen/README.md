# loadgen: the load generator based on OTel collector

To generate load to an OTLP target, run Elastic collector components distro with specific pipelines to replay canned data at a configurable rate.

See an example configuration at `config.example.yaml`. There are rate limiting and trace ID rewriting by default.

## Usage

```
../_build/elastic-collector-components --config config.example.yaml --set "exporter.otlp.endpoint=http://localhost:8200" --set "exporter.otlp.headers.Authorization=ApiKey xxx" --set "exporter.otlp.headers.X-FOO-HEADER=bar"
```

Alternatively, there's `ELASTIC_APM_SERVER_URL` and `ELASTIC_APM_API_KEY` env var handling out of the box. `ELASTIC_APM_SECRET_TOKEN` is NOT supported without changing `config.example.yaml`.

```
ELASTIC_APM_SERVER_URL=http://localhost:8200 ELASTIC_APM_API_KEY=some_api_key ../_build/elastic-collector-components --config config.example.yaml
```

Even better, create your own `config.yaml` from `config.example.yaml` to fit your needs.
