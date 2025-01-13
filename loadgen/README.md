# loadgen: the load generator based on OTel collector

loadgen is itself an OTel collector, and it replays canned data at a configurable rate.

In `config.example.yaml`, there are rate limiting and trace ID rewriting by default.

## Usage

```
./loadgen --config config.example.yaml --set "exporter.otlp.endpoint=http://localhost:8200" --set "exporter.otlp.headers.Authorization=ApiKey xxx" --set "exporter.otlp.headers.X-FOO-HEADER=bar"
```

Alternatively, there's `ELASTIC_APM_SERVER_URL` and `ELASTIC_APM_API_KEY` env var handling out of the box. `ELASTIC_APM_SECRET_TOKEN` is NOT supported without changing `config.example.yaml`.

```
ELASTIC_APM_SERVER_URL=http://localhost:8200 ELASTIC_APM_API_KEY=some_api_key ./loadgen --config config.example.yaml
```

Even better, create your own `config.yaml` from `config.example.yaml` to fit your needs.
