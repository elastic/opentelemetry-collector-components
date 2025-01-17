# otelsoak

otelsoak is just an OTel collector.

To generate load to an OTLP target, run Elastic collector components distro with specific pipelines to replay canned data at a configurable rate.

See an example configuration at [`config.example.yaml`](./config.example.yaml). There are rate limiting and trace ID rewriting by default.

## Usage

1. Build the Elastic collector components distro by running `make genelasticcol` at the root of this repository.
2. Run `otelsoak`, which is a symlink to the collector binary.

To override any config, use `--config` or `--set`. See [official OTel configuration documentation](https://opentelemetry.io/docs/collector/configuration/).
```
ELASTIC_APM_SERVER_URL="" ELASTIC_APM_API_KEY="" ./otelsoak --config config.example.yaml --set "exporters.otlp.endpoint=http://localhost:8200" --set "exporters.otlp.headers.Authorization=ApiKey xxx" --set "exporters.otlp.headers.X-FOO-HEADER=bar"
```

Alternatively, there's `ELASTIC_APM_SERVER_URL` and `ELASTIC_APM_API_KEY` env var handling out of the box in the example config yaml. Note that `ELASTIC_APM_SECRET_TOKEN` is NOT supported without changing `config.example.yaml`.

```
ELASTIC_APM_SERVER_URL=http://localhost:8200 ELASTIC_APM_API_KEY=some_api_key ./otelsoak --config config.example.yaml
```

It is recommended to create your own `config.yaml` from `config.example.yaml` to fit your needs.
