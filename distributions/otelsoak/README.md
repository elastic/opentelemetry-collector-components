# otelsoak distribution

This minimal Collector distribution runs the default OTLP soak configuration with the embedded fixtures from `loadgenreceiver`.

Build and validate the distribution with `make otelsoak-image-validate`. Build a local Docker image with:

```shell
make buildotelsoakdocker TAG=dev
```

The image runs as a non-root user and reads `/etc/otelsoak/config.yaml` by default. Override the config by mounting a file at that path. Set `ELASTIC_APM_SERVER_URL` and `ELASTIC_APM_API_KEY` to target an Elastic APM Server.
