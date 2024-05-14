# Example Distribution of the OpenTelemetry Collector

## How to build

Here are the quick and dirty instructions to build the example distribution, verified to work on Linux.
Might need some tweaks to work on MacOS.

Run the following commands in the root directory of the repository:

```shell
otelcol_version=$(grep '^  otelcol_version: ' distributions/example/manifest.yaml | cut -f 4 -d ' ')
echo "otelcol_version: $otelcol_version"

uname_os=$(uname -s)
[ "$uname_os" = "Darwin" ] && otelcol_os=darwin
[ "$uname_os" = "Linux" ] && otelcol_os=linux
echo "otelcol_os: $otelcol_os"

uname_arch=$(uname -m)
[ "$uname_arch" = "arm64" ] && otelcol_arch=arm64
[ "$uname_arch" = "x86_64" ] && otelcol_arch=amd64
echo "otelcol_arch: $otelcol_arch"

ocb_file_name="ocb_${otelcol_version}_${otelcol_os}_${otelcol_arch}"
download_url="https://github.com/open-telemetry/opentelemetry-collector/releases/download/cmd/builder/v${otelcol_version}/${ocb_file_name}"
echo "Downloading OpenTelemetry Collector Builder from ${download_url}"

curl -LO "${download_url}"
chmod +x ${ocb_file_name}
./${ocb_file_name} --config=./distributions/example/manifest.yaml
```

The above should download the [OpenTelemetry Collector Builder](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.100.0/cmd/builder/README.md) for your platform and version specified in the manifest, and build the distro with the builder.
The resulting distro binary should be available as `./output/otelcol-example`:

```shell
./output/otelcol-example --version
./output/otelcol-example --config ./distributions/example/config.yaml
```

You should see the configured pipeline at <http://localhost:55679/debug/pipelinez> thanks to the zPages extension.
