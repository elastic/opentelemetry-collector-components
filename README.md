# OpenTelemetry Collector Components

OpenTelemetry Collector Components is a collection of components of the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) that were created at [Elastic](https://www.elastic.co/).

## Code of Conduct

See [CODE_OF_CONDUCT](CODE_OF_CONDUCT.md).

## License

This software is licensed under the [Apache 2 license](LICENSE).

## Building a collector with a custom component

In order to build a collector with a custom component, e.g. for testing purposes, follow these steps:
1. Edit the [manifest.yaml](distributions/elastic-components/manifest.yaml) file:
   - Add the component you want to test to the proper component section. For  example, if you are testing a processor, add it to the 
     `processors` section.
   - If you are testing a non-published version of the component, add an entry to the `replace` section, pointing to the local path of 
     the component's source.
2. Build the collector through the `genelasticcol` target of the root [Makefile](Makefile). 
   Make sure to provide `TARGET_GOOS` and/or `TARGET_GOARCH` environment variables if you are building for a different platform.
   For example, when building on macOS in order to run through the Linux Docker image that is built by the `builddocker` make target 
   (see next bullet) - use the following command:
   ```shell
   TARGET_GOOS=linux CGO_ENABLED=0 make genelasticcol
   ```
   The resulting binary will be placed in the `_build` directory.
3. In order to build a Docker image with the collector, run the `builddocker` target of the root [Makefile](Makefile). 
   This target requires the environment variable `TAG` to be set. The resulting image will be tagged as `elastic-collector-components:<TAG>`.
   You may also specify the `USERNAME` environment variable to name the image as `<USERNAME>/elastic-collector-components:<TAG>`. For example:
   ```shell
   make builddocker TAG=v0.1.0 USERNAME=johndoe
   ``` 

## Load generator

See [./loadgen/README.md](./loadgen/README.md).
