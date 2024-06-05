# Example Distribution for the OpenTelemetry Elastic's components

This distribution contains all the components from the [Elastic OpenTelemetry Collector components](https://github.com/elastic/opentelemetry-collector-components) repository.

**This distribution is used to ensure compatibility between Elastic's components and the OpenTelemetry collector builder. It is not intended for public release.**.

The list of components included in the public Elastic OpenTelemetry collector can be found [here](https://github.com/elastic/elastic-agent/blob/main/internal/pkg/otel/README.md).

## Contributing

All components within this repository should be added in this distribution's manifest in order to ensure its integration with any OpenTelemetry Collector binary. For example, to add a receiver component named `customreceiver` and located in the root repository directory `./receiver/customreceiver`, append the following entries into the [manifest.yaml](./manifest.yaml) file:

```
receivers:
  - gomod: github.com/elastic/opentelemetry-collector-components/receiver/customreceiver v0.0.1

...
replaces:
  - github.com/open-telemetry/opentelemetry-collector-contrib/receiver/customreceiver => ../receiver/customreceiver
```

Finally, add the component into the example [distribution's configuration file](./config.yaml).
