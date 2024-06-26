# Elastic Infra Metrics Processor

<!-- status autogenerated section -->
| Status        |           |
| ------------- |-----------|
| Stability     | [alpha]:  metrics  |
| Distributions |  |
| [Code Owners](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/CONTRIBUTING.md#becoming-a-code-owner)    | [@ishleenk17](https://www.github.com/ishleenk17) |

[beta]: https://github.com/open-telemetry/opentelemetry-collector#beta
[core]: https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol
[contrib]: https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol-contrib
<!-- end autogenerated section -->

The Elastic Infra Metrics Processor can be used to bridge the gap between OTEL and Elastic Infra Metrics. It is used to power the Curated UI's in Elastic. 
This processor uses the [elastic/opentelemtery-lib](https://github.com/elastic/opentelemetry-lib) library, which derives and adds Elastic compatible metrics from the OTEL metrics without altering the OTEL metrics. 
The processor should be used only with the below receivers and exporter.

**Receivers**: It should be used with the [Host Metrics Receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/hostmetricsreceiver#host-metrics-receiver) and [Kubelet Stats Receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/kubeletstatsreceiver#kubelet-stats-receiver).<br>
**Exporter**: It should be used only with the [Elasticsearch Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/elasticsearchexporter#elasticsearch-exporter). 


## Configuration

```yaml
processors:
  elasticinframetrics:
    # Defines if System Infra Metrics compatibility should be enabled
    # default = true
    add_system_metrics: {true,false}
    # Defines if K8s Infra Metrics compatibility should be enabled
    # default = true
    add_k8s_metrics: {true,false}
```
