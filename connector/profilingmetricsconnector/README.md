# Profiling metrics connector

The Profiling metrics connector is an opinionated OTel connector that generates OTel metrics from selected OTel profiling data.

## Configuration

Any [generated metric](./metadata.yaml) can be disabled through the configuration. For example:

```
metrics:
  samples.classification:
    enabled: false
  samples.dotnet.count:
    enabled: false
```

**⚠️ Configuration Warning: Metric Dependencies**

To ensure data integrity and accurate ratio calculations, adhere to the following rules:
  - Required Combination: You must enable `samples.kernel.count` and `samples.user.count`. Their sum is the only reliable way to calculate the total sample count.
  - Frame metrics: Avoid disabling specific frame metrics like `samples.native.count`. Disabling these results in a loss of information regarding shared libraries.


[Quickstart guide](https://www.elastic.co/docs/reference/edot-collector/config/configure-profiles-collection) to use this connector as part of [EDOT](https://www.elastic.co/docs/reference/opentelemetry).
