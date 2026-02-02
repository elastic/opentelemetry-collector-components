## Rate limiter processor


Processor to rate limit the data sent from a receiver to an exporter using an in-memory rate limiter.


## Configuration

| Field               | Description                                                                                                                                                                                                       | Required | Default    |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|------------|
| `metadata_keys`     | List of metadata keys inside the request's context that will be used to create a unique key for rate limiting.                                                                                                     | No       |            |
| `strategy`          | Rate limit by requests (`requests`), records (`records`), or by bytes (`bytes`). If by `records`, then it will limit what is applicable between log record, span, metric data point, or profile sample.           | Yes      | `requests` |
| `rate`              | Bucket refill rate, in tokens per second.                                                                                                                                                                         | Yes      |            |
| `burst`             | Maximum number of tokens that can be consumed.                                                                                                                                                                    | Yes      |            |
| `throttle_behavior` | Processor behavior for when the rate limit is exceeded. Options are `error`, return an error immediately on throttle and does not send the event, and `delay`, delay the sending until it is no longer throttled. | Yes      | `error`    |
| `throttle_interval` | Time interval for throttling.                                                                                                                                                                                     | No       | `1s`       |
| `retry_delay`       | Suggested client retry delay included via gRPC `RetryInfo` when throttled.                                                                                                                                        | No       | `1s`       |
| `overrides`         | Allows customizing rate limiting parameters for specific metadata key-value pairs. Use this to apply different rate limits to different tenants, projects, or other entities identified by metadata. Each override is identified by a set of metadata key values and can specify custom `rate`, `burst`, and `throttle_interval` settings that take precedence over the global configuration for matching requests. | No       |            |

### Overrides

Overrides are identified by configuring `matches`. The overrides are prioritised based on order i.e. the first override that matches is prioritised. An override is considered a match if the configured `matches` are a subset of the client metadata. If any of the `matches` does not exist in the client metadata, that override is not considered a match.

| Field               | Description                                                                                                                                                                                                       | Required | Default    |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|------------|
| `matches`           | Map of key value pairs to identify the target for an override. It must be an subset of the client metadata for a successful override.                                                                             | No       |            |
| `rate`              | Bucket refill rate, in tokens per second.                                                                                                                                                                         | No       |            |
| `burst`             | Maximum number of tokens that can be consumed.                                                                                                                                                                    | No       |            |
| `throttle_interval` | Time interval for throttling.                                                                                                                                                                                     | No       |            |

### Example

Example when using as a local rate limiter:

```yaml
processors:
  ratelimiter:
    metadata_keys:
    - x-tenant-id
    rate: 1000
    burst: 10000
    strategy: requests
```
### Telemetry and metrics

The processor emits attributes on relevant metrics to aid debugging and monitoring:

* `decision`
* `reason`

Metrics exposed by the processor include:

* `otelcol_ratelimit.requests` — total number of rate limiting requests
* `otelcol_ratelimit.request_duration` — histogram of request processing duration (seconds)
* `otelcol_ratelimit.request_size` — histogram of bytes per request (only when strategy is `bytes`)
* `otelcol_ratelimit.concurrent_requests` — current number of in-flight requests
