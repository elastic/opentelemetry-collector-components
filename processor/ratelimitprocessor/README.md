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

#### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `otelcol_ratelimit.requests` | Counter | Total number of rate-limiting decisions made. Labelled with `decision` and `reason`. |
| `otelcol_ratelimit.request_duration` | Histogram | Time (seconds) to evaluate the rate limit check itself. Labelled by metadata keys only. |
| `otelcol_ratelimit.request_size` | Histogram | Size (bytes) of the request. Only recorded when `strategy: bytes`. Labelled with `decision` and `reason`. |
| `otelcol_ratelimit.concurrent_requests` | UpDownCounter | Number of requests currently being processed (in-flight). Labelled by metadata keys only. |
| `otelcol_ratelimit.delay_duration` | Histogram | Time (seconds) a request spent waiting due to rate limiting. Only recorded when `throttle_behavior: delay` and a delay actually occurred (`decision: delayed`). Labelled with `decision` and `reason`. |
| `otelcol_ratelimit.tokens` | Gauge | Current token level in the rate limiter bucket. Negative values mean the bucket is in debt (active throttling). Labelled by metadata keys and `limit_threshold`. |

#### Attributes

**`decision`** — the outcome of the rate limit check:

| Value | Meaning |
|-------|---------|
| `accepted` | Request passed immediately; tokens were available. |
| `delayed` | Request was held for a short wait while the bucket refilled (`throttle_behavior: delay`). No error is returned. |
| `throttled` | Request was rejected because the bucket was empty (`throttle_behavior: error`). An error is returned to the sender. |
| `cancelled` | Request was waiting for the bucket to refill but the client cancelled the context before the wait completed. The cancellation error is propagated back. |

**`reason`** — the rate-limiting state that produced the decision:

| Value | Meaning |
|-------|---------|
| `under_limit` | Bucket had enough tokens; request accepted immediately. |
| `over_limit` | Bucket was empty or in deficit. Applies to `delayed`, `throttled`, and `cancelled` decisions. |

**`limit_threshold`** — the configured token refill rate (tokens/second) for the key. Appears only on `otelcol_ratelimit.tokens`.
