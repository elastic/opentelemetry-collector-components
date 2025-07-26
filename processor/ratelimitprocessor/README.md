## Rate limiter processor


Processor to rate limit the data sent from a receiver to an exporter. The rate limiter processor implements
a in-memory rate limiter, or makes use of a [gubernator](https://github.com/gubernator-io/gubernator) instance, if configured.


## Configuration

| Field               | Description                                                                                                                                                                                                       | Required | Default    |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|------------|
| `metadata_keys`     | List of metadata keys inside the request's context that will be used to create a unique key to make a [rate limit request](https://pkg.go.dev/github.com/mailgun/gubernator/v2#section-readme) to the gubernator. | No       |            |
| `strategy`          | Rate limit by requests (`requests`), records (`records`), or by bytes (`bytes`). If by `records`, then it will limit what is applicable between log record, span, metric data point, or profile sample.           | Yes      | `requests` |
| `rate`              | Bucket refill rate, in tokens per second.                                                                                                                                                                         | Yes      |            |
| `burst`             | Maximum number of tokens that can be consumed.                                                                                                                                                                    | Yes      |            |
| `throttle_behavior` | Processor behavior for when the rate limit is exceeded. Options are `error`, return an error immediately on throttle and does not send the event, and `delay`, delay the sending until it is no longer throttled. | Yes      | `error`    |
| `throttle_interval` | Time interval for throttling. It has effects only when `type` is `gubernator`.                                                                                                                                    | No       | `1s`       |
| `type`              | Type of rate limiter. Options are `local` or `gubernator`.                                                                                                                                                        | No       | `local`    |
| `overrides`         | Allows customizing rate limiting parameters for specific metadata key-value pairs. Use this to apply different rate limits to different tenants, projects, or other entities identified by metadata. Each override is keyed by a metadata value and can specify custom `rate`, `burst`, and `throttle_interval` settings that take precedence over the global configuration for matching requests. | No       |            |
| `dynamic_limits`    | Holds the dynamic rate limiting configuration. This is only applicable when the rate limiter type is `gubernator`.                                                                                                   | No       |            |

### Overrides

You can override one or more of the following fields:

| Field               | Description                                                                                                                                                                                                       | Required | Default    |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|------------|
| `rate`              | Bucket refill rate, in tokens per second.                                                                                                                                                                         | No       |            |
| `burst`             | Maximum number of tokens that can be consumed.                                                                                                                                                                    | No       |            |
| `throttle_interval` | Time interval for throttling. It has effect only when `type` is `gubernator`.                                                                                                                                     | No       |            |
| `static_only`       | Disables dynamic rate limiting for the override.                                                                                                                                                                  | No       | `false`    |

### Dynamic Rate Limiting

| Field                  | Description                                                                                                                                                                                                       | Required | Default    |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|------------|
| `enabled`              | Enables the dynamic rate limiting feature.                                                                                                                                                                        | No       | `false`    |
| `ewma_multiplier`      | The factor by which the current EWMA is multiplied to get the dynamic part of the limit.                                                                                                                            | No       | `1.5`      |
| `ewma_window`          | The time window for the Exponentially Weighted Moving Average.                                                                                                                                                    | No       | `5m`       |
| `recent_window_weight` | The weight given to the recent window of the EWMA.                                                                                                                                                                | No       | `0.75`     |

### Dynamic Rate Limiting Deep Dive

The dynamic rate limiting feature uses a sliding window approach with an Exponentially Weighted Moving Average (EWMA) to adjust the rate limit based on recent traffic patterns. This allows the processor to be more responsive to changes in traffic volume, preventing sudden spikes from overwhelming downstream services while also allowing for higher throughput when traffic is low.

The system maintains two time windows:

*   **Current Window**: The most recent time window, defined by `ewma_window`.
*   **Previous Window**: The time window immediately preceding the current one.

The dynamic limit is calculated using the following formula:

```
ewma = (recent_window_weight * current_rate) + ((1 - recent_window_weight) * previous_rate)
dynamic_limit = max(static_rate, min(ewma * ewma_multiplier, previous_rate * ewma_multiplier))
```

Where:

*   `current_rate`: The rate of traffic in the current window.
*   `previous_rate`: The rate of traffic in the previous window.
*   `static_rate`: The configured `rate` in the main configuration.
*   `recent_window_weight`: The weight given to the `current_rate` in the EWMA calculation.
*   `ewma_multiplier`: A factor to prevent the dynamic limit from growing too quickly.

**Important Notes:**

* When `previous_rate` is 0 (no previous traffic), the dynamic limit defaults to the `static_rate`.
* When `current_rate` is 0 (no current traffic), the dynamic limit is set to `previous_rate * ewma_multiplier`.
* The `min()` operation ensures the dynamic limit doesn't grow faster than `previous_rate * ewma_multiplier`, providing stability during traffic spikes.

Let's walk through a few examples to illustrate the behavior of the dynamic rate limiter.

Assume the following configuration:

* `ewma_window`: 5m
* `recent_window_weight`: 0.75
* `ewma_multiplier`: 1.5
* `rate`: 1000 requests/second (this is our `static_rate`)

**Scenario 1: Initial Traffic**

1. **First 5 minutes**: 120,000 requests are received (400 requests/second). `previous_rate` is 0.
   * Since `previous_rate` is 0, the `dynamic_limit` is capped at the `static_rate` of 1000.
2. **Next 5 minutes**: 150,000 requests are received (500 requests/second). `previous_rate` is 400.
   * `ewma = (0.75 * 500) + (0.25 * 400) = 375 + 100 = 475`
   * `dynamic_limit = max(1000, min(475 * 1.5, 400 * 1.5)) = max(1000, min(712.5, 600)) = max(1000, 600) = 1000`
   * The `dynamic_limit` remains at the `static_rate` as the traffic is below the threshold.

**Scenario 2: Ramping Up Traffic**

1. **Previous window rate**: 900 requests/second.
2. **Current window rate**: 1200 requests/second.
   * `ewma = (0.75 * 1200) + (0.25 * 900) = 900 + 225 = 1125`
   * `dynamic_limit = max(1000, min(1125 * 1.5, 900 * 1.5)) = max(1000, min(1687.5, 1350)) = max(1000, 1350) = 1350`
   * The `dynamic_limit` increases to 1350, allowing more traffic.

**Scenario 3: Sustained High Traffic**

1. **Previous window rate**: 1500 requests/second.
2. **Current window rate**: 1600 requests/second.
   * `ewma = (0.75 * 1600) + (0.25 * 1500) = 1200 + 375 = 1575`
   * `dynamic_limit = max(1000, min(1575 * 1.5, 1500 * 1.5)) = max(1000, min(2362.5, 2250)) = max(1000, 2250) = 2250`
   * The `dynamic_limit` continues to increase as the high traffic is sustained.

**Scenario 4: Traffic Spike**

1. **Previous window rate**: 1000 requests/second.
2. **Current window rate**: 3000 requests/second.
   * `ewma = (0.75 * 3000) + (0.25 * 1000) = 2250 + 250 = 2500`
   * `dynamic_limit = max(1000, min(2500 * 1.5, 1000 * 1.5)) = max(1000, min(3750, 1500)) = max(1000, 1500) = 1500`
   * The `ewma_multiplier` dampens the response to the spike, preventing the limit from jumping to 3750 immediately.

**Scenario 5: Traffic Reduction**

1. **Previous window rate**: 2000 requests/second.
2. **Current window rate**: 500 requests/second.
   * `ewma = (0.75 * 500) + (0.25 * 2000) = 375 + 500 = 875`
   * `dynamic_limit = max(1000, min(875 * 1.5, 2000 * 1.5)) = max(1000, min(1312.5, 3000)) = max(1000, 1312.5) = 1312.5`
   * The `dynamic_limit` decreases, but not as drastically as the traffic reduction, providing a buffer.

This mechanism allows the rate limiter to adapt to sustained increases in traffic, while the `ewma_multiplier` prevents abrupt changes that could destabilize the system.

### Examples

Example when using as a local rate limiter:

```yaml
processors:
  ratelimiter:
    metadata_keys:
    - x-tenant-id
    rate: 1000
    burst: 10000
    strategy: requests
    # type: local # local is the default
```

Example when using as a distributed rate limiter (Gubernator):

```yaml
processors:
  ratelimiter:
    metadata_keys:
    - x-tenant-id
    rate: 1000
    burst: 10000
    strategy: requests
    type: gubernator
```

You can configure the Gubernator using `GUBER_*` [environment variables](https://github.com/gubernator-io/gubernator/blob/master/example.conf).

Example when using as a distributed rate limiter (Gubernator) with throttle interval:

```yaml
processors:
  ratelimiter:
    metadata_keys:
    - x-tenant-id
    rate: 1000
    burst: 10000
    strategy: requests
    type: gubernator
    throttle_interval: 10s # has effect only when `type` is `gubernator`
```

Example when using as a distributed rate limiter (Gubernator) with overrides:

```yaml
processors:
  ratelimiter:
    metadata_keys:
    - x-tenant-id
    rate: 1000
    burst: 10000
    strategy: requests
    type: gubernator
    overrides:
      project-id:e678ebd7-3a15-43dd-a95c-1cf0639a6292:
        rate: 2000
        burst: 20000
        throttle_interval: 10s # has effect only when `type` is `gubernator`
```

Example when using as a distributed rate limiter (Gubernator) with dynamic rate limiting:

```yaml
processors:
  ratelimiter:
    rate: 100
    # NOTE burst isn't used when dynamic limits are enabled.
    burst: 200
    type: gubernator
    strategy: bytes
    throttle_behavior: error
    dynamic_limits:
      enabled: true
      ewma_multiplier: 1.5
      ewma_window: 1m
```
