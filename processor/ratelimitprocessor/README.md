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
| `window_multiplier`    | The factor by which the previous window rate is multiplied to get the dynamic limit.                                                                                                                             | No       | `1.3`      |
| `window_duration`      | The time window duration for calculating traffic rates.                                                                                                                                                           | No       | `2m`       |

### Dynamic Rate Limiting Deep Dive

The dynamic rate limiting feature uses a sliding window approach to adjust the rate limit based on recent traffic patterns. This allows the processor to be more responsive to changes in traffic volume, preventing sudden spikes from overwhelming downstream services while also allowing for higher throughput when traffic is sustained.

The system maintains two time windows:

* **Current Window**: The most recent time window, defined by `window_duration`.
* **Previous Window**: The time window immediately preceding the current one.

The dynamic limit is calculated using the following simplified formula:

```text
dynamic_limit = max(static_rate, previous_rate * window_multiplier)
```

Where:

* `previous_rate`: The rate of traffic in the previous window (normalized per second).
* `static_rate`: The configured `rate` in the main configuration.
* `window_multiplier`: A factor applied to the previous window rate to determine the dynamic limit.

**Important Notes:**

* When `previous_rate` is 0 (no previous traffic), the dynamic limit defaults to the `static_rate`.
* The dynamic limit will always be at least the `static_rate`, ensuring a minimum level of throughput.
* The algorithm only records traffic in the current window when the incoming rate is within acceptable bounds to prevent runaway scaling.

Let's walk through a few examples to illustrate the behavior of the dynamic rate limiter.

Assume the following configuration:

* `window_duration`: 2m
* `window_multiplier`: 1.5
* `rate`: 1000 requests/second (this is our `static_rate`)

#### Scenario 1: Initial Traffic

1. **First 2 minutes**: 120,000 requests are received (1000 requests/second). `previous_rate` is 0.
   * Since `previous_rate` is 0, the `dynamic_limit` is set to the `static_rate` of 1000.
2. **Next 2 minutes**: 90,000 requests are received (750 requests/second). `previous_rate` is 1000.
   * `dynamic_limit = max(1000, 1000 * 1.5) = max(1000, 1500) = 1500`
   * The `dynamic_limit` increases to 1500, allowing more traffic.

#### Scenario 2: Ramping Up Traffic

1. **Previous window rate**: 900 requests/second.
2. **Current window**: Incoming rate of 1200 requests/second.
   * `dynamic_limit = max(1000, 900 * 1.5) = max(1000, 1350) = 1350`
   * The `dynamic_limit` increases to 1350, allowing the increased traffic.

#### Scenario 3: Sustained High Traffic

1. **Previous window rate**: 1500 requests/second.
2. **Current window**: Incoming rate of 1600 requests/second.
   * `dynamic_limit = max(1000, 1500 * 1.5) = max(1000, 2250) = 2250`
   * The `dynamic_limit` continues to increase as the high traffic is sustained.

#### Scenario 4: Traffic Spike Protection

1. **Previous window rate**: 1000 requests/second.
2. **Current window**: Sudden spike to 3000 requests/second.
   * `dynamic_limit = max(1000, 1000 * 1.5) = max(1000, 1500) = 1500`
   * The dynamic limit only allows 1500 requests/second, protecting against the spike.
   * The excess traffic (1500+ requests/second) is throttled, preventing the spike from being recorded.

#### Scenario 5: Traffic Reduction

1. **Previous window rate**: 2000 requests/second.
2. **Current window**: Reduced to 500 requests/second.
   * `dynamic_limit = max(1000, 2000 * 1.5) = max(1000, 3000) = 3000`
   * The `dynamic_limit` remains high initially, providing a buffer for traffic variations.
   * As the reduced traffic continues, the limit will gradually decrease in subsequent windows.

This mechanism allows the rate limiter to adapt to sustained increases in traffic while the `window_multiplier` provides protection against sudden spikes that could destabilize the system.

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
      window_multiplier: 1.5
      window_duration: 1m
```
