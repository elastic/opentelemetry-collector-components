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
| `retry_delay`       | Suggested client retry delay included via gRPC `RetryInfo` when throttled.                                                                                                                                       | No       | `1s`       |
| `type`              | Type of rate limiter. Options are `local` or `gubernator`.                                                                                                                                                        | No       | `local`    |
| `overrides`         | Allows customizing rate limiting parameters for specific metadata key-value pairs. Use this to apply different rate limits to different tenants, projects, or other entities identified by metadata. Each override is keyed by a metadata value and can specify custom `rate`, `burst`, and `throttle_interval` settings that take precedence over the global configuration for matching requests. | No       |            |
| `dynamic_limits`    | Holds the dynamic rate limiting configuration. This is only applicable when the rate limiter type is `gubernator`.                                                                                                   | No       |            |
| `classes`           | Named rate limit class definitions for class-based dynamic rate limiting. Only applicable when the rate limiter type is `gubernator`.                                                                              | No       |            |
| `default_class`     | Default class name to use when resolver returns unknown/empty class. Must exist in classes when set. Only applicable when the rate limiter type is `gubernator`.                                                 | No       |            |
| `class_resolver`    | Extension ID used to resolve a class name for a given unique key. Only applicable when the rate limiter type is `gubernator`.                                                                                    | No       |            |

### Overrides

You can override one or more of the following fields:

| Field               | Description                                                                                                                                                                                                       | Required | Default    |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|------------|
| `rate`              | Bucket refill rate, in tokens per second.                                                                                                                                                                         | No       |            |
| `burst`             | Maximum number of tokens that can be consumed.                                                                                                                                                                    | No       |            |
| `throttle_interval` | Time interval for throttling. It has effect only when `type` is `gubernator`.                                                                                                                                     | No       |            |
| `disable_dynamic`       | Disables dynamic rate limiting for the override.                                                                                                                                                                  | No       | `false`    |

### Dynamic Rate Limiting

| Field                  | Description                                                                                                                                                                                                       | Required | Default    |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|------------|
| `enabled`              | Enables the dynamic rate limiting feature.                                                                                                                                                                        | No       | `false`    |
| `window_duration`      | The time window duration for calculating traffic rates.                                                                                                                                                           | No       | `2m`       |
| `default_window_multiplier` | The default factor by which the previous window rate is multiplied to get the dynamic limit. Can be overridden by providing a `window_configurator` extension.                                               | No       | `1.3`      |
| `window_configurator`  | An optional extension to calculate window multiplier dynamically based on unique keys.                                                                                                                            | No       |            |

### Dynamic Rate Limiting Deep Dive

The dynamic rate limiting feature uses a sliding window approach to adjust the rate limit based on recent traffic patterns. This allows the processor to be more responsive to changes in traffic volume, preventing sudden spikes from overwhelming downstream services while also allowing for higher throughput when traffic is sustained.

The system maintains two windows. Eventually, the current window becomes the previous window.

* **Current Window**: The most recent time window, defined by `window_duration`.
* **Previous Window**: The time window immediately preceding the current one.

The dynamic limit is calculated using the following simplified formula:

```text
dynamic_limit = max(static_rate, previous_rate * window_multiplier)
```

Where:

* `previous_rate`: The rate of traffic in the previous window (normalized per second).
* `static_rate`: The configured `rate` in the main configuration.
* `window_multiplier`: A factor applied to the previous window rate to determine the dynamic limit. It is derived based on the following formula:

  ```text
  window_multiplier = default_window_multiplier
  if window_configurator is defined {
    window_multiplier = window_configurator::Multiplier(...)
  }
  ```

**Important Notes:**

* When `previous_rate` is `0` (no previous traffic), the dynamic limit defaults to the `static_rate`.
* The dynamic limit will always be at least the `static_rate`, ensuring a minimum level of throughput.
* The algorithm only records traffic in the current window when the incoming rate is within acceptable bounds to prevent runaway scaling.

Let's walk through a few examples to illustrate the behavior of the dynamic rate limiter.

Assume the following configuration:

* `window_duration`: 2m
* `window_multiplier`: 1.5 (with no `window_configurator` provided)
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

### Class-Based Dynamic Rate Limiting

When using the Gubernator rate limiter type, you can define named rate limit classes with different base rates and escalation policies. A class resolver extension maps unique keys to class names, enabling different rate limits per customer tier, API version, or other business logic.

Example with class-based configuration:

```yaml
extension:
  configmapclassresolverextension:
    name: resolutions
    namespace: default

processors:
  ratelimiter:
    metadata_keys:
    - x-customer-id
    rate: 100           # fallback rate
    burst: 200          # fallback burst
    type: gubernator
    strategy: requests

    # define the class resolver ID.
    class_resolver: configmapclassresolver
    # Define named classes
    classes:
      trial:
        rate: 50        # 50 requests/second for trial users
        burst: 100      # burst capacity of 100
        disable_dynamic: true  # no dynamic escalation

      paying:
        rate: 500       # 500 requests/second for paying customers
        burst: 1000     # burst capacity of 1000
        disable_dynamic: false

      enterprise:
        rate: 2000      # 2000 requests/second for enterprise
        burst: 4000     # burst capacity of 4000
        disable_dynamic: false   # allow gradual increase.

    # Default class when resolver returns unknown class
    default_class: "trial"

    # Enable dynamic rate limiting
    dynamic_limits:
      enabled: true
      window_multiplier: 1.3
      window_duration: 60s

    # Per-key overrides (highest precedence)
    overrides:
      "customer-123":
        rate: 5000      # special override
        burst: 10000
        disable_dynamic: true
```

Class Resolution Precedence:

1. **Per-key override** (highest) - From `overrides` section
2. **Resolved class** - From class resolver extension
3. **Default class** - From `default_class` setting
4. **Top-level fallback** (lowest) - From top-level `rate`/`burst`

### Class resolver

The `class_resolver` option tells the processor which OpenTelemetry Collector extension should be used to map a unique key (derived from the configured `metadata_keys`) to a class name. The value is the extension ID (the extension's configured name), for example:

```yaml
processors:
  ratelimiter:
    # This is an example class_resolver. It doesn't exist.
    class_resolver: configmapclassresolverextension
```

Behavior and notes:

* The resolver is optional. If no `class_resolver` is configured the processor skips class resolution and falls back to the top-level `rate`/`burst` values.

* The processor initializes the resolver during Start. If a `class_resolver` is configured but the extension is not found or fails to start, the processor fails to start with an error.

* When the resolver returns an unknown or empty class name, the processor treats it as "unknown" and uses the configured `default_class` (if set) or falls back to the top-level rate/burst.

Caching and performance:

* Implementations of resolver extensions should be mindful of latency; the processor assumes the resolver is reasonably fast. If the resolver returns an error at runtime the processor logs a warning and falls back to default/class precedence for that request.

* Ideally, implementations of the `class_resolver` implement their own caching to guarantee performance if they require on an external source.

Telemetry and metrics:

* The processor emits attributes on relevant metrics to aid debugging and monitoring:

  * `rate_source`: one of `static`, `dynamic`, `fallback`, or `degraded` (indicates whether dynamic calculation was used or not)
  * `class`: resolved class name when applicable
  * `source_kind`: which precedence path was used (`override`, `class`, or `fallback`)
  * `reason`: for dynamic escalations, one of `gubernator_error`, `success`, or `skipped`

* Metrics exposed by the processor include:

  * `otelcol_ratelimit.requests` — total number of rate limiting requests
  * `otelcol_ratelimit.request_duration` — histogram of request processing duration (seconds)
  * `otelcol_ratelimit.request_size` — histogram of bytes per request (only when strategy is `bytes`)
  * `otelcol_ratelimit.concurrent_requests` — current number of in-flight requests
  * `otelcol_ratelimit.resolver.failures` — total number of class resolver failures
  * `otelcol_ratelimit.dynamic.escalations` — count of dynamic rate decisions (attributes: `class`, `source_kind`, `reason`)

### Throttling rate based on custom logic using `window_configurator`

The `window_configurator` option configures a custom OpenTelemetry Collector extension to dynamically choose a window multiplier for the current period. The value is the extension ID (the extension's configured name). The extension MUST implement the `WindowConfigurator` interface. The multiplier can be used to scale up the rate limit from the previous window (by returning a multiplier greater than `1`) or scale down the rate limit from the previous window (by returning a multiplier less than `1`, greater than `0`). The lowest possible value of the rate is `1/s`. If the extension returns a negative value, then the `default_window_multiplier` will be used. An example configuration including the window configurator:

```yaml
processors:
  ratelimiter:
    dynamic_limits:
      enabled: true
      window_duration: 2m
      default_window_multiplier: 1.5
      # This is an example window configurator. It doesn't exist.
      window_configurator: kafkalagwindowconfiguratorextension
```
