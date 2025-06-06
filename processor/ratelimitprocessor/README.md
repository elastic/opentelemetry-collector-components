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
| `overrides`         | Map of metadata key overrides for the rate limiter. See the possible overrides and examples below.                                                                                                                | No       |            |

### Overrides

You can override one or more of the following fields:

| Field               | Description                                                                                                                                                                                                       | Required | Default    |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|------------|
| `rate`              | Bucket refill rate, in tokens per second.                                                                                                                                                                         | No       |            |
| `burst`             | Maximum number of tokens that can be consumed.                                                                                                                                                                    | No       |            |
| `throttle_interval` | Time interval for throttling. It has effect only when `type` is `gubernator`.                                                                                                                                     | No       |            |

### Examples

Example when using as a local rate limiter:

```yaml
processors:
  ratelimiter:
    metadata_keys:
    - x-elastic-project-id
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
    - x-elastic-project-id
    rate: 1000
    burst: 10000
    strategy: requests
    type: gubernator
```

You can configure the Gubernator using `GUBER_*` [environment variables](https://github.com/gubernator-io/gubernator/blob/master/example.conf).

Example when using as a distributed rate limiter (Gubernator) with overrides:

```yaml
processors:
  ratelimiter:
    metadata_keys:
    - x-elastic-project-id
    rate: 1000
    burst: 10000
    strategy: requests
    type: gubernator
    overrides:
      project-id:e678ebd7-3a15-43dd-a95c-1cf0639a6292:
        rate: 2000
        burst: 20000
        throttle_interval: 10s
```
