### What changed

#### Two new metrics

| Metric | Type | When recorded |
|--------|------|---------------|
| `otelcol_ratelimit.delay_duration` | Histogram | Only when a request actually waited (`throttle_behavior: delay`) |
| `otelcol_ratelimit.tokens` | Gauge | Every request — shows current token bucket level, negative means active debt |

#### Richer `decision` attribute on `otelcol_ratelimit.requests`

Previously only `accepted` / `throttled` were distinguishable via the error return. Now every outcome is explicit:

| `decision` | `reason` | Meaning |
|------------|----------|---------|
| `accepted` | `under_limit` | Tokens available, passed immediately |
| `delayed` | `over_limit` | Waited for bucket to refill, no error returned |
| `throttled` | `over_limit` | Rejected — bucket empty in error mode |
| `cancelled` | `over_limit` | Was waiting in delay mode but client context was cancelled |

#### Internal: `RateLimiter.RateLimit` now returns `RateLimitResult`

The interface changed from `RateLimit(ctx, n) error` to `RateLimit(ctx, n) (RateLimitResult, error)`. The result carries `Decision`, `Delay`, `Tokens`, and `ConfigRate` so the processor layer can record all telemetry without the rate limiter needing any OTel imports.

### Test improvements

- `TestConsume_DelayMode` and `TestConsume_DelayMode_ContextCancelled` are now table-driven over all four signal types (logs, metrics, traces, profiles) — previously only logs were covered in delay mode.
- `local_test.go` now asserts `Decision`, `Delay`, and `ConfigRate` on all core behaviour tests, closing a gap where a wrong return value from `local.go` would have gone undetected.
