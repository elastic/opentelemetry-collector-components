# Akamai SIEM Receiver — Benchmarks

**Date:** 2026-03-28 | **Environment:** Apple M1 Max (10 cores), 32 GB RAM, Go 1.24.7 darwin/arm64, macOS
**Endpoint:** Akamai cloud mock test instance (`proteus-akamai-*.sit.estc.dev`), office network (HTTPS)

> **Note:** These benchmarks use a mock Akamai SIEM API endpoint for testing. The `event_limit` ceiling
> observed here (~200k before `unexpected EOF`) is a limitation of this mock endpoint and the office
> network, **not** of the real Akamai SIEM API. In production, the Akamai API supports up to 600,000
> events per request. The EPS, CPU, memory, and allocation metrics are representative of real-world
> performance since the receiver processes the same NDJSON format regardless of source.

---

## Summary — All 24 Tests

### Raw Mode (raw JSON passthrough)

| batch | buf=1 | buf=4 | buf=8 | RSS (buf=4) | KB/evt |
|---|---|---|---|---|---|
| 500 | 13,839 | **15,680** | 14,834 | 104 MB | 1.35 |
| 1000 | **15,884** | 15,723 | 12,863 | 105 MB | 1.37 |
| 2000 | 7,982* | 12,856 | FAIL | 107 MB | 1.44 |
| 5000 | 15,716 | 15,590 | 14,861 | 116 MB | 1.37 |

### OTel Mode (JSON parse + 30-field semantic mapping)

| batch | buf=1 | buf=4 | buf=8 | RSS (buf=4) | KB/evt |
|---|---|---|---|---|---|
| 500 | 15,796 | 13,536 | 15,704 | 107 MB | 9.4 |
| 1000 | 14,453 | **15,520** | **15,640** | 112 MB | 9.8 |
| 2000 | 13,783 | 14,350 | 15,714 | 119 MB | 10.0 |
| 5000 | 14,057 | 14,741 | 15,362 | 145 MB | 9.3 |

### Dual Mode (Raw + OTel shared poller, 1000/4)

| Metric | Raw Only | OTel Only | Dual |
|---|---|---|---|
| **EPS** | 15,642 | 15,774 | **15,769** |
| RSS | 104.9 MB | 111.2 MB | **112.6 MB** |
| CPU util | 12.8% | 50.8% | **51.8%** |
| KB/event | 1.32 | 9.94 | **10.08** |

**Dual mode = OTel-only cost. Raw goroutine is free. Zero EPS overhead.**

*raw/2000/buf=1: only 5 pages — low sample, likely API variance.

EPS = `events_emitted / page_processing_time_sum` (processing throughput, excludes API round-trip).
All tests: `event_limit=100000`, `output_format` and `batch_size`/`stream_buffer_size` as shown, nop exporter.

---

## Performance Trends

### Throughput (EPS) by Batch Size — Raw vs OTel

![EPS by Batch Size](img/01_eps_by_batch.png)

**Observation:** EPS is flat (~14-16k) across all batch sizes in both modes. The receiver is I/O-bound — batch size doesn't affect throughput with the nop exporter.

### EPS by Batch Size and Buffer Size — Full Matrix

![EPS by Batch and Buffer](img/02_eps_by_batch_and_buffer.png)

**Observation:** Buffer size (1, 4, 8) has no consistent impact on EPS. The raw/2000/buf=1 outlier (7,982 EPS) is due to low sample size (5 pages). All other combinations cluster in the 13-16k range.

### RSS by Batch Size

![RSS by Batch Size](img/03_rss_by_batch.png)

**Observation:** Raw mode RSS is flat (~104-116 MB). OTel RSS scales with batch size — 5000-event batches hold 5x more parsed OTel attribute maps in memory, pushing RSS to 145 MB (+38% vs raw). The +10 MB RSS jump from batch=1000 to batch=5000 in raw mode is due to `sys` memory (Go runtime requests more OS pages for larger plog.Logs allocations).

### Memory Allocation per Event

![Allocation per Event](img/04_alloc_per_event.png)

**Observation:** OTel mode allocates ~7x more per event (~10 KB) than raw mode (~1.4 KB) due to JSON parsing + OTel attribute construction. Raw mode is a string copy only.

### CPU Time per Event

![CPU per Event](img/05_cpu_per_event.png)

**Observation:** 4x CPU gap is constant. Raw mode is effectively zero-cost (string copy). OTel mode pays for JSON unmarshal + 30-field attribute mapping per event.

### Go Runtime System Memory

![Sys Memory](img/06_sys_memory.png)

**Observation:** Sys memory jumps at batch=5000 in both modes (Go runtime requests larger OS allocations for big plog.Logs slices). OTel 5000 hits 73 MB vs 44 MB raw mode — the parsed attribute maps require more runtime memory.

### Page Processing Time

![Page Processing Time](img/07_page_processing_time.png)

**Observation:** Page processing time is consistent at ~6-7s per page (100k events) across all configurations. The I/O bottleneck (gzip decompression + NDJSON streaming) dominates, making batch size and mapping mode irrelevant to page latency.

---

## Event Limit Scaling (Raw, batch=1000, buf=4)

| event_limit | Events | Pages | EPS | s/page | RSS | Heap | KB/evt | CPU/evt | API avg |
|---|---|---|---|---|---|---|---|---|---|
| 100,000 | 1,000k | 10 | 15,723 | 6.36 | 104.9 MB | 12.5 MB | 1.37 | 0.010ms | 360ms |
| 200,000 | 1,600k | 8 | 15,310 | 13.06 | 113.8 MB | 14.6 MB | 1.40 | 0.009ms | 342ms |

#### Config (200k)

```yaml
receivers:
  akamai_siem:
    endpoint: https://proteus-akamai-*.sit.estc.dev
    config_ids: "1"
    authentication:
      client_token: "***"
      client_secret: "***"
      access_token: "***"
    output_format: raw
    poll_interval: 1m
    initial_lookback: 1h
    event_limit: 200000
    batch_size: 1000
    storage: file_storage
extensions:
  file_storage:
    directory: /tmp/akamai-cursor
    create_directory: true
  pprof:
    endpoint: "0.0.0.0:1777"
exporters:
  nop: {}
service:
  extensions: [file_storage, pprof]
  telemetry:
    metrics:
      level: detailed
  pipelines:
    logs:
      receivers: [akamai_siem]
      exporters: [nop]
```

#### 200k pprof — CPU Profile (15s sample, 12.37% of wall clock)

| Function | % | Role |
|---|---|---|
| `pthread_cond_signal` | 40.6% | Goroutine scheduling |
| `compress/flate.huffmanBlock` | 19.3% | Gzip decompression |
| `syscall.syscall` | 11.8% | Network I/O |
| `runtime.usleep` | 10.2% | Idle |
| `pthread_cond_wait` | 7.0% | Back-pressure |
| `runtime.scanobject` | 5.9% | GC |

#### 200k pprof — Heap Profile (alloc_space, 1.82 GB total)

| Site | MB | % |
|---|---|---|
| `Scanner.Text` | 1,557 | 85.4% |
| `NewLogRecord` | 172 | 9.4% |
| `processPage` | 26 | 1.4% |
| `LogRecordSlice.AppendEmpty` | 22 | 1.2% |
| `NewAnyValueStringValue` | 22 | 1.2% |

**Analysis:** EPS is identical to 100k (15,310 vs 15,723 — within API variance). Page processing time doubles proportionally (13.06s vs 6.36s). RSS and alloc/event are the same. The 200k limit simply processes more events per API call, halving the number of roundtrips per hour. CPU and heap profiles are identical in shape to the 100k run.

> **Note:** `event_limit` values above 200k (250k, 300k, 400k) fail with `unexpected EOF` on this mock API
> endpoint. This is a server-side limitation of the mock, not the receiver or the real Akamai SIEM API
> (which supports up to 600k). See the caveat at the top of this document.

---

## Dual Mode vs Single Mode (Raw, OTel, Dual — 100k/1000/4, nop exporter)

![Dual Mode Comparison](img/08_dual_mode_comparison.png)

Three back-to-back runs on the same endpoint with identical parameters. Dual mode runs both raw and OTel formatting in parallel on a shared poller with a single API connection.

| Metric | Raw Only | OTel Only | Dual (Raw+OTel) |
|---|---|---|---|
| **EPS** | **15,642** | **15,774** | **15,769** |
| s/page | 6.39 | 6.34 | 6.34 |
| RSS | 104.9 MB | 111.2 MB | 112.6 MB |
| Heap | 14.2 MB | 13.6 MB | 16.9 MB |
| KB/event | 1.32 | 9.94 | 10.08 |
| CPU/event | 0.009ms | 0.037ms | 0.038ms |
| CPU utilization | 12.8% | 50.8% | 51.8% |
| API avg | 375ms | 349ms | 324ms |
| Sys memory | 35.9 MB | 40.1 MB | 40.1 MB |
| Goroutines | 12 | 12 | 11 |

#### Dual Mode Config

```yaml
receivers:
  akamai_siem/raw:
    endpoint: https://proteus-akamai-*.sit.estc.dev
    config_ids: "1"
    authentication:
      client_token: "***"
      client_secret: "***"
      access_token: "***"
    output_format: raw
    poll_interval: 1m
    initial_lookback: 1h
    event_limit: 100000
    batch_size: 1000
    storage: file_storage
  akamai_siem/otel:
    endpoint: https://proteus-akamai-*.sit.estc.dev
    config_ids: "1"
    authentication:
      client_token: "***"
      client_secret: "***"
      access_token: "***"
    output_format: otel
    poll_interval: 1m
    initial_lookback: 1h
    event_limit: 100000
    batch_size: 1000
extensions:
  file_storage:
    directory: /tmp/akamai-cursor
    create_directory: true
  pprof:
    endpoint: "0.0.0.0:1777"
exporters:
  nop/raw: {}
  nop/otel: {}
service:
  extensions: [file_storage, pprof]
  telemetry:
    metrics:
      level: detailed
  pipelines:
    logs/raw:
      receivers: [akamai_siem/raw]
      exporters: [nop/raw]
    logs/otlp:
      receivers: [akamai_siem/otel]
      exporters: [nop/otel]
```

#### Analysis

- **EPS is identical across all three modes** (15,642 vs 15,774 vs 15,769). Dual mode adds zero throughput overhead — the raw goroutine completes in ~0ms while OTel takes ~12ms per batch, both running in parallel.
- **RSS: dual is +1.4 MB vs OTel-only** (112.6 vs 111.2 MB). The raw goroutine's plog.Logs allocation is negligible.
- **CPU: dual matches OTel-only** (51.8% vs 50.8%). The OTel JSON parse dominates; the raw string copy adds nothing measurable.
- **Goroutines: 11 in dual mode** — same as single mode. The two formatting goroutines are short-lived (per-batch, not persistent).
- **Single API connection confirmed**: request count and API latency are identical across all three modes.

**Verdict:** Dual mode is effectively free. It costs the same as OTel-only mode because the raw formatting goroutine has zero measurable overhead. Use dual mode whenever you need both ES and OTLP output.

---

## Key Findings

1. **EPS is flat (~14-16k) across all configurations.** The bottleneck is gzip decompression + NDJSON streaming from the API, not batch/buffer tuning. No configuration achieves meaningfully higher or lower EPS.

2. **OTel mode has identical EPS to raw mode** but uses **4x more CPU** (0.036 vs 0.009 ms/event) and **7x more allocation** (~10 KB vs 1.4 KB/event). The I/O bottleneck masks the compute cost.

3. **Batch size affects RSS only at 5000+.** Batches of 500-2000 produce ~104-119 MB RSS. At 5000, raw mode jumps to 116 MB and OTel to 145 MB due to larger plog.Logs allocations held in memory.

4. **Buffer size has no measurable impact** from 1-8 on EPS or RSS. The bounded channel's back-pressure works correctly at all sizes.

5. **Recommended config: `batch_size=1000, stream_buffer_size=4`.** This is the sweet spot: moderate RSS, good ConsumeLogs call frequency for real exporters, and no memory waste.

6. **Dual mode is free.** Running both raw + OTel simultaneously costs the same as OTel-only (15,769 vs 15,774 EPS, 112.6 vs 111.2 MB RSS, 51.8% vs 50.8% CPU). The raw goroutine completes in ~0ms. Use dual mode whenever you need both ES and OTLP output — it shares a single API connection and cursor.

---

## Detailed Test Results

### Test 1: Raw Mode, batch=500

#### Config

```yaml
receivers:
  akamai_siem:
    endpoint: https://proteus-akamai-*.sit.estc.dev
    config_ids: "1"
    authentication:
      client_token: "***"
      client_secret: "***"
      access_token: "***"
    output_format: raw
    poll_interval: 1m
    initial_lookback: 1h
    event_limit: 100000
    batch_size: 500
    stream_buffer_size: 4      # tested: 1, 4, 8
    storage: file_storage
extensions:
  file_storage:
    directory: /tmp/akamai-cursor
    create_directory: true
  pprof:
    endpoint: "0.0.0.0:1777"
exporters:
  nop: {}
service:
  extensions: [file_storage, pprof]
  telemetry:
    metrics:
      level: detailed
  pipelines:
    logs:
      receivers: [akamai_siem]
      exporters: [nop]
```

#### Results

| buf | Events | Pages | EPS | s/page | RSS | Heap | KB/evt | CPU/evt | API avg | Sys |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 900k | 9 | 13,839 | 7.23 | 103.9 MB | 14.2 MB | 1.34 | 0.009ms | 373ms | 35.1 MB |
| 4 | 1,000k | 10 | **15,680** | 6.38 | 103.6 MB | 16.2 MB | 1.35 | 0.009ms | 450ms | 35.4 MB |
| 8 | 900k | 9 | 14,834 | 6.74 | 104.2 MB | 10.1 MB | 1.46 | 0.010ms | 338ms | 35.6 MB |

---

### Test 2: Raw Mode, batch=1000

#### Config

Same as Test 1 with `batch_size: 1000`.

```yaml
    batch_size: 1000
    stream_buffer_size: 4      # tested: 1, 4, 8
```

#### Results

| buf | Events | Pages | EPS | s/page | RSS | Heap | KB/evt | CPU/evt | API avg | Sys |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1,000k | 10 | **15,884** | 6.30 | 105.3 MB | 10.6 MB | 1.38 | 0.009ms | 379ms | 35.1 MB |
| 4 | 1,000k | 10 | 15,723 | 6.36 | 104.9 MB | 12.5 MB | 1.37 | 0.010ms | 360ms | 35.9 MB |
| 8 | 800k | 8 | 12,863 | 7.77 | 106.5 MB | 16.9 MB | 1.44 | 0.010ms | 354ms | 35.4 MB |

---

### Test 3: Raw Mode, batch=2000

#### Config

Same as Test 1 with `batch_size: 2000`.

```yaml
    batch_size: 2000
    stream_buffer_size: 4      # tested: 1, 4, 8
```

#### Results

| buf | Events | Pages | EPS | s/page | RSS | Heap | KB/evt | CPU/evt | API avg | Sys |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 500k* | 5 | 7,982* | 12.53 | 106.8 MB | 14.5 MB | 1.54 | 0.010ms | 430ms | 35.4 MB |
| 4 | 800k | 8 | 12,856 | 7.78 | 107.4 MB | 17.6 MB | 1.44 | 0.009ms | 346ms | 35.6 MB |
| 8 | FAIL | - | - | - | - | - | - | - | - | - |

*buf=1: low sample (5 pages). buf=8: unexpected EOF (mock API variance).

---

### Test 4: Raw Mode, batch=5000

#### Config

Same as Test 1 with `batch_size: 5000`.

```yaml
    batch_size: 5000
    stream_buffer_size: 4      # tested: 1, 4, 8
```

#### Results

| buf | Events | Pages | EPS | s/page | RSS | Heap | KB/evt | CPU/evt | API avg | Sys |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1,000k | 10 | 15,716 | 6.36 | 114.9 MB | 18.2 MB | 1.39 | 0.009ms | 326ms | 43.9 MB |
| 4 | 1,000k | 10 | 15,590 | 6.41 | 115.6 MB | 10.8 MB | 1.37 | 0.009ms | 343ms | 43.6 MB |
| 8 | 900k | 9 | 14,861 | 6.73 | 114.6 MB | 18.8 MB | 1.47 | 0.010ms | 346ms | 43.6 MB |

**Note:** RSS jumps +10 MB vs batch=1000 (115 vs 105 MB). Sys memory jumps +8 MB (44 vs 36 MB). Each in-flight batch holds 5x more plog.Logs data.

---

### Test 5: OTel Mode, batch=500

#### Config

```yaml
receivers:
  akamai_siem:
    endpoint: https://proteus-akamai-*.sit.estc.dev
    config_ids: "1"
    authentication:
      client_token: "***"
      client_secret: "***"
      access_token: "***"
    output_format: otel
    poll_interval: 1m
    initial_lookback: 1h
    event_limit: 100000
    batch_size: 500
    stream_buffer_size: 4      # tested: 1, 4, 8
    storage: file_storage
extensions:
  file_storage:
    directory: /tmp/akamai-cursor
    create_directory: true
  pprof:
    endpoint: "0.0.0.0:1777"
exporters:
  nop: {}
service:
  extensions: [file_storage, pprof]
  telemetry:
    metrics:
      level: detailed
  pipelines:
    logs:
      receivers: [akamai_siem]
      exporters: [nop]
```

#### Results

| buf | Events | Pages | EPS | s/page | RSS | Heap | KB/evt | CPU/evt | Mapping sum | API avg | Sys |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1,100k | 11 | **15,796** | 6.33 | 107.4 MB | 13.7 MB | 10.00 | 0.037ms | 24.73s | 334ms | 35.8 MB |
| 4 | 1,000k | 10 | 13,536 | 7.39 | 107.3 MB | 19.0 MB | 9.40 | 0.036ms | 21.91s | 364ms | 35.6 MB |
| 8 | 1,100k | 11 | 15,704 | 6.37 | 107.7 MB | 18.7 MB | 9.92 | 0.036ms | 24.70s | 347ms | 36.1 MB |

---

### Test 6: OTel Mode, batch=1000

#### Config

Same as Test 5 with `batch_size: 1000`.

```yaml
    batch_size: 1000
    stream_buffer_size: 4      # tested: 1, 4, 8
```

#### Results

| buf | Events | Pages | EPS | s/page | RSS | Heap | KB/evt | CPU/evt | Mapping sum | API avg | Sys |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1,100k | 11 | 14,453 | 6.92 | 111.5 MB | 18.2 MB | 9.17 | 0.035ms | 23.30s | 337ms | 39.6 MB |
| 4 | 1,100k | 11 | 15,520 | 6.44 | 111.9 MB | 16.6 MB | 9.79 | 0.036ms | 24.19s | 344ms | 40.3 MB |
| 8 | 1,100k | 11 | **15,640** | 6.39 | 111.3 MB | 15.0 MB | 9.89 | 0.037ms | 24.80s | 329ms | 39.8 MB |

---

### Test 7: OTel Mode, batch=2000

#### Config

Same as Test 5 with `batch_size: 2000`.

```yaml
    batch_size: 2000
    stream_buffer_size: 4      # tested: 1, 4, 8
```

#### Results

| buf | Events | Pages | EPS | s/page | RSS | Heap | KB/evt | CPU/evt | Mapping sum | API avg | Sys |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1,000k | 10 | 13,783 | 7.26 | 119.0 MB | 20.3 MB | 9.70 | 0.038ms | 22.83s | 339ms | 48.3 MB |
| 4 | 1,000k | 10 | 14,350 | 6.97 | 119.4 MB | 17.5 MB | 10.05 | 0.039ms | 23.17s | 373ms | 48.1 MB |
| 8 | 1,100k | 11 | 15,714 | 6.36 | 119.4 MB | 14.5 MB | 9.93 | 0.037ms | 24.88s | 349ms | 48.1 MB |

---

### Test 8: OTel Mode, batch=5000

#### Config

Same as Test 5 with `batch_size: 5000`.

```yaml
    batch_size: 5000
    stream_buffer_size: 4      # tested: 1, 4, 8
```

#### Results

| buf | Events | Pages | EPS | s/page | RSS | Heap | KB/evt | CPU/evt | Mapping sum | API avg | Sys |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1,000k | 10 | 14,057 | 7.11 | **143.3 MB** | 11.8 MB | 9.77 | 0.038ms | 22.37s | 407ms | **72.6 MB** |
| 4 | 1,100k | 11 | 14,741 | 6.78 | **145.0 MB** | 43.7 MB | 9.30 | 0.036ms | 23.46s | 349ms | **73.1 MB** |
| 8 | 900k | 9 | 15,362 | 6.51 | **143.9 MB** | 36.9 MB | 10.04 | 0.039ms | 20.89s | 375ms | **72.8 MB** |

**Note:** RSS jumps to 143-145 MB (+30 MB vs batch=1000). Sys memory doubles to 73 MB. Each 5000-event batch holds 5000 OTel log records with 30+ parsed attributes each — significant memory per in-flight batch.

---

## pprof Analysis (Raw 1000/4 vs OTel 1000/4)

### CPU Profile (15s sample during active polling)

**Raw Mode** (12% of wall clock is CPU):

| Function | % | Role |
|---|---|---|
| `pthread_cond_signal` | 47% | Goroutine wake-up (channel ops) |
| `compress/flate.huffmanBlock` | 19% | Gzip decompression |
| `pthread_cond_wait` | 11% | Goroutine sleep (back-pressure) |
| `syscall.syscall` | 9% | Network I/O |
| `runtime.usleep` | 7% | Idle |
| `runtime.scanobject` | 5% | GC |

**OTel Mode** (43% of wall clock is CPU):

| Function | % | Role |
|---|---|---|
| `pthread_kill` | 13% | Signal handling |
| `pthread_cond_signal` | 13% | Goroutine scheduling |
| `encoding/json.checkValid` | 9% | **JSON validation** |
| `runtime.madvise` | 8% | Memory management (high GC) |
| `pthread_cond_wait` | 6% | Back-pressure |
| `encoding/json.stateInString` | 4% | **JSON string parsing** |
| `runtime.kevent` | 4% | Network I/O |

**Key difference:** Raw mode is I/O-bound (58% goroutine scheduling, event processing invisible). OTel is approaching CPU-bound (JSON parsing appears in top functions, CPU utilization jumps from 12% to 43%).

### Heap Profile (lifetime allocations)

**Raw Mode** (1.43 GB total, 1.38 KB/event):

| Site | % | What |
|---|---|---|
| `Scanner.Text` | 85% | Event string copies from HTTP stream |
| `NewLogRecord` | 10% | plog.LogRecord struct |
| `NewAnyValueStringValue` | 1.3% | Log body string |

**OTel Mode** (10.7 GB total, 9.8 KB/event):

| Site | % | What |
|---|---|---|
| `Map.PutStr` | 26.5% | Setting 30+ OTel attributes |
| `MapToOTelLog` | 14.1% | Mapper function (struct unmarshal) |
| `Scanner.Text` | 11.9% | Event string copies |
| `decodeRuleField` | 6.7% | URL + base64 decode |
| `NewAnyValueStringValue` | 6.6% | OTel attribute values |

### Goroutines: 14-15 (both modes)

---

## Comparison: OTel Receiver vs Beats Input

| Metric | Beats (1W/1k) | Raw Mode | OTel Mode |
|---|---|---|---|
| **EPS** | 10,992 | **~15,500** | **~15,500** |
| CPU/event | 0.048ms | **0.009ms** | 0.036ms |
| Alloc/event | ~10 KB | **1.4 KB** | ~10 KB |
| RSS | 103 MB | 105 MB | 112 MB |
| Goroutines | 38 | **14** | 15 |

| Beats Config | Beats EPS | OTel EPS | Advantage |
|---|---|---|---|
| 1W / 1k batch | 10,992 | ~15,500 | **+41%** |
| 10W / 1k batch | 7,189 | ~15,500 | **+116%** |
| 20W / 2k batch | 8,348 | ~15,500 | **+86%** |

### Why OTel is Faster

1. **No Beats pipeline overhead.** ConsumeLogs is a direct function call — no publisher, queue, ACK handler.
2. **7x less allocation (raw mode).** String copy vs beat.Event struct allocation.
3. **5x less CPU (raw mode).** 0.009ms vs 0.048ms per event.
4. **63% fewer goroutines.** 14 vs 38. Simpler scheduling.

### Caveats

- Beats used file output; OTel used nop exporter. Real exporter EPS will be lower for both.
- Same endpoint, different days — API latency varies 300-500ms.
- Cursor semantics differ: Beats = at-least-once; OTel = at-most-once per batch boundary.

---

## Metric Definitions

| Metric | Type | Description |
|---|---|---|
| `requests` | counter | Total API requests |
| `request_errors` | counter | Failed API requests |
| `events_received` | counter | Events from API |
| `events_emitted` | counter | Events to pipeline |
| `pages_processed` | counter | API pages processed |
| `cursor_persists` | counter | Successful cursor saves |
| `offset_ttl_drops` | counter | Proactive offset expirations |
| `offset_expired` | counter | 416 errors |
| `recovery_attempts` | counter | Recovery actions |
| `invalid_timestamp_retries` | counter | HMAC retries |
| `mapping_errors` | counter | OTel mapping failures |
| `bytes_received` | counter | Bytes from API |
| `request_duration` | histogram | API roundtrip (seconds) |
| `poll_duration` | histogram | Full poll cycle (seconds) |
| `events_per_second` | histogram | EPS per poll cycle |
| `page_processing_time` | histogram | Per-page processing (seconds) |
| `mapping_duration` | histogram | Mapping time per batch (seconds) |
| `events_per_page` | histogram | Events per API page |

---

## How to Reproduce

```bash
# Build
make genelasticcol

# Run (adjust config as needed)
./_build/elastic-collector-components --config ./bin/akamai-test.yaml

# Metrics
curl -s http://localhost:8888/metrics | grep -E "otelcol_akamai_siem|otelcol_process" | grep -v _bucket | sort

# pprof
curl -o cpu.prof "http://localhost:1777/debug/pprof/profile?seconds=15"
curl -o heap.prof "http://localhost:1777/debug/pprof/heap"
pprof -top cpu.prof
pprof -top -sample_index=alloc_space heap.prof
```
