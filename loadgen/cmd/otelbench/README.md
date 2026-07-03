# otelbench

otelbench wraps the collector inside a Go test benchmark. It outputs throughput numbers in standard Go benchmark format. It uses a collector config yaml as a base, and applies some overrides from command line options.

## Usage

```
Usage of ./otelbench:
  -api-key string
        API key for target server
  -concurrency list
        comma-separated list of concurrency (number of simulated agents) to run each benchmark with. Supports numeric values (e.g., "1,4,8"), "auto" to use available CPU cores, or "auto:Nx" for multipliers (e.g., "auto:2x" for double, "auto:0.5x" for half)
  -config string
        path to collector config yaml. If empty, the config.yaml embedded in the binary will be used.
  -duration-metrics duration
        optional safety cap for -soak; 0 means run until the collector exits on its own (e.g. via metricsgen exit_after_end)
  -endpoint value
        target server endpoint for both otlp and otlphttp exporters (default to value in config yaml), equivalent to setting both -endpoint-otlp and -endpoint-otlphttp
  -endpoint-otlp value
        target server endpoint for otlp exporter (default to value in config yaml)
  -endpoint-otlphttp value
        target server endpoint for otlphttp exporter (default to value in config yaml)
  -exporter-otlp
        benchmark exporter otlp (default true)
  -exporter-otlphttp
        benchmark exporter otlphttp (default true)
  -exporter-elasticsearch
        benchmark exporter elasticsearch
  -header value
        extra headers in key=value format when sending data to the server. Can be repeated. e.g. -header X-FIRST-HEADER=foo -header X-SECOND-HEADER=bar
  -insecure
        disable TLS, ignored by otlphttp exporter (default to value in config yaml)
  -insecure-skip-verify
        skip validating the remote server TLS certificates (default to value in config yaml)
  -logs
        benchmark logs (default true)
  -logs-data-path string
        path to logs data file (e.g. logs.json). If empty, embedded data will be used.
  -metrics
        benchmark metrics (default true)
  -metrics-data-path string
        path to metrics data file (e.g. metrics.json). If empty, embedded data will be used.
  -metrics-telemetry-endpoint string
        collector self-telemetry Prometheus host to scrape for -soak benchmark output; empty disables it (default "127.0.0.1")
  -profiles
        benchmark profiles (default false)
  -profiles-data-path string
        path to profiles data file (e.g. profiles.json). If empty, embedded data will be used.
  -mixed
        benchmark mixed signals, i.e. logs, metrics, traces and profiles at the same time (default true)
  -secret-token string
        secret token for target server
  -shuffle
        shuffle the order of benchmarks. This is useful for concurrent runs.
  -soak
        run as a collector soak test (plain collector run reading -config) instead of the benchmark harness
  -telemetry-elasticsearch-api-key string
        optional remote Elasticsearch telemetry API key
  -telemetry-elasticsearch-index string
        optional remote Elasticsearch telemetry metrics index pattern (default "metrics-*")
  -telemetry-elasticsearch-password string
        optional remote Elasticsearch telemetry password
  -telemetry-elasticsearch-timeout duration
        optional remote Elasticsearch telemetry request timeout (default 1m0s)
  -telemetry-elasticsearch-url list
        optional comma-separated list of remote Elasticsearch telemetry hosts
  -telemetry-elasticsearch-username string
        optional remote Elasticsearch telemetry username
  -telemetry-filter-cluster-name string
        optional remote Elasticsearch telemetry cluster name metrics filter
  -telemetry-filter-project-id string
        optional remote Elasticsearch telemetry project id metrics filter
  -telemetry-metrics list
        optional comma-separated list of remote Elasticsearch telemetry metrics to be reported (default otelcol_process_cpu_seconds,otelcol_process_memory_rss,otelcol_process_runtime_total_alloc_bytes,otelcol_process_runtime_total_sys_memory_bytes,otelcol_process_uptime)
  -test.bench regexp
        run only benchmarks matching regexp
  -test.benchmem
        print memory allocations for benchmarks
  -test.benchtime d
        run each benchmark for duration d or N times if `d` is of the form Nx (default 1s)
  -test.blockprofile file
        write a goroutine blocking profile to file
  -test.blockprofilerate rate
        set blocking profile rate (see runtime.SetBlockProfileRate) (default 1)
  -test.count n
        run tests and benchmarks n times (default 1)
  -test.coverprofile file
        write a coverage profile to file
  -test.cpu list
        comma-separated list of cpu counts to run each test with
  -test.cpuprofile file
        write a cpu profile to file
  -test.failfast
        do not start new tests after the first test failure
  -test.fullpath
        show full file names in error messages
  -test.fuzz regexp
        run the fuzz test matching regexp
  -test.fuzzcachedir string
        directory where interesting fuzzing inputs are stored (for use only by cmd/go)
  -test.fuzzminimizetime value
        time to spend minimizing a value after finding a failing input (default 1m0s)
  -test.fuzztime value
        time to spend fuzzing; default is to run indefinitely
  -test.fuzzworker
        coordinate with the parent process to fuzz random values (for use only by cmd/go)
  -test.gocoverdir string
        write coverage intermediate files to this directory
  -test.list regexp
        list tests, examples, and benchmarks matching regexp then exit
  -test.memprofile file
        write an allocation profile to file
  -test.memprofilerate rate
        set memory allocation profiling rate (see runtime.MemProfileRate)
  -test.mutexprofile string
        write a mutex contention profile to the named file after execution
  -test.mutexprofilefraction int
        if >= 0, calls runtime.SetMutexProfileFraction() (default 1)
  -test.outputdir dir
        write profiles to dir
  -test.paniconexit0
        panic on call to os.Exit(0)
  -test.parallel n
        run at most n tests in parallel (default 16)
  -test.run regexp
        run only tests and examples matching regexp
  -test.short
        run smaller test suite to save time
  -test.shuffle string
        randomize the execution order of tests and benchmarks (default "off")
  -test.skip regexp
        do not list or run tests matching regexp
  -test.testlogfile file
        write test action log to file (for use only by cmd/go)
  -test.timeout d
        panic test binary after duration d (default 0, timeout disabled)
  -test.trace file
        write an execution trace to file
  -test.v
        verbose: print additional output
  -traces
        benchmark traces (default true)
  -traces-data-path string
        path to traces data file (e.g. traces.json). If empty, embedded data will be used.
```

## Example usage

**It is recommended to explicitly configure `concurrency` to roughly the number of threads of the system running otelbench. Otherwise, `concurrency` will default to 1, and load generation will be single threaded and will send to target with only 1 connection, resulting in an underestimated throughput.**

To send to a local apm-server

```shell
./otelbench -config=./config.yaml -endpoint=http://localhost:8200 -secret-token=foobar -insecure
```

To send to an ESS apm-server

```shell
./otelbench -test.benchtime=1m -config=./config.yaml -endpoint=https://foobar.apm.europe-west2.gcp.elastic-cloud.com:443 -api-key=some_api_key
```

To send to an OTel collector with a special otlphttp path

```shell
./otelbench -config=./config.yaml -endpoint-otlp=http://localhost:4317 -endpoint-otlphttp=https://localhost:4318/prefix -api-key some_api_key
```

It is possible to run with a customized config to avoid passing in command line options every time

```shell
./otelbench -config=./my-custom-config.yaml
```

Optional remote OTel collector metrics will be reported as bench stats when additional telemetry flags are provided.
Gauge metrics will be aggregated to average, while Counter and Histogram will be aggregated to sum.
For the full list of reported metrics see https://opentelemetry.io/docs/collector/internal-telemetry/#basic-level-metrics.

```shell
./otelbench -config=./config.yaml -endpoint-otlp=localhost:4317 -endpoint-otlphttp=https://localhost:4318/prefix -api-key some_api_key -telemetry-elasticsearch-url=localhost:9200 -telemetry-elasticsearch-api-key telemetry_api_key -telemetry-elasticsearch-index "metrics*" -telemetry-filter-cluster-name cluster_name
```

## Soak Mode

With the use of flag `-soak`, otelbench runs the collector defined by `-config` directly as a load generator. This is intended
for `metricsgen`-based metrics pipelines (e.g. Prometheus Remote Write) and the `loadgenreceiver` is not used.
Read more information for [metricsgenreceiver](https://github.com/elastic/metricsgenreceiver)

The run continues until the collector exits, for example when metricsgen reaches `exit_after_end`. You can interrupt the run manually, or use -duration-metrics as a safety cap to shut the collector down after a fixed time. Use `-duration-metrics` as an optional safety cap (a Go duration like `90s`, `2m`, `1h`); when it elapses, the collector is shut down. Leaving it at `0` (the default) means no cap.

```shell
# Run soak mode with an explicit metricsgen PRW config.
./otelbench -soak -config ./config-prw.yaml

# Add a safety cap.
./otelbench -soak -config ./config-prw.yaml -duration-metrics=120s
```

> Note: if the exporter is configured to retry forever (`retry_on_failure.max_elapsed_time: 0`) together with
> `sending_queue.block_on_overflow: true` and the endpoint is unreachable, shutdown can block draining the queue,
> so even `-duration-metrics` will not free the process. Use a finite `max_elapsed_time` for load tests against
> flaky or local endpoints.

### Benchmark output

If the collector config exposes its own telemetry via a Prometheus pull reader, otelbench scrapes it during the run
and prints one go-benchmark-style line once the run completes, derived from the collector's
`otelcol_exporter_sent_metric_points` / `otelcol_exporter_send_failed_metric_points` counters:

```shell
BenchmarkOTelbench/metricsgen <sent+failed>     <ns/op> ns/op     <duration> duration_s   <sent/s> metric_points/s      <failed/s> failed_metric_points/s

#Example
BenchmarkOTelbench/metricsgen     633440     294036 ns/op       186.3 duration_s     0 failed_metric_points/s        3401 metric_points/s
```

Soak mode reports a single observed collector run, not a repititive Go benchmark loop. The benchmark `N` is the
observed exporter attempt count (`sent + failed`) from collector self-telemetry, and `duration_s` is the active
telemetry window used for the throughput calculation.

The scrape host is controlled by `-metrics-telemetry-endpoint` (default `127.0.0.1`). Otelbench picks a random
available internal telemetry port, prints the selected host:port, and overrides the collector's
`service.telemetry.metrics` pull reader to expose Prometheus telemetry on the selected port. The example
`config-prw.yaml` includes the telemetry reader that otelbench overrides:

```yaml
service:
  telemetry:
    metrics:
      level: basic
      readers:
        - pull:
            exporter:
              prometheus:
                host: '127.0.0.1'
                port: 8889
```

Setting `-metrics-telemetry-endpoint=""` disables benchmark output, keeping the pure soak behavior. Because the
counters are scraped off the data path, enabling telemetry does not change the generated load. `metric_points/s` is
a saturation/ingest throughput (generator-driven), not a per-iteration latency like the default OTLP `loadgen` numbers.

The default host works with the bundled config, so no flag is needed for benchmark output. Pass
`-metrics-telemetry-endpoint` only to override the host, or set it empty to disable benchmark output (pure soak):

```shell
# Benchmark output on (default).
./otelbench -soak -config ./config-prw.yaml

# Disable benchmark output, soak only.
./otelbench -soak -config ./config-prw.yaml -metrics-telemetry-endpoint=""
```

## Example usage with Docker image

### Basic usage

```shell
docker run -it docker.elastic.co/observability-ci/otelbench:v0.10.1 -endpoint-otlp=http://172.17.0.1:4317 -api-key some_api_key -insecure
```

Remember that `localhost` does not work because otelbench runs in a container. Use `172.17.0.1` for Linux and `host.docker.internal` for macOS.

### Advanced usage with custom config file

```shell
docker run -it --volume /path/to/config.yaml:/config.yaml docker.elastic.co/observability-ci/otelbench:v0.10.1 -endpoint-otlp=http://172.17.0.1:4317 -api-key some_api_key -insecure -config=/config.yaml
```

## Contribute

If you want to contribute to any go files, you need to create a changelog entry:

1. Create a changelog entry by running `make chlog-new`.
2. Validate your changelog entry by running `make chlog-validate`.
3. If you want to preview the future `CHANGELOG.md` you can also run `make chlog-preview`.

## Create new release

There are two ways you can trigger a new release:

- Manually, by triggering the GH actions workflow `bump-otelbench`.
- Automatically, by updating the VERSION field in the Makefile.

The `bump-otelbench` workflow will check the new version increased in regards to the previous version.
If it did, then a new PR will be opened, updating the CHANGELOG file and removing
the changelog fragments.

Once this PR has been merged, a new workflow, `release-otelbench` will be triggered. The `otelbench` image
is built and pushed to `docker.elastic.co/observability-ci/otelbench` registry. The new image should have
as a tag the newest `otelbench` version.
