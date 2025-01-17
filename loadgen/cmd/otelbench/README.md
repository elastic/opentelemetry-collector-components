# otelbench

otelbench wraps the collector inside a Go test benchmark. It outputs throughput numbers in standard Go benchmark format. It uses a collector config yaml as a base, and applies some overrides from command line options.

## Usage

```
Usage of ./otelbench:
  -api-key string
        API key for target server
  -config string
        path to collector config yaml (default "config.yaml")
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
  -header value
        extra headers in key=value format when sending data to the server. Can be repeated. e.g. -header X-FIRST-HEADER=foo -header X-SECOND-HEADER=bar
  -insecure
        disable TLS, ignored by otlphttp exporter (default to value in config yaml)
  -insecure-skip-verify
        skip validating the remote server TLS certificates (default to value in config yaml)
  -logs
        benchmark logs (default true)
  -metrics
        benchmark metrics (default true)
  -secret-token string
        secret token for target server
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
```

## Example usage

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
./otelbench -config=./config.yaml -endpoint-otlp=localhost:4317 -endpoint-otlphttp=https://localhost:4318/prefix -api-key some_api_key
```

It is possible to run with a customized config to avoid passing in command line options every time

```shell
./otelbench -config=./my-custom-config.yaml
```
