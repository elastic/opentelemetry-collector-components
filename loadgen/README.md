# loadgen: OTel load generation tooling

In `cmd/` directory, there are
- [otelsoak](./otelsoak/README.md)
    - Load generator that is exactly an OTel collector. It sends load to an endpoint and never terminates.
    - Suitable for soak testing
- [otelbench](./otelbench/README.md)
    - Load generator based on OTel collector and Go benchmarking. It sends load to an endpoint, terminates after a configured duration, and outputs statistics.
    - Suitable for benchmarking

otelsoak and otelbench is synonymous to apmsoak and apmbench in [elastic/apm-perf](https://github.com/elastic/apm-perf).
