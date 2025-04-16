# Releasing opentelemetry-collector-components

0. Clone the `https://github.com/open-telemetry/opentelemetry-collector`
   repository (or ensure is up-to-date) one directory above the components
repository (../../opentelemetry-collector).

1. Open a PR to the repository to use the newly released OpenTelemetry Collector Core version by doing the following:
   - Manually update core and contrib collector module versions in
   `../distributions/elastic-components/manifest.yaml`
   - Run `make genelasticcol`
   - Commit the changes
   - Run `make update-otel OTEL_VERSION=v0.85.0 OTEL_STABLE_VERSION=v1.1.0`
     - If there is no new stable version released in core collector, use the current stable module version in contrib as `OTEL_STABLE_VERSION`.
   - If you were unable to run `make update-otel` before releasing core, fix any errors from breaking changes.
   - Commit the changes
   - Open the PR
   -  🛑 **Do not move forward until this PR is merged.** 🛑
