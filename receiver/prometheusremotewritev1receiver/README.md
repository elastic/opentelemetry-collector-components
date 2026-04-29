# Prometheus Remote Write v1 Receiver

This receiver implements the [Prometheus Remote Write 1.0 specification](https://prometheus.io/docs/specs/prw/remote_write_spec/) and exposes an HTTP endpoint that accepts Prometheus remote write data.

| Status      |                                                     |
|-------------|-----------------------------------------------------|
| Stability   | alpha: metrics                                      |
| Pipelines   | metrics                                             |

## Overview

The Prometheus Remote Write protocol (v1) allows Prometheus and compatible agents to send metric data over HTTP in a compact binary format. This receiver accepts those writes and translates them into OpenTelemetry metrics (OTLP).
