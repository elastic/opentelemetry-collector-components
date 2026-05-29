# kubetunnel relay

The public rendezvous service for [remote kubectl-read access](../internal/kubetunnel/README.md).
A standalone Go binary (no collector runtime) that:

- accepts persistent outbound gRPC tunnels from in-cluster **probes**
  ([`kubetunnelprobe` extension](../extension/kubetunnelprobeextension)) and keeps
  an in-memory registry of them, and
- exposes an HTTP API the external AI agent uses to discover probes and run
  read-only commands against a chosen one.

## Run

```sh
go run ./cmd/relay --grpc-addr :4317 --http-addr :8080
# or build:
go build -o relay ./cmd/relay
```

| Flag | Default | Description |
|------|---------|-------------|
| `--grpc-addr` | `:4317` | gRPC tunnel endpoint probes dial. |
| `--http-addr` | `:8080` | Agent-facing HTTP API. |
| `--request-timeout` | `30s` | How long to wait for a probe to answer a read. |
| `--auth-tokens` | env `RELAY_AUTH_TOKENS` | Comma-separated accepted bearer tokens; empty disables auth. |
| `--tls-cert` / `--tls-key` | — | Optional TLS for both gRPC and HTTP. |

## HTTP API

```sh
# 1. Discover connected probes (the AI agent picks a target):
curl -s localhost:8080/targets | jq

# 2. Run a read against a chosen probe:
curl -s localhost:8080/targets/<probe_id> \
  -d '{"operation":"list","resource":"pods","namespace":"kube-system"}' | jq

# logs:
curl -s localhost:8080/targets/<probe_id> \
  -d '{"operation":"logs","namespace":"kube-system","name":"<pod>","tailLines":100}'
```

Responses: `200` with a `ReadResult` (`{"json": ...}` or `{"error": ...}`), `404`
unknown probe, `503` probe disconnected, `504` probe did not answer in time.

## Docker

```sh
# build context is the repo root (the binary's module has a replace directive):
docker build -t kubetunnelrelay:poc -f kubetunnelrelay/Dockerfile .
```

See [`examples/kubetunnel`](../examples/kubetunnel) for the full minikube walkthrough.
