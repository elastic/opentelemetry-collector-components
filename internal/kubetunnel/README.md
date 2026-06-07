# Remote ad-hoc kubectl-read access (relay tunnel)

This is a proof-of-concept that lets an investigation agent running **outside** a
customer's Kubernetes cluster run **read-only** queries (`get` / `list` / `logs`)
against that cluster with low latency — even though the cluster exposes no inbound
network path.

It uses a **reverse-tunnel / rendezvous** pattern across three pieces:

| Piece | Where it runs | What it is |
|-------|---------------|------------|
| **Relay** | public internet | standalone Go binary ([`../../kubetunnelrelay`](../../kubetunnelrelay)) |
| **Probe extension** | inside the EDOT collector pod, in the cluster | collector extension ([`../../extension/kubetunnelprobeextension`](../../extension/kubetunnelprobeextension)) |
| **Shared transport** | both | this module — gRPC contract + command schema + relay registry |

Naming: the **probe** is the in-cluster collector component (read-only,
investigative — hence "probe"). The relay exposes connected probes as discoverable
**targets**. "Agent" is reserved for the external AI investigation agent, to avoid
the obvious collision.

```
                  1) GET /targets            → connected probes + metadata
                  2) POST /targets/{id}     → buffered JSON response
  ┌─────────────┐                              ┌─────────────────────┐
  │ AI invest-  │ ───────── HTTP ────────────► │  Relay (public)      │
  │ igation     │ ◄──────────────────────────  │  gRPC + HTTP +       │
  │ agent       │                              │  probe registry      │
  └─────────────┘                              └───▲──────▲──────▲────┘
                                                   │      │      │  many persistent
                       collectors dial OUT & hold ─┘      │      └─── gRPC bidi tunnels
                                              ┌───────────┴┐  ┌──┴─────────┐
                                              │ Probe ext  │  │ Probe ext  │  ...
                                              │ (in pod)   │  │ (in pod)   │
                                              └─────┬──────┘  └─────┬──────┘
                                                    │ client-go get/list/logs
                                              ┌─────▼───────────────────────┐
                                              │      Kube API server(s)      │
                                              └──────────────────────────────┘
```

## Why a relay?

The in-cluster collector almost never accepts inbound connections (NAT, firewalls,
private networking). So the collector **dials out** to the relay and holds a
bidirectional gRPC stream open. The relay parks that stream and, when the agent
makes a plain HTTP request, forwards the command down the matching stream and
returns the answer as the HTTP response. The relay guarantees the request can
*enter* the otherwise-unreachable cluster and that the caller gets an answer.

## Contract

The gRPC service (`proto/tunnel.proto`) is a deliberately thin transport: the
collector calls `Connect` and exchanges `ClientFrame` / `ServerFrame` envelopes.
The actual command schema travels as JSON bytes inside those frames so it can
evolve without regenerating protobuf:

- `ReadRequest` — `operation` (`get`/`list`/`logs`), `resource`, `namespace`,
  `name`, selectors, `container`, `tailLines`.
- `ReadResult` — `json` payload or `error`.
- `ProbeInfo` — what `GET /targets` returns: `probeId`, `clusterId`, `nodeName`,
  `podName`, `namespace`, `collectorVersion`, `k8sVersion`, `labels`.

The first `ClientFrame` a collector sends carries a `Register` message with its
identifying metadata; the relay stores it in an in-memory `Registry` (keyed by
`probeId`) and surfaces it via the discovery endpoint.

## Relay HTTP API

- `GET /targets` — list connected probes and their metadata (optional
  `?cluster_id=` / `?node_name=` filters). The AI agent uses this to pick a target.
- `POST /targets/{probe_id}` — body is a `ReadRequest`; response is a
  `ReadResult`. `404` if the probe id is unknown, `503` if it has disconnected,
  `504` if the collector did not answer within the relay's request timeout.

## Read-only by construction

The in-cluster executor only implements `get` / `list` / `logs`. There is no
create/update/delete/patch code path at all. As defense in depth the collector
runs under a read-only `ClusterRole` (`get`/`list`/`watch`) — see
[`../../examples/kubetunnel/rbac.yaml`](../../examples/kubetunnel/rbac.yaml).

> **Auth is stubbed** for the PoC: a single static bearer token on both legs,
> structured so it can be replaced with real authentication later.

## Regenerating the protobuf stubs

The repo has no protobuf pipeline, so generation is scoped to this module. With
`protoc`, `protoc-gen-go` and `protoc-gen-go-grpc` on `PATH`:

```sh
cd internal/kubetunnel
protoc --proto_path=proto \
  --go_out=. --go_opt=module=github.com/elastic/opentelemetry-collector-components/internal/kubetunnel \
  --go-grpc_out=. --go-grpc_opt=module=github.com/elastic/opentelemetry-collector-components/internal/kubetunnel \
  proto/tunnel.proto
```

(Install the plugins with `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`
and `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`.) The
generated `tunnelpb/*.pb.go` files are committed.

## Trying it out

See [`../../examples/kubetunnel/README.md`](../../examples/kubetunnel/README.md) for
an end-to-end minikube walkthrough (build the EDOT image + relay, deploy with
read-only RBAC, then discover-then-read with `curl`).
