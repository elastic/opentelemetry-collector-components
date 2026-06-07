# Kubernetes tunnel probe extension

| Status | |
|--------|--|
| Stability | development |
| Class | extension |

The in-cluster half of the [remote kubectl-read access](../../internal/kubetunnel/README.md)
proof-of-concept. This extension turns an EDOT collector into a **probe**: it dials
*out* to a public [relay](../../kubetunnelrelay) and holds a bidirectional gRPC
tunnel open, then serves **read-only** Kubernetes queries (`get` / `list` / `logs`)
that arrive down the tunnel using `client-go` against the cluster API.

Because the collector dials out, no inbound connectivity to the cluster is needed.
The external AI investigation agent talks only to the relay's HTTP API.

## Configuration

```yaml
extensions:
  kubetunnelprobe:
    relay:
      endpoint: relay.example.com:4317
      tls:
        insecure: true            # PoC only — terminate/verify TLS in production
    auth_token: ${env:RELAY_TOKEN} # stubbed bearer token presented on registration
    cluster_id: prod-eu-west-1
    # Discovery metadata — wire from the Downward API in Kubernetes:
    node_name: ${env:NODE_NAME}
    pod_name: ${env:POD_NAME}
    namespace: ${env:POD_NAMESPACE}
    reconnect_backoff: 5s
    # kubeconfig: ""               # empty = in-cluster config
    # allowed_namespaces: []       # optional read guardrail on top of RBAC

service:
  extensions: [kubetunnelprobe]
```

| Setting | Description |
|---------|-------------|
| `relay` | Outbound gRPC client config (`configgrpc.ClientConfig`). `relay.endpoint` is required. |
| `auth_token` | Bearer token sent in the registration frame (stubbed auth). |
| `cluster_id` | Logical cluster identity reported as discovery metadata. |
| `probe_id` | Unique id; defaults to `<namespace>/<pod_name>`, then hostname. |
| `node_name`, `pod_name`, `namespace` | Discovery metadata, typically from the Downward API. |
| `reconnect_backoff` | Delay before redialing the relay after the tunnel drops (default `5s`). |
| `kubeconfig` | Path to a kubeconfig; empty uses in-cluster config. |
| `allowed_namespaces` | If set, reads are restricted to these namespaces. |

## Read-only guarantee

The executor implements only `get`, `list`, and `logs`; no create/update/delete/
patch path exists. Pair it with a read-only `ClusterRole` (see
[`examples/kubetunnel/rbac.yaml`](../../examples/kubetunnel/rbac.yaml)) for defense
in depth.
