# Remote kubectl-read access — minikube walkthrough

End-to-end proof of concept: an external "AI agent" (here, `curl`) discovers
in-cluster probes through a relay and runs **read-only** Kubernetes queries against
a chosen one — without any inbound connectivity to the cluster.

```
curl ──HTTP──► relay ──gRPC tunnel──► probe (EDOT collector) ──client-go──► kube API
            (external/host)          (in cluster, dials OUT to the relay)
```

The **relay runs outside the cluster** — it is a shared, multi-tenant service, so
it is never deployed in-cluster. Only the **probe** (an EDOT collector) runs inside
Kubernetes and dials out. For this minikube demo the relay runs as a binary on the
host and the probe reaches it via `host.minikube.internal`.

See [`../../internal/kubetunnel/README.md`](../../internal/kubetunnel/README.md) for
the architecture and [the probe extension](../../extension/kubetunnelprobeextension)
/ [the relay](../../kubetunnelrelay) for the components.

## Prerequisites

- `minikube` running with the docker driver: `minikube start`
- Docker, Go 1.25+, and (to regenerate protobuf) `protoc`.

## 1. Build & load the EDOT image (the probe)

```sh
# From the repo root. minikube here is linux/arm64; adjust GOARCH for your node.
rm -f _build/elastic-collector-components
TARGET_GOOS=linux TARGET_GOARCH=arm64 make genelasticcol
docker build --platform linux/arm64 -t elastic-collector-components:poc \
  -f distributions/elastic-components/Dockerfile .
minikube image load elastic-collector-components:poc
```

## 2. Run the relay on the host (outside the cluster)

```sh
go build -o /tmp/kubetunnel-relay ./kubetunnelrelay/cmd/relay
# 7317/7080 instead of 4317/8080 to avoid a common local OTLP port conflict:
/tmp/kubetunnel-relay --grpc-addr 0.0.0.0:7317 --http-addr 0.0.0.0:7080 &
```

The relay must bind `0.0.0.0` so the cluster can reach it via
`host.minikube.internal`. (In production the relay is deployed as the
`kubetunnelrelay:poc` container behind a public address.)

## 3. Deploy the probe (read-only RBAC + collector)

```sh
kubectl apply -f examples/kubetunnel/namespace.yaml
kubectl apply -f examples/kubetunnel/rbac.yaml
kubectl apply -f examples/kubetunnel/collector.yaml   # RELAY_ENDPOINT=host.minikube.internal:7317

kubectl -n kubetunnel rollout status deploy/kubetunnel-probe
# The probe logs "registered with relay" once the tunnel is up:
kubectl -n kubetunnel logs deploy/kubetunnel-probe | grep -i "registered with relay"
```

## 4. Discover, then read (talking to the relay on `localhost:7080`)

```sh
# Discover connected probes (the agent picks a target):
curl -s localhost:7080/targets | jq
# -> [{"probeId":"kubetunnel/kubetunnel-probe-...","clusterId":"minikube",
#      "nodeName":"minikube","k8sVersion":"v1.xx.x", ...}]

PROBE=$(curl -s localhost:7080/targets | jq -r '.[0].probeId')

# List pods in kube-system (.json is the real kubectl-style JSON, embedded):
curl -s "localhost:7080/targets/$PROBE" \
  -d '{"operation":"list","resource":"pods","namespace":"kube-system"}' \
  | jq -r '.json.items[].metadata.name'

# Get one deployment:
curl -s "localhost:7080/targets/$PROBE" \
  -d '{"operation":"get","resource":"deployments.apps","namespace":"kube-system","name":"coredns"}' \
  | jq '.json.status'

# Pod logs (.json is a JSON string of the log text):
POD=$(curl -s "localhost:7080/targets/$PROBE" \
  -d '{"operation":"list","resource":"pods","namespace":"kube-system"}' \
  | jq -r '.json.items[0].metadata.name')
curl -s "localhost:7080/targets/$PROBE" \
  -d "{\"operation\":\"logs\",\"namespace\":\"kube-system\",\"name\":\"$POD\",\"tailLines\":20}" \
  | jq -r '.json'

# Read-only is enforced — a write verb is rejected without touching the API:
curl -s "localhost:7080/targets/$PROBE" \
  -d '{"operation":"delete","resource":"pods","namespace":"kube-system","name":"x"}' | jq
# -> {"error":"operation \"delete\" is not allowed (read-only: get/list/logs)"}
```

## Notes

- **Auth is stubbed.** To require a shared token: start the relay with
  `--auth-tokens=secret`, set `RELAY_TOKEN=secret` on the probe, and send
  `-H "Authorization: Bearer secret"` on the `curl` calls.
- **Many probes:** scale `kubetunnel-probe` (or deploy more collectors with
  distinct `CLUSTER_ID`) and they all appear under `GET /targets`; filter with
  `?cluster_id=` / `?node_name=`.
- **Cleanup:** `kubectl delete ns kubetunnel`.
