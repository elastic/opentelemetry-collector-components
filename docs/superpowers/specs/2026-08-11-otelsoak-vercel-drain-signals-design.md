# otelsoak Vercel drain signals (logs + speed-insights)

## Goal

Make otelsoak Vercel Managed Input soak traffic selectable via env so operators can replay **logs**, **speed-insights**, or **both** against the same drain endpoint—covering [hosted-otel-collector#3428](https://github.com/elastic/hosted-otel-collector/issues/3428) fixture needs without changing `httpexporter`.

## Context

- Vercel drains (logs and Speed Insights) are raw HTTPS NDJSON, not OTLP.
- `httpexporter` POSTs **log record bodies** joined by newlines to a full URL.
- loadgen for OTLP uses separate logs/metrics/traces pipelines because those are real OTel signals. Vercel drain types are **payload variants on one HTTP path**, so they stay on a single loadgen **logs** → `http` pipeline.
- Speed Insights schema: https://vercel.com/docs/drains/reference/speed-insights (`schema: vercel.speed_insights.v1`).

## Decisions

| Topic | Choice |
| --- | --- |
| Selection UX | Env var `VERCEL_SIGNAL` |
| Values | `logs` \| `speed_insights` \| `both` |
| Default | `logs` (current behavior) |
| Endpoint | Same for all: `${ELASTIC_SERVER_URL}/inputs/vercel/_default_` |
| `both` semantics | `make otelsoak-run-vercel` concatenates `logs.jsonl` + `speed_insights.jsonl` into a temp file |
| Implementation | Env-expanded `jsonl_file` for `logs`/`speed_insights`; Makefile temp concat for `both` |

## Design

### Fixtures

Directory: `loadgen/cmd/otelsoak/testdata/vercel/`

| File | Contents |
| --- | --- |
| `logs.jsonl` | Existing Vercel log drain samples (OTLP-JSONL; each body is one drain NDJSON line) |
| `speed_insights.jsonl` | Same OTLP-JSONL wrapping; bodies are Speed Insights NDJSON objects (`vercel.speed_insights.v1`) with a few metric types (e.g. LCP, CLS, INP) |

There is no checked-in `both.jsonl`. `VERCEL_SIGNAL=both` is handled by `make otelsoak-run-vercel`, which cats the two fixtures into a temp path and passes it via `--set`.

Each loadgen line remains one OTLP log record whose `body.stringValue` is the exact drain NDJSON object string that `httpexporter` will POST.

### Config

`loadgen/cmd/otelsoak/config.vercel.example.yaml`:

- `receivers.loadgen.logs.jsonl_file: loadgen/cmd/otelsoak/testdata/vercel/${env:VERCEL_SIGNAL:-logs}.jsonl`
- Unchanged: concurrency, ratelimit, `http` exporter endpoint/headers/TLS, single logs pipeline

Invalid `VERCEL_SIGNAL` values fail when loadgen cannot open the file; document allowed values in README/Makefile.

### Usage

```bash
make genelasticcol
VERCEL_SIGNAL=logs|speed_insights|both \
ELASTIC_SERVER_URL=https://<deployment>.ingest.<region>.<csp>.qa.elastic.cloud \
ELASTIC_APM_API_KEY=<encoded-api-key> \
./loadgen/cmd/otelsoak/otelsoak --config ./loadgen/cmd/otelsoak/config.vercel.example.yaml
```

`make otelsoak-run-vercel` continues to work; callers pass `VERCEL_SIGNAL` in the environment.

### Docs

- Update `loadgen/cmd/otelsoak/README.md` Vercel section for `VERCEL_SIGNAL` and new fixtures.
- Brief note in Makefile `otelsoak-run-vercel` comment and `internal/exporter/httpexporter/README.md` sample pointer if it names only `logs.jsonl`.

## Out of scope

- `httpexporter` API/signal changes (no metrics exporter path).
- Separate Managed Input URLs per drain type.
- Parallel pipelines or independent rates for logs vs speed-insights.
- Encoding/parsing work inside hosted-otel-collector (issue 3428 server-side); this repo only supplies soak fixtures and config.

## Success criteria

1. `VERCEL_SIGNAL=logs` (or unset) behaves as today’s vercel soak.
2. `VERCEL_SIGNAL=speed_insights` POSTs Speed Insights NDJSON to the same endpoint.
3. `make otelsoak-run-vercel VERCEL_SIGNAL=both` POSTs both payload kinds from a temp concat of the two fixtures.
4. Docs show the three values and the env-selected path pattern.
