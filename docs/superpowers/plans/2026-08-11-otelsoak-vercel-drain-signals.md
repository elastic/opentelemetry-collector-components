# otelsoak Vercel drain signals Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let otelsoak select Vercel drain payload type via `VERCEL_SIGNAL` (`logs` | `speed_insights` | `both`) using env-selected fixture files on one HTTP pipeline.

**Architecture:** Keep a single loadgen logs → httpexporter pipeline. Each fixture line is OTLP-JSONL whose log body is one drain NDJSON object. `jsonl_file` uses `${env:VERCEL_SIGNAL:-logs}` so the same config covers all three modes.

**Tech Stack:** OTel Collector config YAML, loadgenreceiver jsonl fixtures, httpexporter (unchanged).

## Global Constraints

- Same Managed Input URL for all signals: `${env:ELASTIC_SERVER_URL}/inputs/vercel/_default_`
- No httpexporter API changes; Speed Insights rides log bodies
- Default `VERCEL_SIGNAL` is `logs`
- Spec: `docs/superpowers/specs/2026-08-11-otelsoak-vercel-drain-signals-design.md`

## File map

| File | Responsibility |
| --- | --- |
| `loadgen/cmd/otelsoak/testdata/vercel/speed_insights.jsonl` | Speed Insights drain samples |
| `loadgen/cmd/otelsoak/testdata/vercel/logs.jsonl` | Unchanged (existing) |
| `loadgen/cmd/otelsoak/config.vercel.example.yaml` | Env-selected `jsonl_file` + usage comment |
| `loadgen/cmd/otelsoak/README.md` | Document `VERCEL_SIGNAL` |
| `Makefile` | `otelsoak-run-vercel`: temp concat when `VERCEL_SIGNAL=both` |
| `internal/exporter/httpexporter/README.md` | Point at signal selection |

Note: no checked-in `both.jsonl`; `both` is Makefile-only (cat + `--set`).

---

### Task 1: Speed Insights + both fixtures

**Files:**
- Create: `loadgen/cmd/otelsoak/testdata/vercel/speed_insights.jsonl`
- Create: `loadgen/cmd/otelsoak/testdata/vercel/both.jsonl`
- Keep: `loadgen/cmd/otelsoak/testdata/vercel/logs.jsonl`

**Interfaces:**
- Consumes: existing `logs.jsonl` line shape (OTLP `resourceLogs` → `body.stringValue` = drain JSON string)
- Produces: `speed_insights.jsonl` and `both.jsonl` with same wrapping; SI bodies use `schema: vercel.speed_insights.v1`

- [x] **Step 1: Create `speed_insights.jsonl`**

Two OTLP-JSONL lines. Inner bodies (escaped inside `stringValue`) must be valid Speed Insights objects, e.g.:

```json
{"schema":"vercel.speed_insights.v1","timestamp":"2026-08-11T10:00:00.000Z","projectId":"soak_si_project","ownerId":"team_soak","deviceId":1001,"metricType":"LCP","value":2.4,"origin":"https://soak.vercel.app","path":"/","route":"/","country":"US","region":"CA","city":"San Francisco","osName":"macOS","clientName":"Chrome","clientType":"browser","deviceType":"desktop","vercelEnvironment":"production","vercelUrl":"soak.vercel.app","deploymentId":"dpl_soak","sdkName":"@vercel/speed-insights","sdkVersion":"1.0.0"}
```

and a second line with `metricType":"CLS"` (and optionally a third with `INP`). Wrap each as:

```json
{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"body":{"stringValue":"<escaped-ndjson>"},"timeUnixNano":"1786443432624000000"}]}]}]}
```

- [x] **Step 2: Create `both.jsonl`**

Concatenate all lines from `logs.jsonl` then all lines from `speed_insights.jsonl` (or interleave 1:1). No other transforms.

- [x] **Step 3: Sanity-check JSON**

Run:

```bash
python3 -c "
import json
from pathlib import Path
base = Path('loadgen/cmd/otelsoak/testdata/vercel')
for name in ('logs.jsonl','speed_insights.jsonl','both.jsonl'):
    for i, line in enumerate(base.joinpath(name).read_text().splitlines(), 1):
        if not line.strip():
            continue
        outer = json.loads(line)
        body = outer['resourceLogs'][0]['scopeLogs'][0]['logRecords'][0]['body']['stringValue']
        inner = json.loads(body)
        print(f'{name}:{i} ok keys={sorted(inner)[:5]}...')
"
```

Expected: each line prints `ok` with no JSON decode errors. For SI files, inner must include `"schema": "vercel.speed_insights.v1"`.

- [x] **Step 4: Commit** (only if user asked to commit)

```bash
git add loadgen/cmd/otelsoak/testdata/vercel/speed_insights.jsonl loadgen/cmd/otelsoak/testdata/vercel/both.jsonl
git commit -m "$(cat <<'EOF'
Add Vercel speed-insights and both soak fixtures for otelsoak.

EOF
)"
```

---

### Task 2: Config + docs for `VERCEL_SIGNAL`

**Files:**
- Modify: `loadgen/cmd/otelsoak/config.vercel.example.yaml`
- Modify: `loadgen/cmd/otelsoak/README.md`
- Modify: `Makefile` (otelsoak-run-vercel comment)
- Modify: `internal/exporter/httpexporter/README.md`

**Interfaces:**
- Consumes: fixture filenames from Task 1 (`logs` | `speed_insights` | `both`)
- Produces: config path `.../testdata/vercel/${env:VERCEL_SIGNAL:-logs}.jsonl`

- [x] **Step 1: Update config header + `jsonl_file`**

In `config.vercel.example.yaml`:

- Document `VERCEL_SIGNAL=logs|speed_insights|both` in the usage comment block
- Set:

```yaml
      jsonl_file: loadgen/cmd/otelsoak/testdata/vercel/${env:VERCEL_SIGNAL:-logs}.jsonl
```

Leave endpoint and pipeline unchanged.

- [x] **Step 2: Update README Vercel section**

Document:

```bash
VERCEL_SIGNAL=speed_insights \
ELASTIC_SERVER_URL=... ELASTIC_APM_API_KEY=... \
./loadgen/cmd/otelsoak/otelsoak --config ./loadgen/cmd/otelsoak/config.vercel.example.yaml
```

List allowed values and that unset defaults to `logs`. Link fixtures directory.

- [x] **Step 3: Touch Makefile + httpexporter README**

- Makefile comment above `otelsoak-run-vercel`: mention `VERCEL_SIGNAL`.
- httpexporter README: say fixtures are selected via `VERCEL_SIGNAL` / point at vercel testdata dir.

- [x] **Step 4: Validate config expands**

Run (after `make genelasticcol` if binary missing):

```bash
VERCEL_SIGNAL=speed_insights ELASTIC_SERVER_URL=http://localhost:8200 ELASTIC_APM_API_KEY=foobar \
  ./loadgen/cmd/otelsoak/otelsoak validate --config ./loadgen/cmd/otelsoak/config.vercel.example.yaml
```

Expected: exit 0, no “file not found” / config error.

Repeat with `VERCEL_SIGNAL=both` and with `VERCEL_SIGNAL` unset (defaults to logs).

- [x] **Step 5: Commit** (only if user asked to commit)

```bash
git add loadgen/cmd/otelsoak/config.vercel.example.yaml loadgen/cmd/otelsoak/README.md Makefile internal/exporter/httpexporter/README.md
git commit -m "$(cat <<'EOF'
Select Vercel soak fixtures with VERCEL_SIGNAL env.

EOF
)"
```

---

## Spec coverage check

| Spec requirement | Task |
| --- | --- |
| `speed_insights.jsonl` fixture | Task 1 |
| `both.jsonl` interleaved/combined | Task 1 |
| Env-selected `jsonl_file` with default `logs` | Task 2 |
| Same endpoint | Task 2 (unchanged) |
| README / Makefile docs | Task 2 |
| No httpexporter API change | N/A (explicit non-change) |
