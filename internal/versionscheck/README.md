# versionscheck

`versionscheck` reads a `module-sets` entry from [`versions.yaml`](../versions.yaml), and compares the version from the module against the highest semver-shaped tag from the repo (e.g. `v1.2.3`, `component/v1.2.3`, or `path/component/v1.2.3`). It **requires** that the YAML version is **strictly greater** than the repo tag. If there are **no** semver tags, it fails unless `-allow-empty-tag` is set.

It is run automatically before [`make push-tags`](../Makefile) (the `push-tags` target depends on `versionscheck`).

## Usage

**From the repository root (recommended):**

```shell
make versionscheck
```

This runs `go run -C internal/versionscheck .` with:

- `-versions` → `<repo-root>/versions.yaml`
- `-github-repo` → `$(VERSIONSCHECK_GITHUB_REPO)` (default: `elastic/opentelemetry-collector-components`)
- `-module-key` → `$(MODSET)` (default: `edot-base`)

Override as needed, for example:

```shell
make versionscheck MODSET=edot-base VERSIONSCHECK_GITHUB_REPO=owner/repo
```

**Manual invocation:**

```shell
go run -C internal/versionscheck . \
  -versions /path/to/versions.yaml \
  -github-repo owner/repo \
  -module-key edot-base
```

Add optional flags (see below) at the end.

## Flags

| Flag | Description |
|------|-------------|
| `-versions` | **Required.** Path to `versions.yaml`. |
| `-github-repo` | **Required.** GitHub repository as `owner/repo` (used with the GitHub API to list tags). |
| `-module-key` | **Required.** Key under `module-sets` in `versions.yaml` (e.g. `edot-base`). |
| `-allow-empty-tag` | If set, do not fail when the repository has no semver-shaped tags; otherwise exit with an error when no such tags exist. |
| `-q` | Quiet: suppress the success message on stdout. |

On failure the process exits non-zero and prints an error (via `log.Fatal`).

