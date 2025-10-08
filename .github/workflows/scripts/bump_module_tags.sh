#!/usr/bin/env bash

set -euo pipefail

# This script reads a YAML file defining a module-set, discovers the latest tag for each module
# in a mono-repo like elastic/opentelemetry-collector-components (tags in the form
#   <module-subpath>/vX.Y.Z), bumps the minor version, pushes the new tag, and creates a GitHub
# release for it.
#
# Requirements:
# - git (with push access to the target repo)
# - gh (GitHub CLI; authenticated with permissions to create releases)
# - yq (https://github.com/mikefarah/yq) for YAML parsing
#
# Usage:
#   bash scripts/bump_module_tags.sh \
#     --yaml path/to/modules.yaml \
#     --repo-path /path/to/local/clone/of/elastic/opentelemetry-collector-components \
#     [--module-set edot-base] \
#     [--bump minor] \
#     [--branch main] \
#     [--dry-run]
#
# Notes:
# - The script expects module entries like:
#     github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver
#   It will treat everything after the owner/repo (elastic/opentelemetry-collector-components)
#   as the module subpath (receiver/elasticapmintakereceiver) and tag using the pattern:
#     receiver/elasticapmintakereceiver/vX.Y.Z
# - By default it bumps the minor version and resets patch to 0 (e.g., v0.9.0 -> v0.10.0).
# - Tags are applied to the current HEAD of the provided --branch in --repo-path.

print_usage() {
  cat <<'USAGE'
Usage: bump_module_tags.sh --yaml <path> --repo-path <path> [--module-set <name>] [--bump minor|patch|major] [--branch <branch>] [--dry-run]

Options:
  --yaml        Path to YAML file containing module-sets
  --repo-path   Local path to the mono-repo clone (e.g., elastic/opentelemetry-collector-components)
  --module-set  Name under module-sets to use (default: edot-base)
  --bump        Which semver part to bump: minor (default), patch, major
  --branch      Branch to tag from (checked out in --repo-path, default: main)
  --dry-run     Print actions without performing git push / gh release
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' not found in PATH" >&2
    exit 1
  fi
}

YAML_PATH=""
REPO_PATH=""
MODULE_SET="edot-base"
BUMP_KIND="minor"
BRANCH_NAME="main"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --yaml)
      YAML_PATH="$2"; shift 2 ;;
    --repo-path)
      REPO_PATH="$2"; shift 2 ;;
    --module-set)
      MODULE_SET="$2"; shift 2 ;;
    --bump)
      BUMP_KIND="$2"; shift 2 ;;
    --branch)
      BRANCH_NAME="$2"; shift 2 ;;
    --dry-run)
      DRY_RUN=true; shift 1 ;;
    -h|--help)
      print_usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      print_usage
      exit 1 ;;
  esac
done

if [[ -z "$YAML_PATH" || -z "$REPO_PATH" ]]; then
  echo "Error: --yaml and --repo-path are required" >&2
  print_usage
  exit 1
fi

require_cmd git
require_cmd gh
require_cmd yq

if [[ ! -f "$YAML_PATH" ]]; then
  echo "Error: YAML file not found at $YAML_PATH" >&2
  exit 1
fi

if [[ ! -d "$REPO_PATH/.git" ]]; then
  echo "Error: --repo-path does not appear to be a git repository: $REPO_PATH" >&2
  exit 1
fi

if [[ "$BUMP_KIND" != "major" && "$BUMP_KIND" != "minor" && "$BUMP_KIND" != "patch" ]]; then
  echo "Error: --bump must be one of: major, minor, patch" >&2
  exit 1
fi

set -x

# Ensure we are up-to-date and on the requested branch
git -C "$REPO_PATH" fetch --tags origin
git -C "$REPO_PATH" fetch origin "$BRANCH_NAME":"$BRANCH_NAME" || true
git -C "$REPO_PATH" checkout "$BRANCH_NAME"
git -C "$REPO_PATH" pull --ff-only origin "$BRANCH_NAME" || true

set +x

# Extract modules for the selected module-set
echo "Reading modules from $YAML_PATH (module-set: $MODULE_SET)"
mapfile -t MODULES < <(yq -r ".\"module-sets\".\"$MODULE_SET\".modules[]" "$YAML_PATH")

if [[ ${#MODULES[@]} -eq 0 ]]; then
  echo "Error: No modules found under module-sets.$MODULE_SET.modules in $YAML_PATH" >&2
  exit 1
fi

# Bump function: given X.Y.Z and bump kind, output new X.Y.Z
bump_version() {
  local version="$1" kind="$2"
  local major minor patch
  IFS='.' read -r major minor patch <<<"$version"
  case "$kind" in
    major)
      major=$((major+1)); minor=0; patch=0 ;;
    minor)
      minor=$((minor+1)); patch=0 ;;
    patch)
      patch=$((patch+1)) ;;
  esac
  echo "$major.$minor.$patch"
}

# Discover latest tag for a prefix and compute the next version
discover_next_tag() {
  local repo_path="$1" tag_prefix="$2" bump_kind="$3"
  local latest_tag latest_version new_version

  # List local tags for the prefix
  mapfile -t tags < <(git -C "$repo_path" tag -l "$tag_prefix/v*")
  if [[ ${#tags[@]} -eq 0 ]]; then
    # Try fetching tags explicitly if none were present locally
    git -C "$repo_path" fetch --tags origin >/dev/null 2>&1 || true
    mapfile -t tags < <(git -C "$repo_path" tag -l "$tag_prefix/v*")
  fi

  if [[ ${#tags[@]} -eq 0 ]]; then
    # Start from v0.1.0 for minor bump base v0.0.0 if nothing exists
    case "$bump_kind" in
      major) new_version="1.0.0" ;;
      minor) new_version="0.1.0" ;;
      patch) new_version="0.0.1" ;;
    esac
    echo "$tag_prefix/v$new_version"
    return 0
  fi

  # Extract the version, sort semver-wise, and take the highest
  latest_version=$(printf '%s\n' "${tags[@]}" \
    | sed -E "s#^${tag_prefix}/v##" \
    | sort -V \
    | tail -n1)

  new_version=$(bump_version "$latest_version" "$bump_kind")
  echo "$tag_prefix/v$new_version"
}

create_tag_and_release() {
  local repo_path="$1" owner_repo="$2" branch="$3" module_subpath="$4" bump_kind="$5" dry_run="$6"

  local prefix new_tag sha title
  prefix="$module_subpath"

  new_tag=$(discover_next_tag "$repo_path" "$prefix" "$bump_kind")

  # Tag at the current tip of the branch
  sha=$(git -C "$repo_path" rev-parse "$branch")
  title="$new_tag"

  echo "Will create tag $new_tag at $owner_repo@$branch ($sha)"

  if [[ "$dry_run" == true ]]; then
    echo "[DRY-RUN] git tag -a '$new_tag' -m 'Release $new_tag' -- $(printf '%.7s' "$sha")"
    echo "[DRY-RUN] git push origin '$new_tag'"
    echo "[DRY-RUN] gh release create '$new_tag' -t '$title' -n 'Automated release for $module_subpath' -R '$owner_repo'"
    return 0
  fi

  # Create annotated tag
  GIT_COMMITTER_DATE="$(date -R)" GIT_AUTHOR_DATE="$(date -R)" \
    git -C "$repo_path" tag -a "$new_tag" -m "Release $new_tag" "$sha"

#  git -C "$repo_path" push origin "$new_tag"

  # Create GitHub release
#  gh release create "$new_tag" \
#    --title "$title" \
#    --notes "Automated release for $module_subpath" \
#    -R "$owner_repo"
}

# Iterate modules
for module in "${MODULES[@]}"; do
  # Expect format github.com/<owner>/<repo>/<subpath...>
  if [[ ! "$module" =~ ^github\.com/([^/]+)/([^/]+)/(.*)$ ]]; then
    echo "Skipping unrecognized module path: $module" >&2
    continue
  fi
  owner="${BASH_REMATCH[1]}"
  repo="${BASH_REMATCH[2]}"
  subpath="${BASH_REMATCH[3]}"
  owner_repo="$owner/$repo"

  echo "Processing module: $module"
  echo "  Repo: $owner_repo"
  echo "  Subpath: $subpath"

  create_tag_and_release "$REPO_PATH" "$owner_repo" "$BRANCH_NAME" "$subpath" "$BUMP_KIND" "$DRY_RUN"
done

echo "Done."