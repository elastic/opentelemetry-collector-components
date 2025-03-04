#!/bin/bash
set -eo pipefail

# This script gets the previous version from the
# previous commit, by running make get-version.
# Then it compares this previous version with the
# new given version. If there was an increase
# in version, then a release is required.

GITHUB_EVENT_NAME=$1
GITHUB_INPUT_VERSION=$2

WORKING_DIR="../../../loadgen/cmd/otelbench"

# Get new version
if [ "$GITHUB_EVENT_NAME" = "workflow_dispatch" ]; then
  NEW_VERSION="$GITHUB_INPUT_VERSION"
else
  NEW_VERSION=$(make -C "$WORKING_DIR" get-version 2>/dev/null || echo "v0.0.0")
fi

# Get previous version
PREV_VERSION="v0.0.0"
if git cat-file -e HEAD~1:"$WORKING_DIR/Makefile" 2>/dev/null; then
  PREV_VERSION=$(git show HEAD~1:"$WORKING_DIR/Makefile" | make -f - get-version 2>/dev/null || echo "v0.0.0")
fi

echo "Previous version: $PREV_VERSION."
echo "New version, obtained from $GITHUB_EVENT_NAME: $NEW_VERSION."
echo "new_version=$NEW_VERSION" >> $GITHUB_OUTPUT

# Check if a release is required.
# For a release to be required, the previous
# version needs to be smaller than the
# given one.
# v0.0.0 -> v0.1.0 -> works
# v0.1.0 -> v0.0.0 -> does not work
if [ "$(printf "%s\n%s" "$PREV_VERSION" "$NEW_VERSION" | sort -V | head -n1)" != "$NEW_VERSION" ]; then
  echo "Release required: $PREV_VERSION < $NEW_VERSION."
  exit 0
else
  echo "No release needed: $PREV_VERSION > $NEW_VERSION."
  exit 1
fi
