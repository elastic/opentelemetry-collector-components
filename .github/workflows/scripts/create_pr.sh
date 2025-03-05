#!/bin/bash
set -eo pipefail

VERSION=$1
GITHUB_EVENT_NAME=$2
BRANCH_NAME="update-changelog-$VERSION"

(
  pushd ../../../loadgen/cmd/otelbench
  ./chloggen update --version $VERSION

  if [ "$GITHUB_EVENT_NAME" = "workflow_dispatch" ]; then
    # update version inside Makefile
    sed -i "s/^VERSION := .*/VERSION := $VERSION/" Makefile
  fi
)

# go to root of this project
cd ../../../

# commit changes
git checkout -b "$BRANCH_NAME"
git add ./loadgen/cmd/otelbench
if ! git diff --quiet ./loadgen/cmd/otelbench; then
  git commit -m "Update otelbench changelog and delete changelog fragments."
  git push origin "$BRANCH_NAME"
else
  echo "No changes to commit."
  exit 0
fi

echo "Create PR"
gh pr create \
  --title="[loadgen/otelbench] Update changelog for $VERSION" \
  --base "main" \
  --head "$BRANCH_NAME" \
  -b "This PR updates the changelog and removes old changelog fragments."