# Releasing opentelemetry-collector-components

## Optional steps

Normally the following steps are not required for releasing the components. The update of otel based on upstream
is automated and should not be performed manually unless there are specific reasons for this.

1. (optional) Open a PR to the repository to use the newly released OpenTelemetry Collector Core version by doing the following:
   - Ensure your local repository is up-to-date with upstream and create a new Git branch named `release/<release-series>` (e.g. `release/v0.85.x`)
   - Manually update core and contrib collector module versions in
     `../distributions/elastic-components/manifest.yaml`
   - Manually update the version to be released in the [versions.yaml file](../versions.yaml)
   - Run `make genelasticcol`
   - Commit the changes
   - Run `make update-otel OTEL_VERSION=vX.Y.Z OTEL_STABLE_VERSION=vA.B.C`
     - If there is no new stable version released in the core collector, use the current stable module version in contrib as `OTEL_STABLE_VERSION`.
   - If you were unable to run `make update-otel` before releasing core, fix any errors from breaking changes.
   - Commit the changes
   - Open the PR
     🛑 **Do not move forward until this PR is merged.** 🛑

## Create the new tags

2. Bump up the `module-sets.edit-base.version` in `versions.yaml` i.e. from `v0.20.0` to `v0.21.0`

    🛑 **Cross check latest version used by [EDOT](https://github.com/elastic/elastic-agent/blob/main/internal/pkg/otel/README.md?plain=1#L30) and https://github.com/elastic/opentelemetry-collector-components/tags** 🛑

3. Tag the module groups with the new release version by running:

   ⚠️ If you set your remote using `https`, you need to
      include `REMOTE=https://github.com/elastic/opentelemetry-collector-components.git` in each command. ⚠️

   - Run `make push-tags`.

   If you encounter an error when running `make push-tags` like:

   ```
   Load key "~/.ssh/id_ed25519": incorrect passphrase supplied to decrypt private key error: unable to sign the tag
   ```

   It’s likely because Git is trying to sign a tag using an SSH key that requires a passphrase.

   ✅ Solution: Run the following commands to decrypt your SSH key and make it available for Git tag signing:

   ```bash
   eval "$(ssh-agent -s)"
   ssh-add <YOUR_SSH/GPG_KEY_PATH>
   make push-tags
   ```

4. Last step is to commit the change of `module-sets.edit-base.version` in `versions.yaml` and push it so as to store
   the new latest version.
