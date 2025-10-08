# Releasing opentelemetry-collector-components

Try with dry-run first and verify if everything is working:

```bash
.github/workflows/scripts/bump_module_tags.sh \
--yaml ./versions.yaml \
  --repo-path . \
  --module-set edot-base \
  --bump minor \
  --branch main \
  --dry-run
```

If output of the dry-run looks legit then repeat without the `--dry-run`.
