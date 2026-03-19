# Elastic Metadata Processor

## Description

The Elastic Metadata Processor enriches log data with metadata from the context metadata.
The log body must be a `pcommon.Map` for enrichment to take effect. Other body types are passed through unchanged.

Currently, supports logs only.

## Configuration

```yaml
processors:
  elasticmetadata:
    log_metadata:
      # Following has the meaning of context_key: destination_key
      # For example, if context metadata has "cloud.provider": "aws", then the log will be enriched with "cloud_provider": "aws"
      cloud.provider: cloud_provider
```