# Elastic Metadata Processor

## Description

The Elastic Metadata Processor enriches log data with metadata from the context metadata.
The log body must be a `pcommon.Map` for enrichment to take effect. Other body types are passed through unchanged.

Currently, supports logs only.

## Configuration

```yaml
processors:
  elasticmetadata:
    log_body_fields:
      # Following has the meaning of context_key: destination_key
      # For example, if context metadata has "cloud.region": "us-east-1", then the log will be enriched with "aws.region": "us-east-1"
      cloud.region: aws.region
```