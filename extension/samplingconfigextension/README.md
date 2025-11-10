# Sampling Config Extension

This extension fetches sampling configuration from Elasticsearch, allowing dynamic updates to sampling rules without collector restarts.

## Configuration

```yaml
extensions:
  samplingconfigextension:
    endpoint: http://localhost:9200
    auth:
      authenticator: basicauth/es
    config_index: .elastic-sampling-config
    poll_interval: 30s
    default_sample_rate: 0.0
```

## Elasticsearch Schema

Store sampling rules in the configured index (default: `.elastic-sampling-config`):

```json
{
  "id": "rule-1",
  "enabled": true,
  "priority": 100,
  "match": {
    "stream": "logs-nginx*",
    "resource_attrs": {
      "deployment.environment": "production"
    },
    "condition": "severity_text == \"ERROR\""
  },
  "sample_rate": 0.1,
  "updated_at": "2025-11-10T14:00:00Z"
}
```

## Usage with samplingdecide Processor

```yaml
processors:
  samplingdecide:
    # Static fallback config
    condition: 'severity_text == "ERROR"'
    sample_rate: 0.1
    # Use dynamic config from extension
    extension_name: samplingconfigextension
```

The processor will query the extension for matching rules and use the highest priority rule that matches.
