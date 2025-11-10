# Dynamic Sampling Configuration Guide

This guide explains how to use the dynamic sampling configuration feature to update sampling rules in real-time without restarting the collector.

## Overview

The `samplingconfigextension` fetches sampling rules from Elasticsearch and provides them to the `samplingdecide` processor. This allows you to:

- **Update sampling rules without collector restarts**
- **Configure different sampling rates per stream/index**
- **Apply resource-based sampling rules**
- **Manage sampling centrally across a fleet of collectors**

## Architecture

```
Elasticsearch (.elastic-sampling-config index)
    ↓ (polls every 30s)
samplingconfigextension
    ↓ (provides rules)
samplingdecide processor
    ↓ (applies matching rule)
Sampled logs → rawsample exporter
```

## Configuration

### 1. Enable the Extension

The extension is already configured in `config.sampling.yaml`:

```yaml
extensions:
  samplingconfigextension:
    endpoint: http://localhost:9200
    username: elastic
    password: password
    config_index: .elastic-sampling-config  # Index storing rules
    poll_interval: 30s                       # How often to check for updates
    default_sample_rate: 0.0                 # Fallback when no rules match
```

### 2. Configure the Processor

The `samplingdecide` processor uses both static and dynamic configuration:

```yaml
processors:
  samplingdecide:
    # Static fallback (used when no dynamic rule matches)
    condition: 'severity_text == "ERROR"'
    sample_rate: 1.0
    
    # Enable dynamic configuration
    extension_name: samplingconfigextension
```

## Creating Sampling Rules

### Rule Structure

Store sampling rules in the `.elastic-sampling-config` index:

```json
{
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

### Field Descriptions

- **`enabled`** (boolean): Whether this rule is active
- **`priority`** (integer): Higher priority rules are evaluated first
- **`match`** (object): Criteria for when this rule applies
  - **`stream`** (string, optional): Glob pattern to match `stream.name` attribute
    - Examples: `"logs-nginx*"`, `"logs-*"`, `"*"`
  - **`resource_attrs`** (object, optional): Resource attributes that must match
  - **`condition`** (string): OTTL expression to evaluate on each log
- **`sample_rate`** (float): Probability of sampling (0.0 to 1.0)
  - `1.0` = 100% (sample all matching logs)
  - `0.1` = 10% (sample 1 in 10 matching logs)
  - `0.0` = 0% (drop all matching logs)
- **`updated_at`** (string, optional): Timestamp for tracking changes

## Examples

### Example 1: High-Priority Errors for Nginx

Sample ALL errors from nginx logs:

```bash
PUT .elastic-sampling-config/_doc/nginx-errors
{
  "enabled": true,
  "priority": 100,
  "match": {
    "stream": "logs-nginx*",
    "condition": "severity_text == \"ERROR\""
  },
  "sample_rate": 1.0,
  "updated_at": "2025-11-10T15:00:00Z"
}
```

### Example 2: Lower Rate for Warnings

Sample 10% of warnings from all streams:

```bash
PUT .elastic-sampling-config/_doc/all-warnings
{
  "enabled": true,
  "priority": 50,
  "match": {
    "stream": "*",
    "condition": "severity_text == \"WARN\" or severity_text == \"WARNING\""
  },
  "sample_rate": 0.1,
  "updated_at": "2025-11-10T15:00:00Z"
}
```

### Example 3: Environment-Specific Sampling

Sample production errors at 100%, staging at 10%:

```bash
# Production - high rate
PUT .elastic-sampling-config/_doc/prod-errors
{
  "enabled": true,
  "priority": 100,
  "match": {
    "resource_attrs": {
      "deployment.environment": "production"
    },
    "condition": "severity_text == \"ERROR\""
  },
  "sample_rate": 1.0
}

# Staging - lower rate
PUT .elastic-sampling-config/_doc/staging-errors
{
  "enabled": true,
  "priority": 90,
  "match": {
    "resource_attrs": {
      "deployment.environment": "staging"
    },
    "condition": "severity_text == \"ERROR\""
  },
  "sample_rate": 0.1
}
```

### Example 4: Specific Error Codes

Sample all HTTP 5xx errors at 100%:

```bash
PUT .elastic-sampling-config/_doc/http-5xx-errors
{
  "enabled": true,
  "priority": 110,
  "match": {
    "condition": "attributes[\"http.status_code\"] >= 500 and attributes[\"http.status_code\"] < 600"
  },
  "sample_rate": 1.0
}
```

## How Rules Are Applied

### Rule Matching Process

1. **Fetch rules** from Elasticsearch (every 30s)
2. **Sort by priority** (highest first)
3. For each log record:
   - Check if `stream.name` matches the pattern (if specified)
   - Check if resource attributes match (if specified)
   - If both match, use this rule's `condition` and `sample_rate`
   - If no rule matches, use static fallback configuration

### Priority Example

```
Priority 100: logs-nginx* + ERROR → 100% sampling
Priority 50:  logs-*      + ERROR → 10% sampling
Fallback:     ERROR               → 100% sampling
```

A log from `logs-nginx-access` with ERROR severity will match the first rule (priority 100) and be sampled at 100%.

## Testing the Configuration

### 1. Start the Collector

```bash
cd distributions/elastic-components
./_build/elastic-collector-with-pipeline-extension --config config.sampling.yaml
```

### 2. Create a Sampling Rule

```bash
curl -X PUT "http://localhost:9200/.elastic-sampling-config/_doc/test-rule" \
  -H "Content-Type: application/json" \
  -u elastic:password \
  -d '{
    "enabled": true,
    "priority": 100,
    "match": {
      "stream": "logs-test*",
      "condition": "severity_text == \"ERROR\""
    },
    "sample_rate": 1.0
  }'
```

### 3. Send Test Logs

```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
    "resourceLogs": [{
      "resource": {
        "attributes": [{
          "key": "service.name",
          "value": {"stringValue": "test-service"}
        }]
      },
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "'$(date +%s000000000)'",
          "severityText": "ERROR",
          "severityNumber": 17,
          "body": {"stringValue": "Test error log"},
          "attributes": [{
            "key": "stream.name",
            "value": {"stringValue": "logs-test-errors"}
          }]
        }]
      }]
    }]
  }'
```

### 4. Update the Rule (No Restart Required!)

```bash
curl -X POST "http://localhost:9200/.elastic-sampling-config/_update/test-rule" \
  -H "Content-Type: application/json" \
  -u elastic:password \
  -d '{
    "doc": {
      "sample_rate": 0.5,
      "updated_at": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
    }
  }'
```

After 30 seconds (or whatever `poll_interval` is set to), the collector will pick up the new rate automatically!

### 5. Disable a Rule

```bash
curl -X POST "http://localhost:9200/.elastic-sampling-config/_update/test-rule" \
  -H "Content-Type: application/json" \
  -u elastic:password \
  -d '{
    "doc": {
      "enabled": false
    }
  }'
```

### 6. View Active Rules

```bash
curl -X GET "http://localhost:9200/.elastic-sampling-config/_search?pretty" \
  -u elastic:password
```

## Dynamic Index Routing

The `rawsample` exporter also supports dynamic index selection via `stream.name`:

```yaml
exporters:
  rawsample:
    endpoint: http://localhost:9200
    username: elastic
    password: password
    index: logs  # Fallback index
```

If a log has `attributes["stream.name"] = "logs-nginx-errors"`, it will be sent to the `logs-nginx-errors` index instead of the fallback `logs` index.

## Monitoring

### Check Extension Logs

Look for these log messages:

```
Starting sampling config extension
Updated sampling rules count=3
Using dynamic sampling configuration extension=samplingconfigextension
```

### Troubleshooting

**No rules being applied:**
- Check that extension is listed in `service.extensions`
- Verify `extension_name` is set in `samplingdecide` processor
- Check Elasticsearch connectivity and credentials
- Look for errors in collector logs

**Rules not updating:**
- Wait for next poll interval (default 30s)
- Check if rules are enabled: `"enabled": true`
- Verify `config_index` exists and is accessible

**Fallback to static config:**
- This is normal when no rules match
- Check rule `match.stream` patterns
- Verify `match.resource_attrs` values
- Test OTTL `condition` expressions

## Advanced OTTL Conditions

The `condition` field uses OTTL (OpenTelemetry Transformation Language):

### Severity-Based
```json
"condition": "severity_text == \"ERROR\" or severity_text == \"FATAL\""
```

### Attribute-Based
```json
"condition": "attributes[\"http.status_code\"] >= 500"
```

### Body Content
```json
"condition": "IsMatch(body, \".*timeout.*\")"
```

### Combined Conditions
```json
"condition": "severity_text == \"ERROR\" and attributes[\"service.name\"] == \"payment-service\""
```

## Best Practices

1. **Use priorities wisely**: Higher priority for more specific rules
2. **Start conservative**: Begin with lower sample rates and increase as needed
3. **Monitor cardinality**: High sample rates on high-volume streams can overwhelm Elasticsearch
4. **Document changes**: Use `updated_at` field to track when rules were modified
5. **Test before production**: Create disabled rules first, test, then enable
6. **Use stream patterns**: Match multiple indices with `logs-nginx*` instead of individual rules

## Summary

The dynamic sampling configuration provides:

✅ **No restarts needed** - Update rules via Elasticsearch API  
✅ **Centralized control** - Manage all collectors from one place  
✅ **Flexible matching** - Stream patterns, resource attributes, OTTL conditions  
✅ **Priority system** - Control which rules apply first  
✅ **Graceful fallback** - Static config when no rules match  

This makes it easy to adjust sampling behavior in production without any downtime!
