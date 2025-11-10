# Dynamic Sampling Configuration - Quick Start

## What You Get

The `config.sampling.yaml` now includes **dynamic sampling configuration** that lets you update sampling rules in real-time without restarting the collector!

## Files

- **`config.sampling.yaml`** - Collector configuration with dynamic sampling enabled
- **`DYNAMIC_SAMPLING.md`** - Complete guide and reference
- **`SAMPLING.md`** - Quick reference for the sampling feature
- **`manage-sampling-rules.sh`** - Helper script to manage rules

## Quick Example

### 1. Start the Collector

```bash
./_build/elastic-collector-with-pipeline-extension --config config.sampling.yaml
```

### 2. Create a Sampling Rule

```bash
# Sample 100% of nginx errors
./manage-sampling-rules.sh example1
```

### 3. Update the Rule (No Restart!)

```bash
# Change to 50% sampling
./manage-sampling-rules.sh update-rate nginx-errors 0.5

# Wait 30 seconds for the collector to pick up the change
```

### 4. Send Test Logs

```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
    "resourceLogs": [{
      "resource": {
        "attributes": [{
          "key": "service.name",
          "value": {"stringValue": "nginx"}
        }]
      },
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "'$(date +%s000000000)'",
          "severityText": "ERROR",
          "body": {"stringValue": "Connection timeout"},
          "attributes": [{
            "key": "stream.name",
            "value": {"stringValue": "logs-nginx-errors"}
          }]
        }]
      }]
    }]
  }'
```

## How It Works

```
1. You create/update rules in Elasticsearch (.elastic-sampling-config index)
   ↓
2. Extension polls Elasticsearch every 30s
   ↓
3. Processor uses dynamic rules to make sampling decisions
   ↓
4. Sampled logs sent to sample API (with dynamic index routing via stream.name)
```

## Features

### Dynamic Sampling Rules

- ✅ Update conditions and rates without restarts
- ✅ Different rates per stream/index pattern
- ✅ Priority-based rule matching
- ✅ Resource attribute matching
- ✅ OTTL condition expressions

### Dynamic Index Routing

The `rawsample` exporter routes to different indices based on `stream.name`:

```yaml
# Config has fallback index
rawsample:
  index: logs  # Used when stream.name is not present

# Log with stream.name goes to specific index
attributes["stream.name"] = "logs-nginx-errors" → sent to logs-nginx-errors index
```

## Helper Script Usage

```bash
# Create example rules
./manage-sampling-rules.sh example1  # Nginx errors
./manage-sampling-rules.sh example2  # All warnings
./manage-sampling-rules.sh example3  # Production errors

# Manage rules
./manage-sampling-rules.sh list                          # View all rules
./manage-sampling-rules.sh update-rate nginx-errors 0.5  # Change rate
./manage-sampling-rules.sh disable nginx-errors          # Disable
./manage-sampling-rules.sh enable nginx-errors           # Re-enable
./manage-sampling-rules.sh delete nginx-errors           # Delete
```

## Configuration Highlights

### Extensions

```yaml
extensions:
  # Dynamic sampling rules from Elasticsearch
  samplingconfigextension:
    endpoint: http://localhost:9200
    username: elastic
    password: password
    config_index: .elastic-sampling-config
    poll_interval: 30s
```

### Processor

```yaml
processors:
  samplingdecide:
    # Static fallback (used when no dynamic rule matches)
    condition: 'severity_text == "ERROR"'
    sample_rate: 1.0
    
    # Enable dynamic configuration
    extension_name: samplingconfigextension
```

## Example Rules

### Stream-Based Sampling

```bash
# 100% of nginx errors
curl -X PUT "http://localhost:9200/.elastic-sampling-config/_doc/nginx-errors" \
  -u elastic:password \
  -H "Content-Type: application/json" \
  -d '{
    "enabled": true,
    "priority": 100,
    "match": {
      "stream": "logs-nginx*",
      "condition": "severity_text == \"ERROR\""
    },
    "sample_rate": 1.0
  }'
```

### Environment-Based Sampling

```bash
# Different rates for prod vs staging
curl -X PUT "http://localhost:9200/.elastic-sampling-config/_doc/prod-errors" \
  -u elastic:password \
  -H "Content-Type: application/json" \
  -d '{
    "enabled": true,
    "priority": 100,
    "match": {
      "resource_attrs": {
        "deployment.environment": "production"
      },
      "condition": "severity_text == \"ERROR\""
    },
    "sample_rate": 1.0
  }'
```

## Next Steps

1. **Read the full guide**: See [DYNAMIC_SAMPLING.md](./DYNAMIC_SAMPLING.md) for complete documentation
2. **Create your first rule**: Use `./manage-sampling-rules.sh example1`
3. **Test with real logs**: Send logs with different stream names and severities
4. **Monitor the collector**: Watch logs for "Updated sampling rules" messages
5. **Experiment**: Try updating rules and see changes take effect without restarts!

## Benefits

- 🚀 **Zero downtime updates** - Change sampling without restarts
- 🎯 **Precise control** - Different rates per stream/environment
- 📊 **Cost optimization** - Reduce volume while keeping important logs
- 🔄 **Easy rollback** - Disable or update rules instantly
- 🌐 **Fleet management** - Manage sampling for all collectors centrally
