# Sampling Decide Processor

This processor evaluates tail-based sampling conditions using OTTL (OpenTelemetry Transformation Language) and filters logs based on conditions and sample rates.

## Configuration

```yaml
processors:
  samplingdecide:
    condition: 'attributes["network.name"] == "Guest"'  # OTTL condition (required)
    sample_rate: 0.05       # Probability 0.0-1.0 (default: 1.0)
    invert_match: false     # Invert condition logic (default: false)
```

## How It Works

1. For each log record:
   - Evaluates the OTTL condition
   - Applies invert logic if configured
   - Applies probabilistic sampling based on sample_rate
   - Drops logs that don't match or aren't sampled

2. Only logs that pass all checks are forwarded to the next consumer

## Usage

Typically used in the sampling pipeline to filter which logs should have their raw versions retrieved:

```yaml
processors:
  samplingdecide:
    condition: 'attributes["network.name"] == "Guest"'
    sample_rate: 0.05

service:
  pipelines:
    logs/sampling:
      processors:
        - samplingdecide
        - rawretriever
```

## OTTL Condition Examples

```yaml
# Sample logs from specific network
condition: 'attributes["network.name"] == "Guest"'

# Sample error logs
condition: 'severity_number >= SEVERITY_NUMBER_ERROR'

# Sample logs with specific body content
condition: 'IsMatch(body, ".*error.*")'

# Multiple conditions
condition: 'attributes["environment"] == "prod" and severity_number >= SEVERITY_NUMBER_WARN'
```
