# Raw Sampling Buffer Extension

This extension provides a shared in-memory circular buffer for storing raw log data. It is used in conjunction with the `rawcapture` and `rawretriever` processors to implement tail-based sampling for logs.

## Configuration

```yaml
extensions:
  rawsamplingbuffer:
    buffer_size: 10000          # Maximum number of entries (default: 1000)
    max_entry_size: 1048576     # Maximum size per entry in bytes (default: 1MB)
    ttl: 5m                     # Time-to-live for entries (default: 5m)
    eviction_policy: lru        # Eviction policy (default: lru)
```

## How It Works

1. The buffer stores raw log data with a unique ID (UUID)
2. Entries are automatically evicted using LRU (Least Recently Used) policy when the buffer is full
3. Entries expire after the configured TTL
4. A background cleanup goroutine removes expired entries periodically

## Usage

This extension must be referenced by the `rawcapture` and `rawretriever` processors:

```yaml
extensions:
  rawsamplingbuffer:
    buffer_size: 10000

processors:
  rawcapture:
    extension_name: rawsamplingbuffer
    
  rawretriever:
    extension_name: rawsamplingbuffer

service:
  extensions:
    - rawsamplingbuffer
  pipelines:
    logs:
      processors:
        - rawcapture
        - rawretriever
```
