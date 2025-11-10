### Updated Proposal: Otel Sampling with `routing` Processor

The architecture will be split into three pipelines:

1.  **`logs/intake`:** Receives data, processes it, and adds the sampling ID.
2.  **`logs/prod`:** Receives from `intake`, *drops* the sampling ID, and sends to production.
3.  **`logs/sampling`:** Receives from `intake`, *uses* the sampling ID, and sends to the raw store.

---

### Proposed Architecture and Data Flow

**1. `raw_sampling_buffer` (Extension)**
This remains the same. It's a shared, in-memory circular buffer storing `(unique_id, raw_log_data)`.

**2. `logs/intake` (The Main Processing Pipeline)**
This pipeline does all the heavy lifting once.
* **`raw_capture` (Custom Processor):**
    * Runs *first* in the pipeline.
    * Generates a unique ID (UUID).
    * Clones the incoming raw log record.
    * Stores the `(UUID, raw_log_data)` pair in the `raw_sampling_buffer` extension.
    * **Adds the ID to the log record as `attributes['raw.id']`.**
* **Standard Processors (`resource`, `attributes`, etc.):**
    * These run as usual, modifying the log. The `attributes['raw.id']` is carried along with the other attributes.
* **`routing` (Standard Processor):**
    * This processor is last.
    * It forwards a copy of the *final, processed log* (which includes `attributes['raw.id']`) to *both* the `logs/prod` and `logs/sampling` pipelines.

**3. `logs/prod` (The Production-Bound Pipeline)**
This pipeline's only job is to clean the log for production.
* **`attributes/drop_id` (Standard Processor):**
    * Receives the log from the `intake` pipeline.
    * **Action:** Configured to `delete` the `attributes['raw.id']` key.
* **`elasticsearch/prod` (Exporter):**
    * Sends the *processed, cleaned* log (without `raw.id`) to the main data stream.

**4. `logs/sampling` (The Diagnostics Pipeline)**
This pipeline handles the sampling logic.
* **`sampling_decide` (Custom Processor):**
    * Receives the log from the `intake` pipeline.
    * Evaluates the tail-based condition (e.g., `attributes['network.name'] == 'Guest'`).
    * **If the condition is `false`, it drops the log.**
* **`raw_retriever` (Custom Processor):**
    * If the log passes, this processor reads the `attributes['raw.id']`.
    * It uses this ID to fetch the *original, raw* log from the `raw_sampling_buffer`.
    * It **replaces the entire body** of the current (processed) log with the original raw log.
* **`elasticsearch/raw` (Exporter):**
    * Sends the retrieved *raw* log to the dedicated `::raw` document store.

---

### Example Otel Collector Configuration

```yaml
extensions:
  raw_sampling_buffer:
    buffer_size: 1000

receivers:
  otlp:
    protocols:
      grpc:
      http:

processors:
  # == Custom Processors ==
  raw_capture: 
    # (Talks to raw_sampling_buffer)

  sampling_decide:
    condition: "attributes['network.name'] == 'Guest'"
    rate: 0.05
  
  raw_retriever: 
    # (Talks to raw_sampling_buffer)

  # == Standard Processors ==
  resource:
  attributes:
  transform:
  batch:
  
  # Standard processor to drop the ID from the production path
  attributes/drop_id:
    actions:
      - key: attributes['raw.id']
        action: delete

  # Standard routing processor to fork the pipeline
  routing:
    default_pipelines: [] # Don't send anywhere by default
    table:
      - statement: true # Send *all* logs
        pipelines: [logs/prod, logs/sampling]

exporters:
  elasticsearch/prod:
    es_url: "[https://es.example.com:9200](https://es.example.com:9200)"
    index: "logs-my_app-prod"
  
  elasticsearch/raw:
    es_url: "[https://es.example.com:9200](https://es.example.com:9200)"
    index: "::raw"

service:
  extensions:
    - raw_sampling_buffer

  pipelines:
    # == 1. Intake and Processing Pipeline ==
    logs/intake:
      receivers:
        - otlp
      processors:
        - raw_capture     # 1. Adds 'raw.id', stores raw log
        - resource        # 2. Standard processing
        - attributes
        - transform
        - batch
        - routing         # 3. Forks to 'prod' and 'sampling' pipelines

    # == 2. Production Pipeline (Cleans and Exports) ==
    logs/prod:
      receivers:
        - otlp # Receives from 'routing' processor (internal)
      processors:
        - attributes/drop_id # 1. Removes 'raw.id'
      exporters:
        - elasticsearch/prod   # 2. Exports clean log

    # == 3. Sampling Pipeline (Decides and Exports) ==
    logs/sampling:
      receivers:
        - otlp # Receives from 'routing' processor (internal)
      processors:
        - sampling_decide # 1. Checks condition, drops if no match
        - raw_retriever   # 2. Uses 'raw.id' to fetch raw log
      exporters:
        - elasticsearch/raw # 3. Exports raw log