// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package rawsampleexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// rawSampleExporter implements the logs exporter.
type rawSampleExporter struct {
	config     *Config
	httpClient *http.Client
	logger     *zap.Logger
}

// Start starts the exporter.
func (e *rawSampleExporter) Start(_ context.Context, _ component.Host) error {
	e.httpClient = &http.Client{
		Timeout: e.config.Timeout,
	}
	return nil
}

// Shutdown stops the exporter.
func (e *rawSampleExporter) Shutdown(_ context.Context) error {
	if e.httpClient != nil {
		e.httpClient.CloseIdleConnections()
	}
	return nil
}

// ConsumeLogs processes and sends logs to the Elasticsearch sample API.
func (e *rawSampleExporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	// Group documents by index (stream.name or fallback)
	docsByIndex := make(map[string][]json.RawMessage)

	// Iterate through all logs and serialize to JSON
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rl := logs.ResourceLogs().At(i)
		resource := rl.Resource()

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			scope := sl.Scope()

			for k := 0; k < sl.LogRecords().Len(); k++ {
				record := sl.LogRecords().At(k)

				// Determine target index from stream.name attribute or use fallback
				targetIndex := e.config.Index
				if streamName, ok := record.Attributes().Get("target_stream"); ok {
					targetIndex = streamName.AsString()
				}

				// Serialize log to JSON
				doc, err := e.serializeLogRecord(resource, scope, record)
				if err != nil {
					e.logger.Warn("Failed to serialize log", zap.Error(err))
					continue
				}

				// Group by index
				docsByIndex[targetIndex] = append(docsByIndex[targetIndex], doc)
			}
		}
	}

	if len(docsByIndex) == 0 {
		return nil
	}

	// Send batches for each index
	for index, docs := range docsByIndex {
		if err := e.sendBatches(ctx, index, docs); err != nil {
			return err
		}
	}

	return nil
}

// serializeLogRecord converts a log record to JSON.
func (e *rawSampleExporter) serializeLogRecord(resource, scope, record interface{}) (json.RawMessage, error) {
	logRecord := record.(plog.LogRecord)
	res := resource.(pcommon.Resource)

	// Build document
	doc := make(map[string]interface{})

	// Add timestamp
	if ts := logRecord.Timestamp(); ts != 0 {
		doc["@timestamp"] = ts.AsTime().Format("2006-01-02T15:04:05.000000000Z")
	} else if ots := logRecord.ObservedTimestamp(); ots != 0 {
		doc["@timestamp"] = ots.AsTime().Format("2006-01-02T15:04:05.000000000Z")
	}

	// Add severity
	if sevText := logRecord.SeverityText(); sevText != "" {
		doc["severity_text"] = sevText
	}
	if sevNum := logRecord.SeverityNumber(); sevNum != 0 {
		doc["severity_number"] = int(sevNum)
	}

	// Add trace context
	if traceID := logRecord.TraceID(); !traceID.IsEmpty() {
		doc["trace_id"] = traceID.String()
	}
	if spanID := logRecord.SpanID(); !spanID.IsEmpty() {
		doc["span_id"] = spanID.String()
	}

	// Add body
	if body := logRecord.Body(); body.Type() != 0 {
		bodyObj := make(map[string]interface{})
		if body.Type() == 1 { // String type
			bodyObj["text"] = body.Str()
		} else if body.Type() == 5 { // Map type
			bodyObj["structured"] = body.Map().AsRaw()
		} else {
			bodyObj["text"] = body.AsString()
		}
		doc["body"] = bodyObj
	}

	// Add resource attributes
	if res.Attributes().Len() > 0 {
		doc["resource"] = map[string]interface{}{
			"attributes": res.Attributes().AsRaw(),
		}
	}

	// Add log record attributes
	if logRecord.Attributes().Len() > 0 {
		doc["attributes"] = logRecord.Attributes().AsRaw()
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal log document: %w", err)
	}

	return json.RawMessage(jsonData), nil
}

// sendBatches sends documents in batches to avoid large payloads.
func (e *rawSampleExporter) sendBatches(ctx context.Context, index string, docs []json.RawMessage) error {
	batchSize := e.config.MaxBatchSize
	if batchSize <= 0 {
		batchSize = 100 // default
	}

	for i := 0; i < len(docs); i += batchSize {
		end := i + batchSize
		if end > len(docs) {
			end = len(docs)
		}

		batch := docs[i:end]
		if err := e.sendBatch(ctx, index, batch); err != nil {
			return err
		}
	}

	return nil
}

// sendBatch sends a single batch of documents to the sample API.
func (e *rawSampleExporter) sendBatch(ctx context.Context, index string, docs []json.RawMessage) error {
	// Wrap array in object with "docs" property
	requestBody := map[string]interface{}{
		"docs": docs,
	}

	payload, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Build URL with the specified index
	url := fmt.Sprintf("%s/%s/_sample/docs", e.config.Endpoint, index)

	// Create request
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	// Add Basic Auth if credentials are provided
	if e.config.Username != "" || e.config.Password != "" {
		req.SetBasicAuth(e.config.Username, e.config.Password)
	}

	// Send request
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("sample API error: %d - %s", resp.StatusCode, string(body))
	}

	e.logger.Debug("Sent batch to sample API",
		zap.Int("count", len(docs)),
		zap.String("index", index),
	)

	return nil
}
