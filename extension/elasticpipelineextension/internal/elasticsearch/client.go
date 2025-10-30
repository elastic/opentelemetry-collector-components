// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go.uber.org/zap"
)

// Client wraps the Elasticsearch client with pipeline-specific functionality.
type Client struct {
	client *elasticsearch.Client
	index  string
	logger *zap.Logger
}

// NewClient creates a new Elasticsearch client for pipeline configurations.
func NewClient(client *elasticsearch.Client, index string, logger *zap.Logger) *Client {
	return &Client{
		client: client,
		index:  index,
		logger: logger,
	}
}

// PipelineDocument represents a pipeline configuration document in Elasticsearch.
type PipelineDocument struct {
	PipelineID string           `json:"pipeline_id"`
	Agent      AgentConfig      `json:"agent"`
	Config     PipelineConfig   `json:"config"`
	Metadata   DocumentMetadata `json:"metadata"`
	Health     *PipelineHealth  `json:"health,omitempty"`
}

// AgentConfig defines agent targeting configuration.
type AgentConfig struct {
	Environment string            `json:"environment,omitempty"`
	Cluster     string            `json:"cluster,omitempty"`
	Version     string            `json:"version,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// PipelineConfig contains the OpenTelemetry pipeline configuration.
type PipelineConfig struct {
	Receivers  map[string]interface{}        `json:"receivers,omitempty"`
	Processors map[string]interface{}        `json:"processors,omitempty"`
	Exporters  map[string]interface{}        `json:"exporters,omitempty"`
	Connectors map[string]interface{}        `json:"connectors,omitempty"`
	Pipelines  map[string]PipelineDefinition `json:"pipelines,omitempty"`
}

// PipelineDefinition defines a signal-specific pipeline following OpenTelemetry model.
// Pipeline names follow the pattern "signal_type/pipeline_name" (e.g., "logs/application", "metrics/infrastructure")
type PipelineDefinition struct {
	Receivers  []string `json:"receivers"`
	Processors []string `json:"processors,omitempty"`
	Exporters  []string `json:"exporters"`
}

// DocumentMetadata contains document metadata.
type DocumentMetadata struct {
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	CreatedBy string    `json:"created_by"`
	Version   int64     `json:"version"`
	Enabled   bool      `json:"enabled"`
	Priority  int       `json:"priority"`
}

// PipelineHealth represents the health status of a pipeline.
type PipelineHealth struct {
	Status      string    `json:"status"`
	LastApplied time.Time `json:"last_applied"`
	LastError   string    `json:"last_error,omitempty"`
	ErrorCount  int64     `json:"error_count"`
}

// FilterConfig represents a filter for querying documents.
type FilterConfig struct {
	Field string
	Value string
}

// FetchConfigurations retrieves pipeline configurations from Elasticsearch.
func (c *Client) FetchConfigurations(ctx context.Context, filters []FilterConfig) ([]PipelineDocument, error) {
	query := c.buildQuery(filters)

	req := esapi.SearchRequest{
		Index: []string{c.index},
		Body:  query,
		Size:  func() *int { i := 1000; return &i }(), // Limit to 1000 documents
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to execute search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("search request failed: %s", res.Status())
	}

	var searchResult SearchResult
	if err := json.NewDecoder(res.Body).Decode(&searchResult); err != nil {
		return nil, fmt.Errorf("failed to decode search response: %w", err)
	}

	documents := make([]PipelineDocument, len(searchResult.Hits.Hits))
	for i, hit := range searchResult.Hits.Hits {
		documents[i] = hit.Source
		// Store document ID for potential updates
		documents[i].Metadata.Version = hit.Version
	}

	c.logger.Info("Fetched pipeline configurations",
		zap.Int("count", len(documents)),
		zap.String("index", c.index))

	return documents, nil
}

// UpdatePipelineHealth updates the health status of a pipeline in Elasticsearch.
func (c *Client) UpdatePipelineHealth(ctx context.Context, pipelineID string, health PipelineHealth) error {
	updateDoc := map[string]interface{}{
		"health": health,
		"metadata": map[string]interface{}{
			"updated_at": time.Now(),
		},
	}

	updateBody := map[string]interface{}{
		"doc": updateDoc,
	}

	body, err := json.Marshal(updateBody)
	if err != nil {
		return fmt.Errorf("failed to marshal update document: %w", err)
	}

	req := esapi.UpdateRequest{
		Index:      c.index,
		DocumentID: pipelineID,
		Body:       bytes.NewReader(body),
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to update pipeline health: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("update request failed: %s", res.Status())
	}

	c.logger.Debug("Updated pipeline health",
		zap.String("pipeline_id", pipelineID),
		zap.String("status", health.Status))

	return nil
}

func (c *Client) buildQuery(filters []FilterConfig) *strings.Reader {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"metadata.enabled": true,
						},
					},
				},
			},
		},
		"sort": []map[string]interface{}{
			{
				"metadata.priority": map[string]string{
					"order": "desc",
				},
			},
			{
				"metadata.updated_at": map[string]string{
					"order": "desc",
				},
			},
		},
	}

	// Add filters to the must clause
	if len(filters) > 0 {
		mustClause := query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"].([]map[string]interface{})

		for _, filter := range filters {
			mustClause = append(mustClause, map[string]interface{}{
				"term": map[string]interface{}{
					filter.Field: filter.Value,
				},
			})
		}

		query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = mustClause
	}

	queryBytes, _ := json.Marshal(query)
	return strings.NewReader(string(queryBytes))
}

// SearchResult represents the Elasticsearch search response.
type SearchResult struct {
	Hits struct {
		Hits []struct {
			Source  PipelineDocument `json:"_source"`
			Version int64            `json:"_version"`
		} `json:"hits"`
	} `json:"hits"`
}
