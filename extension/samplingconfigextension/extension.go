package samplingconfigextension

// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/pkg/samplingconfig"
)

// samplingConfigExtension implements the sampling config extension.
type samplingConfigExtension struct {
	config *Config
	logger *zap.Logger

	// Rules cache
	mu    sync.RWMutex
	rules []*samplingconfig.Rule

	// Lifecycle
	stopCh chan struct{}
	doneCh chan struct{}
}

var _ component.Component = (*samplingConfigExtension)(nil)
var _ samplingconfig.Provider = (*samplingConfigExtension)(nil)

func newSamplingConfigExtension(config *Config, logger *zap.Logger) *samplingConfigExtension {
	return &samplingConfigExtension{
		config: config,
		logger: logger,
		rules:  make([]*samplingconfig.Rule, 0),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start starts the extension.
func (e *samplingConfigExtension) Start(ctx context.Context, host component.Host) error {
	e.logger.Info("Starting sampling config extension",
		zap.String("endpoint", e.config.Endpoint),
		zap.String("config_index", e.config.ConfigIndex),
		zap.Duration("poll_interval", e.config.PollInterval),
	)

	// Do initial fetch
	if err := e.fetchRules(ctx); err != nil {
		e.logger.Warn("Failed to fetch initial rules", zap.Error(err))
	}

	// Start polling goroutine
	go e.pollRules()

	return nil
}

// Shutdown stops the extension.
func (e *samplingConfigExtension) Shutdown(_ context.Context) error {
	e.logger.Info("Stopping sampling config extension")
	close(e.stopCh)
	<-e.doneCh
	return nil
}

// GetSamplingRule implements samplingconfig.Provider.
func (e *samplingConfigExtension) GetSamplingRule(ctx context.Context, streamName string, resourceAttrs map[string]any) (*samplingconfig.Rule, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Find the highest priority matching rule
	for _, rule := range e.rules {
		if !rule.Enabled {
			continue
		}

		// Check stream pattern match
		if rule.Match.StreamPattern != "" && streamName != "" {
			if !matchPattern(rule.Match.StreamPattern, streamName) {
				continue
			}
		}

		// Check resource attributes match
		if len(rule.Match.ResourceAttrs) > 0 {
			if !matchResourceAttrs(rule.Match.ResourceAttrs, resourceAttrs) {
				continue
			}
		}

		// Found a match
		return rule, nil
	}

	return nil, nil
}

// pollRules periodically fetches rules from Elasticsearch.
func (e *samplingConfigExtension) pollRules() {
	defer close(e.doneCh)

	ticker := time.NewTicker(e.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := e.fetchRules(ctx); err != nil {
				e.logger.Warn("Failed to fetch rules", zap.Error(err))
			}
			cancel()
		case <-e.stopCh:
			return
		}
	}
}

// fetchRules fetches sampling rules from Elasticsearch.
func (e *samplingConfigExtension) fetchRules(ctx context.Context) error {
	// Build search URL
	url := fmt.Sprintf("%s/%s/_search", e.config.Endpoint, e.config.ConfigIndex)

	// Create search request (get all enabled rules)
	searchBody := map[string]any{
		"query": map[string]any{
			"match_all": map[string]any{},
		},
		"size": 1000,
	}

	bodyBytes, err := json.Marshal(searchBody)
	if err != nil {
		return fmt.Errorf("failed to marshal search body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Add Basic Auth if credentials are provided
	if e.config.Username != "" {
		req.SetBasicAuth(e.config.Username, e.config.Password)
	}

	// Send request
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to search: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("elasticsearch returned status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var searchResp struct {
		Hits struct {
			Hits []struct {
				ID     string                 `json:"_id"`
				Source map[string]interface{} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Parse rules
	rules := make([]*samplingconfig.Rule, 0, len(searchResp.Hits.Hits))
	for _, hit := range searchResp.Hits.Hits {
		rule, err := parseRule(hit.ID, hit.Source)
		if err != nil {
			e.logger.Warn("Failed to parse rule", zap.String("id", hit.ID), zap.Error(err))
			continue
		}
		rules = append(rules, rule)
	}

	// Sort by priority (descending)
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].Priority > rules[j].Priority
	})

	// Update cache
	e.mu.Lock()
	e.rules = rules
	e.mu.Unlock()

	e.logger.Info("Updated sampling rules", zap.Int("count", len(rules)))

	return nil
}

// parseRule parses a rule document from Elasticsearch.
func parseRule(id string, source map[string]interface{}) (*samplingconfig.Rule, error) {
	rule := &samplingconfig.Rule{
		ID: id,
	}

	// Parse enabled (default true)
	if enabled, ok := source["enabled"].(bool); ok {
		rule.Enabled = enabled
	} else {
		rule.Enabled = true
	}

	// Parse priority (default 0)
	if priority, ok := source["priority"].(float64); ok {
		rule.Priority = int(priority)
	}

	// Parse match criteria
	if matchMap, ok := source["match"].(map[string]interface{}); ok {
		if stream, ok := matchMap["stream"].(string); ok {
			rule.Match.StreamPattern = stream
		}
		if resAttrs, ok := matchMap["resource_attrs"].(map[string]interface{}); ok {
			rule.Match.ResourceAttrs = resAttrs
		}
		if condition, ok := matchMap["condition"].(string); ok {
			rule.Condition = condition
		}
	}

	// Parse sample_rate (required)
	if sampleRate, ok := source["sample_rate"].(float64); ok {
		rule.SampleRate = sampleRate
	} else {
		return nil, fmt.Errorf("missing sample_rate")
	}

	// Parse updated_at
	if updatedAt, ok := source["updated_at"].(string); ok {
		rule.UpdatedAt = updatedAt
	}

	return rule, nil
}

// matchPattern matches a string against a glob pattern.
func matchPattern(pattern, str string) bool {
	// Simple glob matching (* wildcard only)
	if pattern == "*" {
		return true
	}

	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(str, prefix)
	}

	if strings.HasPrefix(pattern, "*") {
		suffix := strings.TrimPrefix(pattern, "*")
		return strings.HasSuffix(str, suffix)
	}

	return pattern == str
}

// matchResourceAttrs checks if resource attributes match the rule criteria.
func matchResourceAttrs(ruleAttrs, resourceAttrs map[string]any) bool {
	for key, expectedVal := range ruleAttrs {
		actualVal, ok := resourceAttrs[key]
		if !ok {
			return false
		}

		// Simple equality check
		if fmt.Sprint(expectedVal) != fmt.Sprint(actualVal) {
			return false
		}
	}

	return true
}
