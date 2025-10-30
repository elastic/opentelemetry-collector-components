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
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Fetcher provides cached access to pipeline configurations from Elasticsearch.
type Fetcher struct {
	client        *Client
	cache         map[string]cacheEntry
	cacheDuration time.Duration
	filters       []FilterConfig
	logger        *zap.Logger
	mu            sync.RWMutex
	lastFetch     time.Time
}

type cacheEntry struct {
	data      []PipelineDocument
	timestamp time.Time
}

// NewFetcher creates a new configuration fetcher with caching.
func NewFetcher(client *Client, cacheDuration time.Duration, filters []FilterConfig, logger *zap.Logger) (*Fetcher, error) {
	return &Fetcher{
		client:        client,
		cache:         make(map[string]cacheEntry),
		cacheDuration: cacheDuration,
		filters:       filters,
		logger:        logger,
	}, nil
}

// FetchConfigurations retrieves pipeline configurations, using cache when possible.
func (f *Fetcher) FetchConfigurations(ctx context.Context) ([]PipelineDocument, error) {
	f.mu.RLock()
	cacheKey := f.buildCacheKey()

	// Check cache first
	if entry, ok := f.cache[cacheKey]; ok && time.Since(entry.timestamp) < f.cacheDuration {
		f.mu.RUnlock()
		f.logger.Debug("Using cached pipeline configurations", zap.Int("count", len(entry.data)))
		return entry.data, nil
	}
	f.mu.RUnlock()

	// Cache miss or expired, fetch from Elasticsearch
	f.mu.Lock()
	defer f.mu.Unlock()

	// Double-check cache after acquiring write lock
	if entry, ok := f.cache[cacheKey]; ok && time.Since(entry.timestamp) < f.cacheDuration {
		f.logger.Debug("Using cached pipeline configurations (double-check)", zap.Int("count", len(entry.data)))
		return entry.data, nil
	}

	f.logger.Debug("Fetching pipeline configurations from Elasticsearch")
	documents, err := f.client.FetchConfigurations(ctx, f.filters)
	if err != nil {
		return nil, err
	}

	// Cache the results
	f.cache[cacheKey] = cacheEntry{
		data:      documents,
		timestamp: time.Now(),
	}
	f.lastFetch = time.Now()

	f.logger.Info("Fetched and cached pipeline configurations",
		zap.Int("count", len(documents)),
		zap.Duration("cache_duration", f.cacheDuration))

	return documents, nil
}

// InvalidateCache clears the cache, forcing a fresh fetch on next request.
func (f *Fetcher) InvalidateCache() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.cache = make(map[string]cacheEntry)
	f.logger.Debug("Pipeline configuration cache invalidated")
}

// UpdateFilters updates the query filters and invalidates the cache.
func (f *Fetcher) UpdateFilters(filters []FilterConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.filters = filters
	f.cache = make(map[string]cacheEntry)
	f.logger.Debug("Updated pipeline configuration filters", zap.Int("filter_count", len(filters)))
}

// GetLastFetchTime returns the time of the last successful fetch.
func (f *Fetcher) GetLastFetchTime() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastFetch
}

// buildCacheKey creates a cache key based on current filters.
func (f *Fetcher) buildCacheKey() string {
	key := "pipeline_configs"
	for _, filter := range f.filters {
		key += "_" + filter.Field + ":" + filter.Value
	}
	return key
}
