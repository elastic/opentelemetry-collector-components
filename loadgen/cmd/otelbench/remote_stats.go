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

package main

import (
	"context"
	"errors"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

type statsAggregation interface {
	Aggregations(string) map[string]types.Aggregations
	Value(string, map[string]types.Aggregate) *float64
}

type avg struct{}

func (avg) Aggregations(metric string) map[string]types.Aggregations {
	return map[string]types.Aggregations{
		metric + "_avg": {Avg: &types.AverageAggregation{Field: some.String(metric)}},
	}
}

func (avg) Value(metric string, aggs map[string]types.Aggregate) *float64 {
	if a, ok := aggs[metric+"_avg"].(*types.AvgAggregate); ok {
		return (*float64)(a.Value)
	}
	return nil
}

type minMax struct{}

func (minMax) Aggregations(metric string) map[string]types.Aggregations {
	return map[string]types.Aggregations{
		metric + "_min": {Min: &types.MinAggregation{Field: some.String(metric)}},
		metric + "_max": {Max: &types.MaxAggregation{Field: some.String(metric)}},
	}
}

func (minMax) Value(metric string, aggs map[string]types.Aggregate) *float64 {
	min, minOk := aggs[metric+"_min"].(*types.MinAggregate)
	max, maxOk := aggs[metric+"_max"].(*types.MaxAggregate)
	if minOk && maxOk && min.Value != nil && max.Value != nil {
		v := float64(*max.Value) - float64(*min.Value)
		return &v
	}
	return nil
}

// knownMetricsAggregations corresponds to know basic-level metrics from
// https://opentelemetry.io/docs/collector/internal-telemetry/#basic-level-metrics.
var knownMetricsAggregations map[string]statsAggregation = map[string]statsAggregation{
	"otelcol_exporter_enqueue_failed_log_records":     minMax{},
	"otelcol_exporter_enqueue_failed_metric_points":   minMax{},
	"otelcol_exporter_enqueue_failed_spans":           minMax{},
	"otelcol_exporter_queue_capacity":                 avg{},
	"otelcol_exporter_queue_size":                     avg{},
	"otelcol_exporter_send_failed_log_records":        minMax{},
	"otelcol_exporter_send_failed_metric_points":      minMax{},
	"otelcol_exporter_send_failed_spans":              minMax{},
	"otelcol_exporter_sent_log_records":               minMax{},
	"otelcol_exporter_sent_metric_points":             minMax{},
	"otelcol_exporter_sent_spans":                     minMax{},
	"otelcol_process_cpu_seconds":                     minMax{},
	"otelcol_process_memory_rss":                      avg{},
	"otelcol_process_runtime_heap_alloc_bytes":        avg{},
	"otelcol_process_runtime_total_alloc_bytes":       minMax{},
	"otelcol_process_runtime_total_sys_memory_bytes":  avg{},
	"otelcol_process_uptime":                          minMax{},
	"otelcol_processor_batch_batch_send_size":         minMax{},
	"otelcol_processor_batch_batch_size_trigger_send": minMax{},
	"otelcol_processor_batch_metadata_cardinality":    minMax{},
	"otelcol_processor_batch_timeout_trigger_send":    minMax{},
	"otelcol_processor_incoming_items":                minMax{},
	"otelcol_processor_outgoing_items":                minMax{},
	"otelcol_receiver_accepted_log_records":           minMax{},
	"otelcol_receiver_accepted_metric_points":         minMax{},
	"otelcol_receiver_accepted_spans":                 minMax{},
	"otelcol_receiver_refused_log_records":            minMax{},
	"otelcol_receiver_refused_metric_points":          minMax{},
	"otelcol_receiver_refused_spans":                  minMax{},
	"otelcol_scraper_errored_metric_points":           minMax{},
	"otelcol_scraper_scraped_metric_points":           minMax{},
}

type elasticsearchTelemetryConfig TelemetryConfig

func (cfg elasticsearchTelemetryConfig) validate() error {
	if len(cfg.ElasticsearchURL) == 0 {
		return errors.New("remote stats will be ignored because of empty telemetry Elasticsearch URL")
	}
	if cfg.ElasticsearchAPIKey == "" && (cfg.ElasticsearchUserName == "" || cfg.ElasticsearchPassword == "") {
		return errors.New("remote stats will be ignored because of empty telemetry Elasticsearch API key and username or password")
	}
	if cfg.ElasticsearchIndex == "" {
		return errors.New("remote stats will be ignored because of empty telemetry Elasticsearch search index pattern")
	}
	if len(cfg.Metrics) == 0 {
		return errors.New("remote stats will be ignored because of empty telemetry metrics list")
	}
	return nil
}

type remoteStatsFetcher interface {
	FetchStats(ctx context.Context, from, to time.Time) (map[string]float64, error)
}

type elasticsearchStatsFetcher struct {
	elasticsearchTelemetryConfig
	client *elasticsearch.TypedClient
}

func newElasticsearchStatsFetcher(cfg elasticsearchTelemetryConfig) (remoteStatsFetcher, bool, error) {
	if err := cfg.validate(); err != nil {
		return nil, true, err
	}
	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses:    cfg.ElasticsearchURL,
		Username:     cfg.ElasticsearchUserName,
		Password:     cfg.ElasticsearchPassword,
		APIKey:       cfg.ElasticsearchAPIKey,
		DisableRetry: true,
	})
	if err != nil {
		return nil, false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ElasticsearchTimeout)
	defer cancel()
	ok, err := client.Ping().Do(ctx)
	if err != nil || !ok {
		return nil, false, err
	}
	return elasticsearchStatsFetcher{elasticsearchTelemetryConfig: cfg, client: client}, true, nil
}

func (esf elasticsearchStatsFetcher) FetchStats(ctx context.Context, from, to time.Time) (map[string]float64, error) {
	var filters []types.Query
	if esf.FilterClusterName != "" {
		filters = append(filters, types.Query{
			Term: map[string]types.TermQuery{
				"labels.orchestrator_cluster_name": {
					Value: esf.FilterClusterName,
				},
			},
		})
	}
	if esf.FilterProjectID != "" {
		filters = append(filters, types.Query{
			Term: map[string]types.TermQuery{
				"labels.project_id": {
					Value: esf.FilterProjectID,
				},
			},
		})
	}
	filters = append(filters, types.Query{
		Range: map[string]types.RangeQuery{
			"@timestamp": types.DateRangeQuery{
				Gte: some.String(from.Format("2006-01-02T15:04:05.000Z")),
				Lte: some.String(to.Format("2006-01-02T15:04:05.000Z")),
			},
		},
	})
	aggs := make(map[string]types.Aggregations, len(esf.Metrics))
	for _, m := range esf.Metrics {
		if agg, ok := knownMetricsAggregations[m]; ok {
			for k, v := range agg.Aggregations(m) {
				aggs[k] = v
			}
		}
	}
	request := search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Filter: filters,
			},
		},
		Aggregations: aggs,
		Size:         some.Int(0),
	}
	ctx, cancel := context.WithTimeout(ctx, esf.ElasticsearchTimeout)
	defer cancel()
	resp, err := esf.client.Search().Index(esf.ElasticsearchIndex).Request(&request).Do(ctx)
	if err != nil {
		return nil, err
	}
	res := make(map[string]float64, len(resp.Aggregations))
	for _, m := range esf.Metrics {
		if agg, ok := knownMetricsAggregations[m]; ok {
			if v := agg.Value(m, resp.Aggregations); v != nil {
				res[m] = float64(*v)
			}
		}
	}
	return res, nil
}
