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

// knownMetricsAggregations corresponds to know basic-level metrics from
// https://opentelemetry.io/docs/collector/internal-telemetry/#basic-level-metrics.
var knownMetricsAggregations map[string]types.Aggregations = map[string]types.Aggregations{
	"otelcol_exporter_enqueue_failed_log_records": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_exporter_enqueue_failed_log_records")},
	},
	"otelcol_exporter_enqueue_failed_metric_points": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_exporter_enqueue_failed_metric_points")},
	},
	"otelcol_exporter_enqueue_failed_spans": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_exporter_enqueue_failed_spans")},
	},
	"otelcol_exporter_queue_capacity": {
		Avg: &types.AverageAggregation{Field: some.String("otelcol_exporter_queue_capacity")},
	},
	"otelcol_exporter_queue_size": {
		Avg: &types.AverageAggregation{Field: some.String("otelcol_exporter_queue_size")},
	},
	"otelcol_exporter_send_failed_log_records": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_exporter_send_failed_log_records")},
	},
	"otelcol_exporter_send_failed_metric_points": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_exporter_send_failed_metric_points")},
	},
	"otelcol_exporter_send_failed_spans": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_exporter_send_failed_spans")},
	},
	"otelcol_exporter_sent_log_records": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_exporter_sent_log_records")},
	},
	"otelcol_exporter_sent_metric_points": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_exporter_sent_metric_points")},
	},
	"otelcol_exporter_sent_spans": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_exporter_sent_spans")},
	},
	"otelcol_process_cpu_seconds": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_process_cpu_seconds")},
	},
	"otelcol_process_memory_rss": {
		Avg: &types.AverageAggregation{Field: some.String("otelcol_process_memory_rss")},
	},
	"otelcol_process_runtime_heap_alloc_bytes": {
		Avg: &types.AverageAggregation{Field: some.String("otelcol_process_runtime_heap_alloc_bytes")},
	},
	"otelcol_process_runtime_total_alloc_bytes": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_process_runtime_total_alloc_bytes")},
	},
	"otelcol_process_runtime_total_sys_memory_bytes": {
		Avg: &types.AverageAggregation{Field: some.String("otelcol_process_runtime_total_sys_memory_bytes")},
	},
	"otelcol_process_uptime": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_process_uptime")},
	},
	"otelcol_processor_batch_batch_send_size": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_processor_batch_batch_send_size")},
	},
	"otelcol_processor_batch_batch_size_trigger_send": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_processor_batch_batch_size_trigger_send")},
	},
	"otelcol_processor_batch_metadata_cardinality": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_processor_batch_metadata_cardinality")},
	},
	"otelcol_processor_batch_timeout_trigger_send": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_processor_batch_timeout_trigger_send")},
	},
	"otelcol_processor_incoming_items": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_processor_incoming_items")},
	},
	"otelcol_processor_outgoing_items": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_processor_outgoing_items")},
	},
	"otelcol_receiver_accepted_log_records": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_receiver_accepted_log_records")},
	},
	"otelcol_receiver_accepted_metric_points": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_receiver_accepted_metric_points")},
	},
	"otelcol_receiver_accepted_spans": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_receiver_accepted_spans")},
	},
	"otelcol_receiver_refused_log_records": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_receiver_refused_log_records")},
	},
	"otelcol_receiver_refused_metric_points": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_receiver_refused_metric_points")},
	},
	"otelcol_receiver_refused_spans": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_receiver_refused_spans")},
	},
	"otelcol_scraper_errored_metric_points": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_scraper_errored_metric_points")},
	},
	"otelcol_scraper_scraped_metric_points": {
		Sum: &types.SumAggregation{Field: some.String("otelcol_scraper_scraped_metric_points")},
	},
}

type elasticsearchTelemetryConfig TelemetryConfig

func (cfg *elasticsearchTelemetryConfig) validate() error {
	if len(cfg.ElasticsearchURL) == 0 {
		return errors.New("remote stats will be ignored because of empty telemetry Elasticsearch URL")
	}
	if cfg.ElasticsearchAPIKey == "" && (cfg.ElasticsearchUserName == "" || cfg.ElasticsearchPassword == "") {
		return errors.New("remote stats will be ignored because of empty telemetry Elasticsearch API key and username or password")
	}
	if cfg.ElasticsearchIndex == "" {
		return errors.New("remote stats will be ignored because of empty telemetry Elasticsearch search index")
	}
	// Use the default list of metrics if empty list was provided.
	if len(cfg.Metrics) == 0 {
		cfg.Metrics = []string{
			"otelcol_process_cpu_seconds",
			"otelcol_process_memory_rss",
			"otelcol_process_runtime_total_alloc_bytes",
			"otelcol_process_runtime_total_sys_memory_bytes",
			"otelcol_process_uptime",
		}
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

func newElasticsearchStatsFetcher(cfg elasticsearchTelemetryConfig) (remoteStatsFetcher, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: cfg.ElasticsearchURL,
		Username:  cfg.ElasticsearchUserName,
		Password:  cfg.ElasticsearchPassword,
		APIKey:    cfg.ElasticsearchAPIKey,
	})
	if err != nil {
		return nil, err
	}
	return elasticsearchStatsFetcher{elasticsearchTelemetryConfig: cfg, client: client}, nil
}

func (esf elasticsearchStatsFetcher) FetchStats(ctx context.Context, from, to time.Time) (map[string]float64, error) {
	var filters []types.Query
	if esf.FilterCluster != "" {
		filters = append(filters, types.Query{
			Term: map[string]types.TermQuery{
				"labels.cluster": {
					Value: esf.FilterCluster,
				},
			},
		})
	}
	if esf.FilterProject != "" {
		filters = append(filters, types.Query{
			Term: map[string]types.TermQuery{
				"labels.project": {
					Value: esf.FilterProject,
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
			aggs[m] = agg
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
	resp, err := esf.client.Search().Index(esf.ElasticsearchIndex).Request(&request).Do(ctx)
	if err != nil {
		return nil, err
	}
	res := make(map[string]float64, len(resp.Aggregations))
	for k, aggr := range resp.Aggregations {
		switch v := aggr.(type) {
		case *types.SumAggregate:
			res[k+"_sum"] = float64(*v.Value)
		case *types.AvgAggregate:
			res[k+"_avg"] = float64(*v.Value)
		}
	}
	return res, nil
}
