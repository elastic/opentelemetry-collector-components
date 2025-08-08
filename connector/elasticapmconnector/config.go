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

package elasticapmconnector // import "github.com/elastic/opentelemetry-collector-components/connector/elasticapmconnector"

import (
	"fmt"
	"slices"
	"time"

	signaltometricsconfig "github.com/open-telemetry/opentelemetry-collector-contrib/connector/signaltometricsconnector/config"
	"go.opentelemetry.io/collector/component"

	lsmconfig "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
)

var _ component.Config = (*Config)(nil)

var defaultIntervals []time.Duration = []time.Duration{
	time.Minute,
	10 * time.Minute,
	60 * time.Minute,
}

type Config struct {
	// Aggregation holds configuration related to aggregation of Elastic APM
	// metrics from other signals.
	Aggregation *AggregationConfig `mapstructure:"aggregation"`

	// CustomResourceAttributes define a list of resource attributes that will
	// be added to all the aggregated metrics as optional attributes i.e. the
	// attribute will be added to the aggregated metrics if they are present in
	// the incoming signal, otherwise, the attribute will be ignored.
	//
	// NOTE: any custom attributes should have a bounded and preferably low
	// cardinality to be performant.
	CustomResourceAttributes []string `mapstructure:"custom_resource_attributes"`

	// CustomSpanAttributes define a list of span attributes that will be added
	// to the aggregated `service_transaction`, `transaction`, and `span_destination`
	// metrics as optional attributes i.e. the attribute will be added to the
	// aggregated metrics if they are present in the incoming signal, otherwise,
	// the attribute will be ignored.
	//
	// NOTE: any custom attributes should have a bounded and preferably low
	// cardinality to be performant.
	CustomSpanAttributes []string `mapstructure:"custom_span_attributes"`
}

type AggregationConfig struct {
	// Directory holds a path to the directory that is used for persisting
	// aggregation state to disk. If Directory is empty, in-memory storage
	// is used.
	Directory string `mapstructure:"directory"`

	// MetadataKeys holds a list of client.Metadata keys that will be
	// propagated through to aggregated metrics. Only the listed metadata
	// keys will be propagated.
	//
	// Entries are case-insensitive, and duplicated entries will trigger
	// a validation error.
	MetadataKeys []string `mapstructure:"metadata_keys"`

	// Intervals holds an optional list of time intervals that the processor
	// will aggregate over. The interval duration must be in increasing
	// order and must be a factor of the smallest interval duration.
	// The default aggregation intervals are 1m, 10m and 60m.
	//
	// NOTE: these intervals should only be overridden for testing purposes when
	// faster processor feedback is required. The default intervals should be preferred
	// in all other cases -- using this configuration may lead to invalid behavior,
	// and will not be supported.
	Intervals []time.Duration `mapstructure:"intervals"`

	// Limits holds optional cardinality limits for aggregated metrics
	Limits AggregationLimitConfig `mapstructure:"limits"`
}

type AggregationLimitConfig struct {
	// ResourceLimit defines the max cardinality of resources
	ResourceLimit LimitConfig `mapstructure:"resource"`

	// ScopeLimit defines the max cardinality of scopes within a resource
	ScopeLimit LimitConfig `mapstructure:"scope"`

	// MetricLimit defines the max cardinality of metrics within a scope
	MetricLimit LimitConfig `mapstructure:"metric"`

	// DatapointLimit defines the max cardinality of datapoints within a metric
	DatapointLimit LimitConfig `mapstructure:"datapoint"`
}

type LimitConfig struct {
	MaxCardinality int64 `mapstructure:"max_cardinality"`
}

func (cfg Config) Validate() error {
	lsmConfig := cfg.lsmConfig()
	return lsmConfig.Validate()
}

func (cfg Config) lsmConfig() *lsmconfig.Config {
	intervals := defaultIntervals
	if cfg.Aggregation != nil && len(cfg.Aggregation.Intervals) != 0 {
		intervals = cfg.Aggregation.Intervals
	}
	intervalsConfig := make([]lsmconfig.IntervalConfig, 0, len(intervals))
	for _, i := range intervals {
		intervalsConfig = append(intervalsConfig, lsmconfig.IntervalConfig{
			Duration: i,
			Statements: []string{
				fmt.Sprintf(`set(attributes["metricset.interval"], "%dm")`, int(i.Minutes())),
				fmt.Sprintf(`set(attributes["data_stream.dataset"], Concat([attributes["metricset.name"], "%dm"], "."))`, int(i.Minutes())),
				`set(attributes["processor.event"], "metric")`,
			},
		})
	}

	lsmConfig := &lsmconfig.Config{
		Intervals:                      intervalsConfig,
		ExponentialHistogramMaxBuckets: 160,
	}

	if cfg.Aggregation != nil {
		lsmConfig.Directory = cfg.Aggregation.Directory
		lsmConfig.MetadataKeys = cfg.Aggregation.MetadataKeys
		lsmConfig.ResourceLimit = lsmconfig.LimitConfig{
			MaxCardinality: cfg.Aggregation.Limits.ResourceLimit.MaxCardinality,
			Overflow: lsmconfig.OverflowConfig{
				Attributes: []lsmconfig.Attribute{
					{Key: "service.name", Value: "_other"}, // Specific attribute required for APU UI compatibility
					{Key: "overflow", Value: "resource"},
				},
			},
		}
		lsmConfig.ScopeLimit = lsmconfig.LimitConfig{
			MaxCardinality: cfg.Aggregation.Limits.ScopeLimit.MaxCardinality,
			Overflow: lsmconfig.OverflowConfig{
				Attributes: []lsmconfig.Attribute{
					{Key: "overflow", Value: "scope"},
				},
			},
		}
		lsmConfig.MetricLimit = lsmconfig.LimitConfig{
			MaxCardinality: cfg.Aggregation.Limits.MetricLimit.MaxCardinality,
			Overflow: lsmconfig.OverflowConfig{
				Attributes: []lsmconfig.Attribute{
					{Key: "overflow", Value: "metric"},
				},
			},
		}
		lsmConfig.DatapointLimit = lsmconfig.LimitConfig{
			MaxCardinality: cfg.Aggregation.Limits.DatapointLimit.MaxCardinality,
			Overflow: lsmconfig.OverflowConfig{
				Attributes: []lsmconfig.Attribute{
					{Key: "overflow", Value: "datapoint"},
				},
			},
		}
	}
	return lsmConfig
}

func (cfg Config) signaltometricsConfig() *signaltometricsconfig.Config {
	// commonResourceAttributes are resource attributes included in
	// all aggregated metrics.
	commonResourceAttributes := append(
		[]signaltometricsconfig.Attribute{
			{Key: "service.name"},
			{Key: "deployment.environment"}, // service.environment
			{Key: "telemetry.sdk.language"}, // service.language.name

			// agent.name is set via elastictraceprocessor for traces,
			// but not for other signals. Default to "unknown" for the
			// others.
			{
				Key:          "agent.name",
				DefaultValue: "unknown",
			},
		}, toSignalToMetricsAttributes(cfg.CustomResourceAttributes)...,
	)

	// serviceSummaryResourceAttributes are resource attributes for service
	// summary metrics.
	serviceSummaryResourceAttributes := slices.Clone(commonResourceAttributes)

	// serviceTransactionResourceAttributes are resource attributes for service
	// transaction metrics
	serviceTransactionResourceAttributes := slices.Clone(commonResourceAttributes)

	// transactionResourceAttributes are resource attributes included
	// in transaction group-level aggregated metrics.
	transactionResourceAttributes := append(
		[]signaltometricsconfig.Attribute{
			{Key: "container.id"},
			{Key: "k8s.pod.name"},
			{Key: "service.version"},
			{Key: "service.instance.id"},     // service.node.name
			{Key: "process.runtime.name"},    // service.runtime.name
			{Key: "process.runtime.version"}, // service.runtime.version
			{Key: "telemetry.sdk.version"},   // service.language.version??
			{Key: "host.name"},
			{Key: "os.type"}, // host.os.platform
			{Key: "faas.instance"},
			{Key: "faas.name"},
			{Key: "faas.version"},
			{Key: "cloud.provider"},
			{Key: "cloud.region"},
			{Key: "cloud.availability_zone"},
			{Key: "cloud.platform"}, // cloud.service.name
			{Key: "cloud.account.id"},
		}, commonResourceAttributes...,
	)

	// spanDestinationResourceAttributes are resource attributes included
	// in service destination aggregations
	spanDestinationResourceAttributes := slices.Clone(commonResourceAttributes)

	serviceSummaryAttributes := []signaltometricsconfig.Attribute{{
		Key:          "metricset.name",
		DefaultValue: "service_summary",
	}}

	serviceTransactionAttributes := append([]signaltometricsconfig.Attribute{
		{Key: "transaction.root"},
		{Key: "transaction.type"},
		{Key: "metricset.name", DefaultValue: "service_transaction"},
	}, toSignalToMetricsAttributes(cfg.CustomSpanAttributes)...)

	transactionAttributes := append([]signaltometricsconfig.Attribute{
		{Key: "transaction.root"},
		{Key: "transaction.name"},
		{Key: "transaction.type"},
		{Key: "transaction.result"},
		{Key: "event.outcome"},
		{Key: "metricset.name", DefaultValue: "transaction"},
	}, toSignalToMetricsAttributes(cfg.CustomSpanAttributes)...)

	spanDestinationAttributes := append([]signaltometricsconfig.Attribute{
		{Key: "span.name"},
		{Key: "event.outcome"},
		{Key: "service.target.type"},
		{Key: "service.target.name"},
		{Key: "span.destination.service.resource"},
		{Key: "metricset.name", DefaultValue: "service_destination"},
	}, toSignalToMetricsAttributes(cfg.CustomSpanAttributes)...)

	transactionDurationHistogram := &signaltometricsconfig.ExponentialHistogram{
		Count: "Int(AdjustedCount())",
		Value: "Microseconds(end_time - start_time)",
	}

	transactionDurationSummaryHistogram := &signaltometricsconfig.Histogram{
		Buckets: []float64{1},
		Count:   "Int(AdjustedCount())",
		Value:   "Microseconds(end_time - start_time)",
	}

	return &signaltometricsconfig.Config{
		Logs: []signaltometricsconfig.MetricInfo{{
			Name:                      "service_summary",
			IncludeResourceAttributes: serviceSummaryResourceAttributes,
			Attributes:                serviceSummaryAttributes,
			Sum:                       &signaltometricsconfig.Sum{Value: "1"},
		}},

		Datapoints: []signaltometricsconfig.MetricInfo{{
			Name:                      "service_summary",
			IncludeResourceAttributes: serviceSummaryResourceAttributes,
			Attributes:                serviceSummaryAttributes,
			Sum:                       &signaltometricsconfig.Sum{Value: "1"},
		}},

		Spans: []signaltometricsconfig.MetricInfo{{
			Name:                      "service_summary",
			IncludeResourceAttributes: serviceSummaryResourceAttributes,
			Attributes:                serviceSummaryAttributes,
			Sum: &signaltometricsconfig.Sum{
				Value: "Int(AdjustedCount())",
			},
		}, {
			Name:                      "transaction.duration.histogram",
			Description:               "APM service transaction aggregated metrics as histogram",
			IncludeResourceAttributes: serviceTransactionResourceAttributes,
			Attributes: append(slices.Clone(serviceTransactionAttributes), signaltometricsconfig.Attribute{
				Key:          "elasticsearch.mapping.hints",
				DefaultValue: []any{"_doc_count"},
			}),
			Unit:                 "us",
			ExponentialHistogram: transactionDurationHistogram,
		}, {
			Name:                      "transaction.duration.summary",
			Description:               "APM service transaction aggregated metrics as summary",
			IncludeResourceAttributes: serviceTransactionResourceAttributes,
			Attributes: append(slices.Clone(serviceTransactionAttributes), signaltometricsconfig.Attribute{
				Key:          "elasticsearch.mapping.hints",
				DefaultValue: []any{"aggregate_metric_double"},
			}),
			Unit:      "us",
			Histogram: transactionDurationSummaryHistogram,
		}, {
			Name:                      "transaction.duration.histogram",
			Description:               "APM transaction aggregated metrics as histogram",
			IncludeResourceAttributes: transactionResourceAttributes,
			Attributes: append(slices.Clone(transactionAttributes), signaltometricsconfig.Attribute{
				Key:          "elasticsearch.mapping.hints",
				DefaultValue: []any{"_doc_count"},
			}),
			Unit:                 "us",
			ExponentialHistogram: transactionDurationHistogram,
		}, {
			Name:                      "transaction.duration.summary",
			Description:               "APM transaction aggregated metrics as summary",
			IncludeResourceAttributes: transactionResourceAttributes,
			Attributes: append(slices.Clone(transactionAttributes), signaltometricsconfig.Attribute{
				Key:          "elasticsearch.mapping.hints",
				DefaultValue: []any{"aggregate_metric_double"},
			}),
			Unit:      "us",
			Histogram: transactionDurationSummaryHistogram,
		}, {
			Name:                      "span.destination.service.response_time.sum.us",
			Description:               "APM span destination metrics",
			IncludeResourceAttributes: spanDestinationResourceAttributes,
			Attributes:                spanDestinationAttributes,
			Unit:                      "us",
			Sum: &signaltometricsconfig.Sum{
				Value: "Double(Microseconds(end_time - start_time))",
			},
		}, {
			Name:                      "span.destination.service.response_time.count",
			Description:               "APM span destination metrics",
			IncludeResourceAttributes: spanDestinationResourceAttributes,
			Attributes:                spanDestinationAttributes,
			Sum: &signaltometricsconfig.Sum{
				Value: "Int(AdjustedCount())",
			},
		}, {
			// event.success_count is populated using 2 metric definition with different conditions
			// and value for the histogram bucket based on event outcome. Both metric definition
			// are created using same name and attribute and will result in a single histogram.
			// We use mapping hint of aggregate_metric_double, so, only the sum and the count
			// values are required and the actual histogram bucket is ignored.
			Name:                      "event.success_count",
			Description:               "Success count as a metric for service transaction",
			IncludeResourceAttributes: serviceTransactionResourceAttributes,
			Attributes: append(slices.Clone(serviceTransactionAttributes), signaltometricsconfig.Attribute{
				Key:          "elasticsearch.mapping.hints",
				DefaultValue: []any{"aggregate_metric_double"},
			}),
			Conditions: []string{
				`attributes["event.outcome"] != nil and attributes["event.outcome"] == "success"`,
			},
			Unit: "us",
			Histogram: &signaltometricsconfig.Histogram{
				Buckets: []float64{1},
				Count:   "Int(AdjustedCount())",
				Value:   "Int(AdjustedCount())",
			},
		}, {
			Name:                      "event.success_count",
			Description:               "Success count as a metric for service transaction",
			IncludeResourceAttributes: serviceTransactionResourceAttributes,
			Attributes: append(slices.Clone(serviceTransactionAttributes), signaltometricsconfig.Attribute{
				Key:          "elasticsearch.mapping.hints",
				DefaultValue: []any{"aggregate_metric_double"},
			}),
			Conditions: []string{
				`attributes["event.outcome"] != nil and attributes["event.outcome"] != "success"`,
			},
			Unit: "us",
			Histogram: &signaltometricsconfig.Histogram{
				Buckets: []float64{0},
				Count:   "Int(AdjustedCount())",
				Value:   "Double(0)",
			},
		}},
	}
}

// toSignalToMetricsAttributes converts slice to string to signal to metricsa attributes
// assuming `optional: true` for each attribute.
func toSignalToMetricsAttributes(in []string) []signaltometricsconfig.Attribute {
	attrs := make([]signaltometricsconfig.Attribute, 0, len(in))
	for _, k := range in {
		attrs = append(attrs, signaltometricsconfig.Attribute{
			Key:      k,
			Optional: true,
		})
	}
	return attrs
}
