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
	"time"

	lsmconfig "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	signaltometricsconfig "github.com/open-telemetry/opentelemetry-collector-contrib/connector/signaltometricsconnector/config"
	"go.opentelemetry.io/collector/component"
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
	}
	return lsmConfig
}

func (cfg Config) signaltometricsConfig() *signaltometricsconfig.Config {
	// serviceResourceAttributes is the resource attributes included in
	// service-level aggregated metrics.
	serviceResourceAttributes := []signaltometricsconfig.Attribute{
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
	}

	// transactionResourceAttributes is the resource attributes included
	// in transaction group-level aggregated metrics.
	transactionResourceAttributes := append([]signaltometricsconfig.Attribute{
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
	}, serviceResourceAttributes...)

	serviceSummaryAttributes := []signaltometricsconfig.Attribute{{
		Key:          "metricset.name",
		DefaultValue: "service_summary",
	}}

	serviceTransactionAttributes := []signaltometricsconfig.Attribute{
		{Key: "transaction.root"},
		{Key: "transaction.type"},
		{Key: "metricset.name", DefaultValue: "service_transaction"},
	}

	transactionAttributes := []signaltometricsconfig.Attribute{
		{Key: "transaction.root"},
		{Key: "transaction.name"},
		{Key: "transaction.type"},
		{Key: "transaction.result"},
		{Key: "event.outcome"},
		{Key: "metricset.name", DefaultValue: "transaction"},
	}

	serviceDestinationAttributes := []signaltometricsconfig.Attribute{
		{Key: "span.name"},
		{Key: "event.outcome"},
		{Key: "service.target.type"},
		{Key: "service.target.name"},
		{Key: "span.destination.service.resource"},
		{Key: "metricset.name", DefaultValue: "service_destination"},
	}

	// TODO switch to exponential histogram once we have optimised
	// exponential histogram merging.
	transactionDurationHistogram := &signaltometricsconfig.Histogram{
		Buckets: []float64{
			5000, 10000, 25000, 50000, 75000, 100000, 250000, 500000,
			750000, 1000000, 2500000, 5000000, 7500000, 10000000,
		},
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
			IncludeResourceAttributes: serviceResourceAttributes,
			Attributes:                serviceSummaryAttributes,
			Sum:                       &signaltometricsconfig.Sum{Value: "1"},
		}},

		Datapoints: []signaltometricsconfig.MetricInfo{{
			Name:                      "service_summary",
			IncludeResourceAttributes: serviceResourceAttributes,
			Attributes:                serviceSummaryAttributes,
			Sum:                       &signaltometricsconfig.Sum{Value: "1"},
		}},

		Spans: []signaltometricsconfig.MetricInfo{{
			Name:                      "service_summary",
			IncludeResourceAttributes: serviceResourceAttributes,
			Attributes:                serviceSummaryAttributes,
			Sum: &signaltometricsconfig.Sum{
				Value: "Int(AdjustedCount())",
			},
		}, {
			Name:                      "transaction.duration.histogram",
			Description:               "APM service transaction aggregated metrics as histogram",
			IncludeResourceAttributes: serviceResourceAttributes,
			Attributes: append(serviceTransactionAttributes[:], signaltometricsconfig.Attribute{
				Key:          "elasticsearch.mapping.hints",
				DefaultValue: []any{"_doc_count"},
			}),
			Unit:      "us",
			Histogram: transactionDurationHistogram,
		}, {
			Name:                      "transaction.duration.summary",
			Description:               "APM service transaction aggregated metrics as summary",
			IncludeResourceAttributes: serviceResourceAttributes,
			Attributes: append(serviceTransactionAttributes[:], signaltometricsconfig.Attribute{
				Key:          "elasticsearch.mapping.hints",
				DefaultValue: []any{"aggregate_metric_double"},
			}),
			Unit:      "us",
			Histogram: transactionDurationSummaryHistogram,
		}, {
			Name:                      "transaction.duration.histogram",
			Description:               "APM transaction aggregated metrics as histogram",
			IncludeResourceAttributes: transactionResourceAttributes,
			Attributes: append(transactionAttributes[:], signaltometricsconfig.Attribute{
				Key:          "elasticsearch.mapping.hints",
				DefaultValue: []any{"_doc_count"},
			}),
			Unit:      "us",
			Histogram: transactionDurationHistogram,
		}, {
			Name:                      "transaction.duration.summary",
			Description:               "APM transaction aggregated metrics as summary",
			IncludeResourceAttributes: transactionResourceAttributes,
			Attributes: append(transactionAttributes[:], signaltometricsconfig.Attribute{
				Key:          "elasticsearch.mapping.hints",
				DefaultValue: []any{"aggregate_metric_double"},
			}),
			Unit:      "us",
			Histogram: transactionDurationSummaryHistogram,
		}, {
			Name:                      "span.destination.service.response_time.sum.us",
			Description:               "APM span destination metrics",
			IncludeResourceAttributes: serviceResourceAttributes,
			Attributes:                serviceDestinationAttributes,
			Unit:                      "us",
			Sum: &signaltometricsconfig.Sum{
				Value: "Double(Microseconds(end_time - start_time))",
			},
		}, {
			Name:                      "span.destination.service.response_time.count",
			Description:               "APM span destination metrics",
			IncludeResourceAttributes: serviceResourceAttributes,
			Attributes:                serviceDestinationAttributes,
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
			IncludeResourceAttributes: serviceResourceAttributes,
			Attributes: append(serviceTransactionAttributes[:], signaltometricsconfig.Attribute{
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
			IncludeResourceAttributes: serviceResourceAttributes,
			Attributes: append(serviceTransactionAttributes[:], signaltometricsconfig.Attribute{
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
