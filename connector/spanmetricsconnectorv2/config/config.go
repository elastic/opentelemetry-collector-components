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

package config // import "github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/config"

import (
	"errors"
	"fmt"

	"github.com/lightstep/go-expohisto/structure"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

const (
	// defaultExponentialHistogramMaxSize is the default maximum number
	// of buckets per positive or negative number range. 160 buckets
	// default supports a high-resolution histogram able to cover a
	// long-tail latency distribution from 1ms to 100s with a relative
	// error of less than 5%.
	// Ref: https://opentelemetry.io/docs/specs/otel/metrics/sdk/#base2-exponential-bucket-histogram-aggregation
	defaultExponentialHistogramMaxSize = 160

	defaultSumAndCountSumSuffix   = ".sum"
	defaultSumAndCountCountSuffix = ".count"
)

var defaultHistogramBuckets = []float64{
	2, 4, 6, 8, 10, 50, 100, 200, 400, 800, 1000, 1400, 2000, 5000, 10_000, 15_000,
}

type MetricUnit string

const (
	MetricUnitNs MetricUnit = "ns"
	MetricUnitUs MetricUnit = "us"
	MetricUnitMs MetricUnit = "ms"
	MetricUnitS  MetricUnit = "s"
)

// Config for the connector. The connector can convert all signal types to metrics.
//
// For spans, the connector can count the number of span events or aggregate them
// based on their durations in histograms, summaries, or as 2 sum metrics.
//
// For all other event types, the connector can count the number of events.
//
// All metrics produced by the connector are in delta temporality.
type Config struct {
	Spans      []MetricInfo `mapstructure:"spans"`
	Datapoints []MetricInfo `mapstructure:"datapoints"`
	Logs       []MetricInfo `mapstructure:"logs"`
}

var _ confmap.Unmarshaler = (*Config)(nil)

func (c *Config) Validate() error {
	if len(c.Spans) == 0 && len(c.Datapoints) == 0 && len(c.Logs) == 0 {
		return fmt.Errorf("no configuration provided, at least one should be specified")
	}
	// TODO: Add validation for the struct
	return nil
}

// Unmarshal with custom logic to set default values.
// This is necessary to ensure that default metrics are
// not configured if the user has specified any custom metrics.
func (c *Config) Unmarshal(componentParser *confmap.Conf) error {
	if componentParser == nil {
		// Nothing to do if there is no config given.
		return nil
	}
	if err := componentParser.Unmarshal(c, confmap.WithIgnoreUnused()); err != nil {
		return err
	}
	for i, info := range c.Spans {
		if info.Unit == "" {
			info.Unit = MetricUnitMs
		}
		if info.noAggregatorDefined() {
			info.Counter = &Counter{}
		}
		if info.Histogram.Explicit != nil {
			// Add default buckets if explicit histogram is defined
			if len(info.Histogram.Explicit.Buckets) == 0 {
				info.Histogram.Explicit.Buckets = defaultHistogramBuckets[:]
			}
		}
		if info.Histogram.Exponential != nil {
			if info.Histogram.Exponential.MaxSize == 0 {
				info.Histogram.Exponential.MaxSize = defaultExponentialHistogramMaxSize
			}
		}
		if info.SumAndCount != nil {
			if info.SumAndCount.SumSuffix == "" {
				info.SumAndCount.SumSuffix = defaultSumAndCountSumSuffix
			}
			if info.SumAndCount.CountSuffix == "" {
				info.SumAndCount.CountSuffix = defaultSumAndCountCountSuffix
			}
		}
		c.Spans[i] = info
	}
	for i, info := range c.Datapoints {
		if info.Counter == nil {
			info.Counter = &Counter{}
		}
		c.Datapoints[i] = info
	}
	for i, info := range c.Logs {
		if info.Counter == nil {
			info.Counter = &Counter{}
		}
		c.Logs[i] = info
	}
	return nil
}

type SpanMetricInfo struct {
	MetricInfo  `mapstructure:",squash"`
	Unit        MetricUnit   `mapstructure:"unit"`
	Histogram   Histogram    `mapstructure:"histogram"`
	Summary     *Summary     `mapstructure:"summary"`
	SumAndCount *SumAndCount `mapstructure:"sum_and_count"`
}

// noAggregatorDefined returns true if none of the required
// aggregators are defined for a MetricInfo.
func (mi *SpanMetricInfo) noAggregatorDefined() bool {
	return mi.Histogram.Exponential == nil &&
		mi.Histogram.Explicit == nil &&
		mi.Summary == nil &&
		mi.SumAndCount == nil &&
		mi.Counter == nil
}

// MetricInfo for a data type
type MetricInfo struct {
	Name        string `mapstructure:"name"`
	Description string `mapstructure:"description"`
	// EphemeralResourceAttribute (experimental) adds a randomly generated
	// ID as a resource attribute. The random ID is unique to a running
	// collector instance.
	EphemeralResourceAttribute bool `mapstructure:"ephemeral_resource_attribute"`
	// IncludeResourceAttributes is a list of resource attributes that
	// needs to be included in the generated metric. If no resource
	// attribute is included in the list then all attributes are included.
	// Note that configuring this setting might cause the produced metric
	// to lose its identity or cause identity conflict. Check out the
	// `ephemeral_resource_attribute`.
	IncludeResourceAttributes []Attribute  `mapstructure:"include_resource_attributes"`
	Attributes                []Attribute  `mapstructure:"attributes"`
	Counter                   *Counter     `mapstructure:"counter"`
	Statements                Statements   `mapstructure:"statements"`
	Unit                      MetricUnit   `mapstructure:"unit"`
	Histogram                 Histogram    `mapstructure:"histogram"`
	Summary                   *Summary     `mapstructure:"summary"`
	SumAndCount               *SumAndCount `mapstructure:"sum_and_count"`
}

// noAggregatorDefined returns true if none of the required
// aggregators are defined for a MetricInfo.
func (mi *MetricInfo) noAggregatorDefined() bool {
	return mi.Histogram.Exponential == nil &&
		mi.Histogram.Explicit == nil &&
		mi.Summary == nil &&
		mi.SumAndCount == nil &&
		mi.Counter == nil
}

// isEqual checks if two metric have a same identity. Identity of a
// metric is defined by name and attribute.
func (mi MetricInfo) isEqual(other MetricInfo) bool {
	if mi.Name != other.Name {
		return false
	}
	if len(mi.Attributes) != len(other.Attributes) {
		return false
	}
	if len(mi.Attributes) == 0 {
		return true
	}
	// Validate attribues equality
	keyMap := make(map[string]Attribute)
	for _, attr := range mi.Attributes {
		keyMap[attr.Key] = attr
	}

	for _, otherAttr := range other.Attributes {
		if _, ok := keyMap[otherAttr.Key]; !ok {
			return false
		}
	}
	return true
}

func (i *MetricInfo) validateAttributes() error {
	tmp := pcommon.NewValueEmpty()
	duplicate := map[string]struct{}{}
	for _, attr := range i.Attributes {
		if _, ok := duplicate[attr.Key]; ok {
			return fmt.Errorf("duplicate key found in attributes config: %s", attr.Key)
		}
		if attr.Key == "" {
			return fmt.Errorf("attribute key missing")
		}
		if err := tmp.FromRaw(attr.DefaultValue); err != nil {
			return fmt.Errorf("invalid default value specified for attribute %s", attr.Key)
		}
		duplicate[attr.Key] = struct{}{}
	}
	return nil
}

func (mi *MetricInfo) validateHistogram() error {
	if mi.Histogram.Explicit != nil {
		if len(mi.Histogram.Explicit.Buckets) == 0 {
			return errors.New("histogram buckets missing")
		}
	}
	if mi.Histogram.Exponential != nil {
		if _, err := structure.NewConfig(
			structure.WithMaxSize(mi.Histogram.Exponential.MaxSize),
		).Validate(); err != nil {
			return err
		}
	}
	return nil
}

func validateSpanOTTLStatement(s Statements) error {
	parser, err := ottlspan.NewParser(
		ottlfuncs.StandardFuncs[ottlspan.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()},
	)
	if err != nil {
		return fmt.Errorf("failed to validate span statements: %w", err)
	}
	if _, err := parser.ParseStatement(s.Count); err != nil {
		return fmt.Errorf("failed to parse span count statement: %w", err)
	}
	if _, err := parser.ParseStatement(s.Value); err != nil {
		return fmt.Errorf("failed to parse span value statement: %w", err)
	}
	return nil
}

func validateDatapointOTTLStatement(s Statements) error {
	parser, err := ottldatapoint.NewParser(
		ottlfuncs.StandardFuncs[ottldatapoint.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()},
	)
	if err != nil {
		return fmt.Errorf("failed to validate datapoint statements: %w", err)
	}
	if _, err := parser.ParseStatement(s.Count); err != nil {
		return fmt.Errorf("failed to parse datapoint count statement: %w", err)
	}
	if _, err := parser.ParseStatement(s.Value); err != nil {
		return fmt.Errorf("failed to parse datapoint value statement: %w", err)
	}
	return nil
}

func validateLogOTTLStatement(s Statements) error {
	parser, err := ottllog.NewParser(
		ottlfuncs.StandardFuncs[ottllog.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()},
	)
	if err != nil {
		return fmt.Errorf("failed to validate log statements: %w", err)
	}
	if _, err := parser.ParseStatement(s.Count); err != nil {
		return fmt.Errorf("failed to parse log count statement: %w", err)
	}
	if _, err := parser.ParseStatement(s.Value); err != nil {
		return fmt.Errorf("failed to parse log value statement: %w", err)
	}
	return nil
}

type Attribute struct {
	Key          string `mapstructure:"key"`
	DefaultValue any    `mapstructure:"default_value"`
}

// Counter counts the number of spans
type Counter struct{}

type Histogram struct {
	Explicit    *ExplicitHistogram    `mapstructure:"explicit"`
	Exponential *ExponentialHistogram `mapstructure:"exponential"`
}

type ExplicitHistogram struct {
	Buckets []float64 `mapstructure:"buckets"`
}

type ExponentialHistogram struct {
	MaxSize int32 `mapstructure:"max_size"`
}

type Summary struct{}

// SunAndCount aggregate spans as 2 cummulative sum metric with delta temporality.
// The configs allow adding suffixes to the metric names, the suffix defaults to
// `.sum` for sum metric and `.count` for count metric.
type SumAndCount struct {
	SumSuffix   string `mapstructure:"sum_suffix"`
	CountSuffix string `mapstructure:"count_suffix"`
}

// Statements are OTTL statements that could extract the relevant value from
// signals to be recorded as a metric by the connector.
type Statements struct {
	Value string `mapstructure:"value"`
	Count string `mapstructure:"count"`
}
