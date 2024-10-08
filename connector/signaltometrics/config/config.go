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

package config // import "github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/config"

import (
	"errors"
	"fmt"

	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/ottlget"
	"github.com/lightstep/go-expohisto/structure"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
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
)

var defaultHistogramBuckets = []float64{
	2, 4, 6, 8, 10, 50, 100, 200, 400, 800, 1000, 1400, 2000, 5000, 10_000, 15_000,
}

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
	for _, span := range c.Spans {
		if err := span.validate(); err != nil {
			return fmt.Errorf("failed to validate spans configuration: %w", err)
		}
	}
	for _, dp := range c.Datapoints {
		if err := dp.validate(); err != nil {
			return fmt.Errorf("failed to validate spans configuration: %w", err)
		}
	}
	for _, log := range c.Logs {
		if err := log.validate(); err != nil {
			return fmt.Errorf("failed to validate spans configuration: %w", err)
		}
	}
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
		info.ensureDefaults()
		c.Spans[i] = info
	}
	for i, info := range c.Datapoints {
		info.ensureDefaults()
		c.Datapoints[i] = info
	}
	for i, info := range c.Logs {
		info.ensureDefaults()
		c.Logs[i] = info
	}
	return nil
}

// MetricInfo for a data type
type MetricInfo struct {
	Name        string `mapstructure:"name"`
	Description string `mapstructure:"description"`
	// Unit, if not-empty, will set the unit associated with the metric.
	// See: https://github.com/open-telemetry/opentelemetry-collector/blob/b06236cc794982916cc956f20828b3e18eb33264/pdata/pmetric/generated_metric.go#L72-L81
	Unit string `mapstructure:"unit"`
	// IncludeResourceAttributes is a list of resource attributes that
	// needs to be included in the generated metric. If no resource
	// attribute is included in the list then all attributes are included.
	// Note that configuring this setting might cause the produced metric
	// to lose its identity or cause identity conflict. Check out the
	// `ephemeral_resource_attribute`.
	IncludeResourceAttributes []Attribute           `mapstructure:"include_resource_attributes"`
	Attributes                []Attribute           `mapstructure:"attributes"`
	Explicit                  *ExplicitHistogram    `mapstructure:"histogram"`
	Exponential               *ExponentialHistogram `mapstructure:"exponential_histogram"`
	Summary                   *Summary              `mapstructure:"summary"`
	Sum                       *Sum                  `mapstructure:"sum"`
}

// isEqual checks if two metric have a same identity. Identity of a
// metric is defined by name and attribute.
func (mi *MetricInfo) isEqual(other MetricInfo) bool {
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

// TODO (lahsivjar): Add validation for OTTL statements.
func (mi *MetricInfo) validate() error {
	if mi.Name == "" {
		return errors.New("missing required metric name configuration")
	}
	if err := mi.validateAttributes(); err != nil {
		return fmt.Errorf("attributes validation failed: %w", err)
	}
	if err := mi.validateHistogram(); err != nil {
		return fmt.Errorf("histogram validation failed: %w", err)
	}
	if err := mi.validateSummary(); err != nil {
		return fmt.Errorf("summary validation failed: %w", err)
	}
	if err := mi.validateSum(); err != nil {
		return fmt.Errorf("sum validation failed: %w", err)
	}

	// Exactly one metric should be defined
	var metricsDefinedCount int
	if mi.Explicit != nil {
		metricsDefinedCount++
	}
	if mi.Exponential != nil {
		metricsDefinedCount++
	}
	if mi.Summary != nil {
		metricsDefinedCount++
	}
	if mi.Sum != nil {
		metricsDefinedCount++
	}
	if metricsDefinedCount != 1 {
		return fmt.Errorf("exactly one of the metrics must be defined, %d found", metricsDefinedCount)
	}
	return nil
}

func (mi *MetricInfo) validateAttributes() error {
	tmp := pcommon.NewValueEmpty()
	duplicate := map[string]struct{}{}
	for _, attr := range mi.Attributes {
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
	if mi.Explicit != nil {
		if len(mi.Explicit.Buckets) == 0 {
			return errors.New("histogram buckets missing")
		}
		if mi.Explicit.Value == "" {
			return errors.New("value OTTL statement is required")
		}
	}
	if mi.Exponential != nil {
		if _, err := structure.NewConfig(
			structure.WithMaxSize(mi.Exponential.MaxSize),
		).Validate(); err != nil {
			return err
		}
		if mi.Exponential.Value == "" {
			return errors.New("value OTTL statement is required")
		}
	}
	return nil
}

func (mi *MetricInfo) validateSummary() error {
	if mi.Summary != nil {
		if mi.Summary.Value == "" {
			return errors.New("value OTTL statement is required")
		}
	}
	return nil
}

func (mi *MetricInfo) validateSum() error {
	if mi.Sum != nil {
		if mi.Sum.Value == "" {
			return errors.New("value must be defined for sum metrics")
		}
	}
	return nil
}

func (mi *MetricInfo) ensureDefaults() {
	if mi.Explicit != nil {
		// Add default buckets if explicit histogram is defined
		if len(mi.Explicit.Buckets) == 0 {
			mi.Explicit.Buckets = defaultHistogramBuckets[:]
		}
	}
	if mi.Exponential != nil {
		if mi.Exponential.MaxSize == 0 {
			mi.Exponential.MaxSize = defaultExponentialHistogramMaxSize
		}
	}
}

func validateSpanOTTLStatement(statements []string) error {
	parser, err := ottlspan.NewParser(
		ottlget.CustomFuncs[ottlspan.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()},
	)
	if err != nil {
		return fmt.Errorf("failed to create parser for OTTL spans: %w", err)
	}
	if _, err := parser.ParseStatements(statements); err != nil {
		return fmt.Errorf("failed to parse span OTTL statements: %w", err)
	}
	return nil
}

func validateDatapointOTTLStatement(statements []string) error {
	parser, err := ottldatapoint.NewParser(
		ottlget.CustomFuncs[ottldatapoint.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()},
	)
	if err != nil {
		return fmt.Errorf("failed to create parser for OTTL datapoints: %w", err)
	}
	if _, err := parser.ParseStatements(statements); err != nil {
		return fmt.Errorf("failed to parse datapoint OTTL statements: %w", err)
	}
	return nil
}

func validateLogOTTLStatement(statements []string) error {
	parser, err := ottllog.NewParser(
		ottlget.CustomFuncs[ottllog.TransformContext](),
		component.TelemetrySettings{Logger: zap.NewNop()},
	)
	if err != nil {
		return fmt.Errorf("failed to create parser for OTTL logs: %w", err)
	}
	if _, err := parser.ParseStatements(statements); err != nil {
		return fmt.Errorf("failed to parse log OTTL statements: %w", err)
	}
	return nil
}

type Attribute struct {
	Key          string `mapstructure:"key"`
	DefaultValue any    `mapstructure:"default_value"`
}

type ExplicitHistogram struct {
	Buckets []float64 `mapstructure:"buckets"`
	Count   string    `mapstructure:"count"`
	Value   string    `mapstructure:"value"`
}

type ExponentialHistogram struct {
	MaxSize int32  `mapstructure:"max_size"`
	Count   string `mapstructure:"count"`
	Value   string `mapstructure:"value"`
}

type Summary struct {
	Count string `mapstructure:"count"`
	Value string `mapstructure:"value"`
}

type Sum struct {
	Value string `mapstructure:"value"`
}
