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

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const (
	defaultMetricNameSpans = "trace.span.duration"
	defaultMetricDescSpans = "Observed span duration."
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

// Config for the connector
type Config struct {
	Spans []MetricInfo `mapstructure:"spans"`
}

// MetricInfo for a data type
type MetricInfo struct {
	Name        string      `mapstructure:"name"`
	Description string      `mapstructure:"description"`
	Attributes  []Attribute `mapstructure:"attributes"`
	Unit        MetricUnit  `mapstructure:"unit"`
	Histogram   Histogram   `mapstructure:"histogram"`
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

type Attribute struct {
	Key          string `mapstructure:"key"`
	DefaultValue any    `mapstructure:"default_value"`
}

type Histogram struct {
	Explicit *ExplicitHistogram `mapstructure:"explicit"`
}

type ExplicitHistogram struct {
	Buckets []float64 `mapstructure:"buckets"`
}

func (c *Config) Validate() error {
	duplicate := make(map[string]MetricInfo)
	for _, info := range c.Spans {
		if old, ok := duplicate[info.Name]; ok && info.isEqual(old) {
			return fmt.Errorf("spans: duplicate configuration found %s", info.Name)
		}
		if info.Name == "" {
			return errors.New("spans: metric name missing")
		}
		if info.Unit == "" {
			return errors.New("spans: metric unit missing")
		}
		if err := info.validateHistogram(); err != nil {
			return fmt.Errorf("spans histogram validation failed: metric %q, %w", info.Name, err)
		}
		if err := info.validateAttributes(); err != nil {
			return fmt.Errorf("spans attributes validation failed: metric %q: %w", info.Name, err)
		}
		duplicate[info.Name] = info
	}
	return nil
}

func (i *MetricInfo) validateHistogram() error {
	if i.Histogram.Explicit == nil {
		return errors.New("histogram definition missing")
	}
	if len(i.Histogram.Explicit.Buckets) == 0 {
		return errors.New("histogram buckets missing")
	}
	return nil
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

var _ confmap.Unmarshaler = (*Config)(nil)

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
	if !componentParser.IsSet("spans") {
		c.Spans = defaultSpansConfig()
		return nil
	}
	for k, info := range c.Spans {
		if info.Unit == "" {
			info.Unit = MetricUnitMs
		}
		if info.Histogram.Explicit == nil {
			info.Histogram.Explicit = &ExplicitHistogram{}
		}
		if len(info.Histogram.Explicit.Buckets) == 0 {
			info.Histogram.Explicit.Buckets = defaultHistogramBuckets[:]
		}
		c.Spans[k] = info
	}
	return nil
}

func defaultSpansConfig() []MetricInfo {
	return []MetricInfo{
		{
			Name:        defaultMetricNameSpans,
			Description: defaultMetricDescSpans,
			Unit:        MetricUnitMs,
			Histogram: Histogram{
				Explicit: &ExplicitHistogram{
					Buckets: defaultHistogramBuckets[:],
				},
			},
		},
	}
}
