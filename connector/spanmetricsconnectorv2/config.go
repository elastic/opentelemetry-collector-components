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

package spanmetricsconnectorv2 // import "github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2"

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/confmap"
)

const (
	defaultMetricNameSpans = "trace.span.duration"
	defaultMetricDescSpans = "Observed span duration."
)

var (
	defaultHistogramBucketsMs = [...]float64{2, 4, 6, 8, 10, 50, 100, 200, 400, 800, 1000, 1400, 2000, 5000, 10_000, 15_000}
)

type MetricUnit string

const (
	MetricUnitMs MetricUnit = "ms"
	MetricUnitS  MetricUnit = "s"
)

// Config for the connector
type Config struct {
	Spans map[string]MetricInfo `mapstructure:"spans"`
}

// MetricInfo for a data type
type MetricInfo struct {
	Description string            `mapstructure:"description"`
	Attributes  []AttributeConfig `mapstructure:"attributes"`
	Unit        MetricUnit        `mapstructure:"unit"`
	Histogram   HistogramConfig   `mapstructure:"histogram"`
}

type AttributeConfig struct {
	Key          string `mapstructure:"key"`
	DefaultValue any    `mapstructure:"default_value"`
}

type HistogramConfig struct {
	Explicit *ExplicitHistogramConfig `mapstructure:"explicit"`
}

type ExplicitHistogramConfig struct {
	Buckets []float64 `mapstructure:"buckets"`
}

func (c *Config) Validate() error {
	for name, info := range c.Spans {
		if name == "" {
			return errors.New("spans: metric name missing")
		}
		if info.Unit == "" {
			return errors.New("spans: metric unit missing")
		}
		if err := info.validateHistogram(); err != nil {
			return fmt.Errorf("sapn attributes: metric %q: %w", name, err)
		}
		if err := info.validateAttributes(); err != nil {
			return fmt.Errorf("spans attributes: metric %q: %w", name, err)
		}
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
	for _, attr := range i.Attributes {
		if attr.Key == "" {
			return fmt.Errorf("attribute key missing")
		}
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
			info.Histogram.Explicit = &ExplicitHistogramConfig{}
		}
		if len(info.Histogram.Explicit.Buckets) == 0 {
			info.Histogram.Explicit.Buckets = defaultExplicitHistogramBuckets(info.Unit)
		}
		c.Spans[k] = info
	}
	return nil
}

func defaultSpansConfig() map[string]MetricInfo {
	return map[string]MetricInfo{
		defaultMetricNameSpans: {
			Description: defaultMetricDescSpans,
			Unit:        MetricUnitMs,
			Histogram: HistogramConfig{
				Explicit: &ExplicitHistogramConfig{
					Buckets: defaultExplicitHistogramBuckets(MetricUnitMs),
				},
			},
		},
	}
}

func defaultExplicitHistogramBuckets(unit MetricUnit) []float64 {
	switch unit {
	case MetricUnitMs:
		return defaultHistogramBucketsMs[:]
	case MetricUnitS:
		buckets := make([]float64, len(defaultHistogramBucketsMs))
		for i := 0; i < len(buckets); i++ {
			buckets[i] /= float64(time.Second.Milliseconds())
		}
		return buckets
	}
	return nil
}
