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
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/metadata"
)

// metricUnitToDivider gives a value that could used to divide the
// nano precision duration to the required unit specified in config.
var metricUnitToDivider = map[MetricUnit]float64{
	MetricUnitMs: float64(time.Millisecond.Nanoseconds()),
	MetricUnitS:  float64(time.Second.Nanoseconds()),
}

// NewFactory returns a ConnectorFactory.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		metadata.Type,
		createDefaultConfig,
		connector.WithTracesToMetrics(createTracesToMetrics, metadata.TracesToMetricsStability),
	)
}

// createDefaultConfig creates the default configuration.
func createDefaultConfig() component.Config {
	return &Config{}
}

// createTracesToMetrics creates a traces to metrics connector based on provided config.
func createTracesToMetrics(
	_ context.Context,
	set connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (connector.Traces, error) {
	c := cfg.(*Config)

	spanMetricDefs := make(map[string]metricDef, len(c.Spans))
	for name, info := range c.Spans {
		md := metricDef{
			desc:        info.Description,
			unitDivider: metricUnitToDivider[info.Unit],
			attrs:       info.Attributes,
			histogram:   info.Histogram,
		}
		spanMetricDefs[name] = md
	}

	return &spanMetrics{
		next:            nextConsumer,
		spansMetricDefs: spanMetricDefs,
	}, nil
}

type metricDef struct {
	desc        string
	unitDivider float64
	attrs       []AttributeConfig
	histogram   HistogramConfig
}
