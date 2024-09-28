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
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/config"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/model"
)

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
	return &config.Config{}
}

// createTracesToMetrics creates a traces to metrics connector based on provided config.
func createTracesToMetrics(
	_ context.Context,
	set connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (connector.Traces, error) {
	c := cfg.(*config.Config)

	metricDefs := make([]model.MetricDef, 0, len(c.Spans))
	for _, info := range c.Spans {
		resAttrs, err := parseAttributeConfigs(info.IncludeResourceAttributes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse resource attribute config: %w", err)
		}
		attrs, err := parseAttributeConfigs(info.Attributes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse attribute config: %w", err)
		}
		md := model.MetricDef{
			Key: model.MetricKey{
				Name:        info.Name,
				Description: info.Description,
			},
			Unit:                       info.Unit,
			EphemeralResourceAttribute: info.EphemeralResourceAttribute,
			IncludeResourceAttributes:  resAttrs,
			Attributes:                 attrs,
			ExplicitHistogram:          info.Histogram.Explicit,
			ExponentialHistogram:       info.Histogram.Exponential,
			Summary:                    info.Summary,
			Counters:                   info.Counters,
		}
		metricDefs = append(metricDefs, md)
	}

	return &spanMetrics{
		next:        nextConsumer,
		metricDefs:  metricDefs,
		ephemeralID: uuid.NewString(),
	}, nil
}

func parseAttributeConfigs(cfgs []config.Attribute) ([]model.AttributeKeyValue, error) {
	var errs []error
	kvs := make([]model.AttributeKeyValue, len(cfgs))
	for i, attr := range cfgs {
		val := pcommon.NewValueEmpty()
		if err := val.FromRaw(attr.DefaultValue); err != nil {
			errs = append(errs, err)
		}
		kvs[i] = model.AttributeKeyValue{
			Key:          attr.Key,
			DefaultValue: val,
		}
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return kvs, nil
}
